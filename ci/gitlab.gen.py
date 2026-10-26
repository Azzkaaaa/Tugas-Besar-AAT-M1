#!/usr/bin/env python3

import argparse
import json
import sys
from dataclasses import dataclass, field
from typing import Self


RUNNER_TAG = 'gitlab--duo'
KANIKO_IMAGE = 'ghcr.io/osscontainertools/kaniko:v1.28.6-debug@sha256:7ab44d0131c3051b6dea128067e258c7afb5c4815082a93b9cf6c624f2140bf0'
CACHE_DIR = '.nx-remote-cache'
CACHE_PORT = 4300


q = json.dumps


@dataclass
class NxTask:
    id: str
    project: str
    target: str
    project_root: str
    continuous: bool


@dataclass
class NxTaskGraphEdge:
    source: str
    target: str


@dataclass
class NxTaskGraph:
    tasks: dict[str, NxTask] = field(default_factory=dict)
    edges: list[NxTaskGraphEdge] = field(default_factory=list)

    def dependencies(self, task_id: str) -> list[str]:
        return [edge.target for edge in self.edges if edge.source == task_id]


@dataclass
class NxGraph:
    task_graph: NxTaskGraph

    @classmethod
    def from_json(cls, content: str) -> Self:
        data = json.loads(content)['tasks']

        tasks = {
            task_id: NxTask(
                id=task_id,
                project=task['target']['project'],
                target=task['target']['target'],
                project_root=task['projectRoot'],
                continuous=task.get('continuous', False),
            )
            for task_id, task in data['tasks'].items()
        }
        edges = [
            NxTaskGraphEdge(source, target)
            for source, targets in data['dependencies'].items()
            for target in targets
        ]

        return cls(NxTaskGraph(tasks, edges))


@dataclass
class Job:
    name: str
    project: str
    target: str
    project_root: str
    needs: list[str]


def build_job_list(nx_graph: NxGraph) -> list[Job]:
    graph = nx_graph.task_graph
    tasks = {
        task_id: task
        for task_id, task in graph.tasks.items()
        if not task.continuous
    }
    deps = {
        task_id: sorted(dep for dep in graph.dependencies(task_id) if dep in tasks)
        for task_id in tasks
    }

    jobs = []
    done = set()
    pending = sorted(tasks)
    while pending:
        ready = [task_id for task_id in pending if all(dep in done for dep in deps[task_id])]
        if not ready:
            raise ValueError(f'cycle in nx task graph: {pending}')
        for task_id in ready:
            task = tasks[task_id]
            jobs.append(Job(task_id, task.project, task.target, task.project_root, deps[task_id]))
            done.add(task_id)
        pending = [task_id for task_id in pending if task_id not in done]

    return jobs


def is_build_task(job: Job) -> bool:
    return job.target == 'build'


def _needs(job: Job) -> str:
    if not job.needs:
        return '  needs: []\n'
    return '  needs:\n' + ''.join(
        f'    - job: {q(dep)}\n'
        f'      artifacts: true\n'
        for dep in job.needs
    )


def build_generic_job(job: Job) -> str:
    return (
        f'{q(job.name)}:\n'
        f'  extends: ".nx-task"\n'
        f'{_needs(job)}'
        f'  script:\n'
        f'    - {q(f"npx nx run {job.name} --outputStyle=stream")}\n'
    )


def build_build_job(job: Job) -> str:
    executor = (
        '/kaniko/executor'
        ' --context "$CI_PROJECT_DIR"'
        f' --dockerfile "$CI_PROJECT_DIR/{job.project_root}/Dockerfile"'
        f' --destination "$PROJECT_IMAGE_REMOTE/{job.project}:$CI_COMMIT_SHA"'
        ' --cleanup'
    )
    return (
        f'{q(job.name)}:\n'
        f'  extends: ".kaniko-task"\n'
        f'{_needs(job)}'
        f'  script:\n'
        f'    - {q(executor)}\n'
    )


def build_pipeline(jobs: list[Job], runner_image: str) -> str:
    cache_server = f'http://127.0.0.1:{CACHE_PORT}'
    header = (
        f'stages:\n'
        f'  - "nx"\n'
        f'\n'
        f'default:\n'
        f'  image: {q(runner_image)}\n'
        f'  tags:\n'
        f'    - {q(RUNNER_TAG)}\n'
        f'\n'
        f'.cache-artifacts:\n'
        f'  stage: "nx"\n'
        f'  artifacts:\n'
        f'    paths:\n'
        f'      - {q(CACHE_DIR + "/")}\n'
        f'    expire_in: "1 day"\n'
        f'\n'
        f'.nx-task:\n'
        f'  extends: ".cache-artifacts"\n'
        f'  variables:\n'
        f'    NX_DAEMON: "false"\n'
        f'    NX_SELF_HOSTED_REMOTE_CACHE_SERVER: {q(cache_server)}\n'
        f'    NX_SELF_HOSTED_REMOTE_CACHE_ACCESS_TOKEN: "local"\n'
        f'  cache:\n'
        f'    key:\n'
        f'      files:\n'
        f'        - "package-lock.json"\n'
        f'    paths:\n'
        f'      - "node_modules/"\n'
        f'  before_script:\n'
        f'    - {q(f"node ci/nx-cache-server.mjs --dir {CACHE_DIR} --port {CACHE_PORT} &")}\n'
        f'    - {q(f"until curl -s -o /dev/null {cache_server}/; do sleep 0.2; done")}\n'
        f'    - "npm ci"\n'
        f'\n'
        f'.kaniko-task:\n'
        f'  extends: ".cache-artifacts"\n'
        f'  image:\n'
        f'    name: {q(KANIKO_IMAGE)}\n'
        f'    entrypoint: [""]\n'
        f'  before_script:\n'
        f'    - "mkdir -p /kaniko/.docker && echo \\"$DOCKER_AUTH_CONFIG\\" > /kaniko/.docker/config.json"\n'
    )

    if not jobs:
        return header + (
            '\n'
            'no-affected-tasks:\n'
            '  stage: "nx"\n'
            '  script:\n'
            '    - "echo no affected nx tasks"\n'
        )

    blocks = [
        build_build_job(job) if is_build_task(job) else build_generic_job(job)
        for job in jobs
    ]
    return header + '\n' + '\n'.join(blocks)


def main(nx_graph: NxGraph, runner_image: str):
    print(build_pipeline(build_job_list(nx_graph), runner_image), end='')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Generate a GitLab child pipeline from an nx task graph read from stdin.',
    )
    parser.add_argument('runner_image', help='image the nx task jobs run in')
    args = parser.parse_args()

    main(NxGraph.from_json(sys.stdin.read()), args.runner_image)
