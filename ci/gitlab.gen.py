#!/usr/bin/env python3

import argparse
import json
import os
import sys
from dataclasses import dataclass, field
from typing import Self


RUNNER_TAG = 'gitlab--duo'
KANIKO_IMAGE = 'ghcr.io/osscontainertools/kaniko:v1.28.6-debug@sha256:7ab44d0131c3051b6dea128067e258c7afb5c4815082a93b9cf6c624f2140bf0'
CACHE_DIR = '.nx-remote-cache'
CACHE_PORT = 4300
IMAGES_DIR = 'images'


q = json.dumps


@dataclass
class NxTask:
    id: str
    project: str
    target: str
    project_root: str
    continuous: bool
    outputs: list[str]


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
    targets: set[str] = field(default_factory=set)

    @classmethod
    def from_json(cls, content: str) -> Self:
        content = json.loads(content)
        data = content['tasks']

        tasks = {
            task_id: NxTask(
                id=task_id,
                project=task['target']['project'],
                target=task['target']['target'],
                project_root=task['projectRoot'],
                continuous=task.get('continuous', False),
                outputs=task.get('outputs', []),
            )
            for task_id, task in data['tasks'].items()
        }

        edges = [
            NxTaskGraphEdge(source, target)
            for source, targets in data['dependencies'].items()
            for target in targets
        ]

        targets = {
            target
            for node in content['graph']['nodes'].values()
            for target in node['data'].get('targets', {})
        }

        return cls(NxTaskGraph(tasks, edges), targets)


@dataclass
class InfraTarget:
    project: str
    branch: str


@dataclass
class Job:
    name: str
    project: str
    target: str
    project_root: str
    needs: list[str]
    outputs: list[str] = field(default_factory=list)
    order_needs: list[str] = field(default_factory=list)


def _dependency_closure(deps: dict[str, list[str]], task_id: str) -> list[str]:
    closure = []
    stack = list(reversed(deps[task_id]))

    while stack:
        dep = stack.pop()

        if dep in closure:
            continue

        closure.append(dep)
        stack.extend(reversed(deps[dep]))

    return closure


def build_job_list(nx_graph: NxGraph, steps: list[str]) -> list[Job]:
    graph = nx_graph.task_graph

    unknown = [step for step in steps if step not in nx_graph.targets]

    if unknown:
        raise ValueError(f'unknown steps: {unknown}')

    duplicates = sorted({step for step in steps if steps.count(step) > 1})

    if duplicates:
        raise ValueError(f'duplicate steps: {duplicates}')

    tasks = {
        task_id: task
        for task_id, task in graph.tasks.items()
        if not task.continuous
    }

    deps = {
        task_id: sorted(dep for dep in graph.dependencies(task_id) if dep in tasks)
        for task_id in tasks
    }

    task_ids = {(task.project, task.target): task_id for task_id, task in tasks.items()}
    order = {}

    for task_id, task in tasks.items():
        if task.target not in steps:
            continue

        earlier = reversed(steps[:steps.index(task.target)])
        previous = [task_ids[(task.project, step)] for step in earlier if (task.project, step) in task_ids]
        order[task_id] = previous[:1]

    jobs = []
    done = set()
    pending = sorted(tasks)

    while pending:
        ready = [
            task_id
            for task_id in pending
            if all(dep in done for dep in deps[task_id] + order.get(task_id, []))
        ]

        if not ready:
            raise ValueError(f'cycle in nx task graph or step order {steps}: {pending}')

        for task_id in ready:
            task = tasks[task_id]
            jobs.append(Job(
                task_id,
                task.project,
                task.target,
                task.project_root,
                deps[task_id],
                order_needs=order.get(task_id, []),
            ))

            done.add(task_id)

        pending = [task_id for task_id in pending if task_id not in done]

    by_name = {job.name: job for job in jobs}

    for job in jobs:
        if not is_build_task(job):
            continue

        closure = _dependency_closure(deps, job.name)
        nested = [dep for dep in closure if is_build_task(by_name[dep])]

        if nested:
            raise ValueError(f'build task {job.name} depends on build tasks {nested}')

        job.outputs = list(dict.fromkeys(
            output
            for dep in closure
            for output in tasks[dep].outputs
        ))

    return jobs


def is_build_task(job: Job) -> bool:
    return job.target == 'build'


def _needs(needs: list[str], order_needs: list[str] | None = None) -> str:
    order_needs = order_needs or []

    if not needs and not order_needs:
        return '  needs: []\n'

    return '  needs:\n' + ''.join(
        f'    - job: {q(dep)}\n'
        f'      artifacts: {"true" if artifacts else "false"}\n'
        for dep, artifacts in [(dep, True) for dep in needs] + [(dep, False) for dep in order_needs]
    )


def _list(key: str, values: list[str]) -> str:
    return f'    {key}:\n' + ''.join(f'      - {q(value)}\n' for value in values)


def build_generic_job(job: Job) -> str:
    return (
        f'{q(job.name)}:\n'
        f'  extends: ".nx-task"\n'
        f'{_needs(job.needs, job.order_needs)}'
        f'  script:\n'
        f'    - {q(f"npx nx run {job.name} --outputStyle=stream")}\n'
    )


def _deps_job_name(job: Job) -> str:
    return f'{job.name}:deps'


def build_build_deps_job(job: Job) -> str:
    paths = [CACHE_DIR + '/'] + [output for output in job.outputs if not output.startswith('!')]
    exclude = [output[1:] for output in job.outputs if output.startswith('!')]
    return (
        f'{q(_deps_job_name(job))}:\n'
        f'  extends: ".nx-task"\n'
        f'{_needs(job.needs)}'
        f'  script:\n'
        + ''.join(f'    - {q(f"npx nx run {dep} --outputStyle=stream")}\n' for dep in job.needs)
        + f'  artifacts:\n'
        f'{_list("paths", paths)}'
        + (_list('exclude', exclude) if exclude else '')
        + f'    expire_in: "1 day"\n'
    )


def build_build_job(job: Job, publish: bool) -> str:
    digest_file = f'{IMAGES_DIR}/{job.project}.digest'
    record_file = f'{IMAGES_DIR}/{job.project}.json'
    executor = (
        '/kaniko/executor'
        ' --context "$CI_PROJECT_DIR"'
        f' --dockerfile "$CI_PROJECT_DIR/{job.project_root}/Dockerfile"'
        ' --destination "$IMAGE_REF:$CI_COMMIT_SHA"'
        + (' --destination "$IMAGE_REF:latest"' if publish else '')
        + f' --digest-file "$CI_PROJECT_DIR/{digest_file}"'
        ' --cleanup'
    )
    record = (
        'printf \'{"project":"%s","repository":"%s","tag":"%s","digest":"%s"}\\n\''
        f' {job.project} "$IMAGE_REF" "$CI_COMMIT_SHA" "$(cat {digest_file})"'
        f' > {record_file}'
    )

    kaniko_job = (
        f'{q(job.name)}:\n'
        f'  extends: ".kaniko-task"\n'
        f'{_needs([_deps_job_name(job)] if job.needs else [], job.order_needs)}'
        f'  variables:\n'
        f'    IMAGE_REF: {q(f"$PROJECT_IMAGE_REMOTE/{job.project}")}\n'
        f'  script:\n'
        f'    - {q(f"mkdir -p {IMAGES_DIR}")}\n'
        f'    - {q(executor)}\n'
        f'    - {q(record)}\n'
        f'  artifacts:\n'
        f'{_list("paths", [record_file])}'
        f'    expire_in: "1 day"\n'
    )

    if not job.needs:
        return kaniko_job

    return build_build_deps_job(job) + '\n' + kaniko_job


def build_publish_jobs(jobs: list[Job], infra: InfraTarget) -> str:
    builds = [job for job in jobs if is_build_task(job)]

    if not builds:
        return ''

    expected = ','.join(job.project for job in builds)
    return (
        f'"collect-images":\n'
        f'  stage: "publish"\n'
        f'  dependencies:\n'
        + ''.join(f'    - {q(job.name)}\n' for job in builds)
        + f'  variables:\n'
        f'    EXPECTED_IMAGES: {q(expected)}\n'
        f'  script:\n'
        f'    - {q(f"python3 ci/collect-images.py {IMAGES_DIR} --expect \"$EXPECTED_IMAGES\" > images.json")}\n'
        f'    - {q("echo AAT_IMAGES_JOB_ID=$CI_JOB_ID > publish.env")}\n'
        f'  artifacts:\n'
        f'{_list("paths", ["images.json"])}'
        f'    reports:\n'
        f'      dotenv: "publish.env"\n'
        f'    expire_in: "1 week"\n'
        f'\n'
        f'"infra-pipeline":\n'
        f'  stage: "publish"\n'
        f'{_needs(["collect-images"])}'
        f'  variables:\n'
        f'    AAT_IMAGES_JOB_ID: "$AAT_IMAGES_JOB_ID"\n'
        f'    AAT_PROJECT_ID: "$CI_PROJECT_ID"\n'
        f'    AAT_COMMIT_SHA: "$CI_COMMIT_SHA"\n'
        f'    AAT_PIPELINE_IID: "$CI_PIPELINE_IID"\n'
        f'  trigger:\n'
        f'    project: {q(infra.project)}\n'
        f'    branch: {q(infra.branch)}\n'
        f'    strategy: "depend"\n'
    )


def build_pipeline(jobs: list[Job], runner_image: str, infra: InfraTarget | None) -> str:
    cache_server = f'http://127.0.0.1:{CACHE_PORT}'
    header = (
        f'workflow:\n'
        f'  rules:\n'
        f'    - if: \'$CI_PIPELINE_SOURCE == "parent_pipeline"\'\n'
        f'\n'
        f'stages:\n'
        f'  - "nx"\n'
        f'  - "publish"\n'
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
        f'  stage: "nx"\n'
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
        build_build_job(job, infra is not None) if is_build_task(job) else build_generic_job(job)
        for job in jobs
    ]

    if infra and (publish_jobs := build_publish_jobs(jobs, infra)):
        blocks.append(publish_jobs)

    return header + '\n' + '\n'.join(blocks)


def main(nx_graph: NxGraph, runner_image: str, steps: list[str], infra: InfraTarget | None):
    print(build_pipeline(build_job_list(nx_graph, steps), runner_image, infra), end='')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Generate a GitLab child pipeline from an nx task graph read from stdin.',
    )
    parser.add_argument('runner_image', help='image the nx task jobs run in')
    parser.add_argument(
        '--steps',
        default='',
        help='comma separated targets that run one after another within each project',
    )
    parser.add_argument(
        '--publish',
        action='store_true',
        help='also tag images latest, collect their digests and trigger the infra pipeline',
    )
    parser.add_argument(
        '--infra-project',
        default=os.environ.get('INFRA_PROJECT', ''),
        help='path of the project whose pipeline --publish triggers (default: $INFRA_PROJECT)',
    )
    parser.add_argument(
        '--infra-branch',
        default=os.environ.get('INFRA_BRANCH', ''),
        help='branch of that project to run the pipeline on (default: $INFRA_BRANCH)',
    )
    args = parser.parse_args()

    if args.publish and not (args.infra_project and args.infra_branch):
        parser.error('--publish needs --infra-project and --infra-branch (or $INFRA_PROJECT and $INFRA_BRANCH)')

    steps = [step for step in args.steps.split(',') if step]
    infra = InfraTarget(args.infra_project, args.infra_branch) if args.publish else None
    main(NxGraph.from_json(sys.stdin.read()), args.runner_image, steps, infra)
