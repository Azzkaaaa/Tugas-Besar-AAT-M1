#!/usr/bin/env python3

import argparse
import json
import os
import re
import sys
from pathlib import Path


DIGEST = re.compile(r'sha256:[0-9a-f]{64}')


def read_records(images_dir: Path) -> dict[str, dict]:
    records = {}

    for path in sorted(images_dir.glob('*.json')):
        record = json.loads(path.read_text())
        project = record['project']

        if project in records:
            raise ValueError(f'duplicate image record for {project}')

        records[project] = record

    return records


def collect(records: dict[str, dict], expected: list[str], commit: str) -> dict[str, dict]:
    missing = sorted(set(expected) - set(records))
    unexpected = sorted(set(records) - set(expected))

    if missing:
        raise ValueError(f'missing image records: {missing}')

    if unexpected:
        raise ValueError(f'unexpected image records: {unexpected}')

    images = {}

    for project in sorted(records):
        record = records[project]

        if not DIGEST.fullmatch(record['digest']):
            raise ValueError(f'bad digest for {project}: {record["digest"]!r}')

        if record['tag'] != commit:
            raise ValueError(f'tag for {project} is {record["tag"]!r}, expected {commit!r}')

        if not record['repository']:
            raise ValueError(f'empty repository for {project}')

        images[project] = {
            'repository': record['repository'],
            'tag': record['tag'],
            'digest': record['digest'],
        }

    return images


def main(images_dir: Path, expected: list[str]):
    commit = os.environ['CI_COMMIT_SHA']
    images = collect(read_records(images_dir), expected, commit)
    json.dump({
        'commit': commit,
        'pipeline_iid': int(os.environ['CI_PIPELINE_IID']),
        'pipeline_url': os.environ['CI_PIPELINE_URL'],
        'images': images,
    }, sys.stdout, indent=2)
    print()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Merge per-image build records into one images.json for the infra pipeline.',
    )
    parser.add_argument('images_dir', type=Path, help='directory holding <project>.json records')
    parser.add_argument('--expect', required=True, help='comma separated projects that must have a record')
    args = parser.parse_args()

    expected = [project for project in args.expect.split(',') if project]

    try:
        main(args.images_dir, expected)
    except (ValueError, KeyError) as error:
        print(f'collect-images: {error}', file=sys.stderr)
        sys.exit(1)
