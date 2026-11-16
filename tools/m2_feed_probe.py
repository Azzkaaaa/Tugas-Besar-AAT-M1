"""Capture the official practice feed without logging credentials or changing its session.

python tools/m2_feed_probe.py --seconds 20 --output .cache/m2/feed.jsonl
The output is replayable with: go run ./cmd/m2replay -input ../../.cache/m2/feed.jsonl
"""
import argparse
import base64
import datetime as dt
import json
import os
from pathlib import Path
import socket
import time
import urllib.error
import urllib.parse
import urllib.request


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--base-url', default='https://satellite.naufarrel.dev')
    parser.add_argument('--token-file', default='.secrets/satellite-token')
    parser.add_argument('--seconds', type=float, default=20)
    parser.add_argument('--from-offset', default='0')
    parser.add_argument('--output', default='.cache/m2/feed.jsonl')
    args = parser.parse_args()
    token = os.environ.get('SATELLITE_TOKEN') or Path(args.token_file).read_text().strip()
    headers = {'Authorization': 'Bearer ' + token}
    base = args.base_url.rstrip('/')
    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)
    try:
        request = urllib.request.Request(base + '/v1/session', headers=headers)
        with urllib.request.urlopen(request, timeout=10) as response:
            session = json.load(response)
        out.with_suffix('.session.json').write_text(json.dumps(session, indent=2), encoding='utf-8')
        print(json.dumps({'session': session}))
        request = urllib.request.Request(base + '/v1/stream?from=' + urllib.parse.quote(args.from_offset),
                                         headers={**headers, 'Accept': 'text/event-stream'})
        started = time.monotonic()
        count, cursor = 0, args.from_offset
        with urllib.request.urlopen(request, timeout=10) as response, out.open('w', encoding='utf-8') as target:
            kind, event_id, data, size = '', '', [], 0
            while time.monotonic() - started < args.seconds:
                line = response.readline(2**20 + 1)
                if not line:
                    break
                size += len(line)
                if size > 2**20:
                    raise ValueError('SSE frame exceeds 1 MiB; capture stopped without advancing cursor')
                line = line.rstrip(b'\r\n')
                if not line:
                    if data:
                        count += 1
                        cursor = event_id or cursor
                        payload = b'\n'.join(data)
                        record = {'sequence': count, 'kind': kind or 'message', 'cursor': cursor,
                                  'received_at': dt.datetime.now(dt.timezone.utc).isoformat(),
                                  'payload': base64.b64encode(payload).decode()}
                        target.write(json.dumps(record) + '\n')
                        target.flush()
                        if count <= 3:
                            print(json.dumps({'event': kind, 'id': event_id,
                                              'data': payload.decode('utf-8', errors='replace')}))
                    kind, event_id, data, size = '', '', [], 0
                    continue
                name, _, value = line.partition(b':')
                value = value.removeprefix(b' ')
                if name == b'event':
                    kind = value.decode()
                elif name == b'id':
                    event_id = value.decode()
                elif name == b'data':
                    data.append(value)
        print(json.dumps({'captured': count, 'last_cursor': cursor, 'path': str(out)}))
    except urllib.error.HTTPError as error:
        print(json.dumps({'status': error.code, 'error': error.read(4096).decode(errors='replace')}))
        raise SystemExit(1)
    except (socket.timeout, TimeoutError):
        print('Feed idle timeout; complete frames already written remain valid.')


if __name__ == '__main__':
    main()
