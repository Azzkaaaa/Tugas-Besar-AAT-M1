#!/usr/bin/env node

import { createReadStream, createWriteStream, existsSync, mkdirSync, renameSync, rmSync, statSync } from 'node:fs';
import { createServer } from 'node:http';
import { join } from 'node:path';
import { pipeline } from 'node:stream/promises';
import { parseArgs } from 'node:util';

const { values: args } = parseArgs({
  options: {
    dir: { type: 'string', default: '.nx-remote-cache' },
    port: { type: 'string', default: '4300' },
  },
});

mkdirSync(args.dir, { recursive: true });

const route = /^\/v1\/cache\/([A-Za-z0-9_-]+)$/;

const server = createServer(async (req, res) => {
  const match = route.exec(new URL(req.url, 'http://localhost').pathname);
  if (!match) {
    res.writeHead(404).end();
    return;
  }

  const file = join(args.dir, match[1]);

  if (req.method === 'GET') {
    if (!existsSync(file)) {
      res.writeHead(404).end();
      return;
    }
    res.writeHead(200, {
      'Content-Type': 'application/octet-stream',
      'Content-Length': statSync(file).size,
    });
    createReadStream(file).pipe(res);
    return;
  }

  if (req.method === 'PUT') {
    if (existsSync(file)) {
      req.resume();
      res.writeHead(409).end();
      return;
    }
    const tmp = `${file}.${process.pid}.tmp`;
    try {
      await pipeline(req, createWriteStream(tmp));
      renameSync(tmp, file);
      res.writeHead(200).end();
    } catch (err) {
      rmSync(tmp, { force: true });
      console.error(`nx-cache-server: failed to store ${match[1]}: ${err}`);
      res.writeHead(500).end();
    }
    return;
  }

  res.writeHead(405).end();
});

server.listen(Number(args.port), '127.0.0.1', () => {
  console.log(`nx-cache-server: serving ${args.dir} on 127.0.0.1:${args.port}`);
});
