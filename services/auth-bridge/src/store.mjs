import { DatabaseSync } from 'node:sqlite';
import { createCipheriv, createDecipheriv, createHash, randomBytes, timingSafeEqual } from 'node:crypto';
import { mkdirSync } from 'node:fs';
import { dirname } from 'node:path';

export const randomSecret = () => randomBytes(32).toString('base64url');
export const hash = value => createHash('sha256').update(value).digest('hex');
export const equal = (a, b) => typeof a === 'string' && typeof b === 'string' && Buffer.byteLength(a) === Buffer.byteLength(b) && timingSafeEqual(Buffer.from(a), Buffer.from(b));

export const LOGIN_TTL_MS = 600000;
// A revoked access token stays denied at least this long, even if it claims to expire sooner.
export const MIN_REVOCATION_MS = 60000;

// Tokens and CSRF secrets are encrypted at rest; identifiers are indexed by SHA-256.
export class SessionCipher {
  constructor(key) {
    this.key = Buffer.from(key, 'hex');
    if (this.key.length !== 32) throw new Error('Invalid session encryption key');
  }
  encrypt(value) {
    const iv = randomBytes(12);
    const cipher = createCipheriv('aes-256-gcm', this.key, iv);
    const bytes = Buffer.concat([cipher.update(JSON.stringify(value), 'utf8'), cipher.final()]);
    return Buffer.concat([iv, cipher.getAuthTag(), bytes]).toString('base64url');
  }
  decrypt(value) {
    const data = Buffer.from(value, 'base64url');
    const decipher = createDecipheriv('aes-256-gcm', this.key, data.subarray(0, 12));
    decipher.setAuthTag(data.subarray(12, 28));
    return JSON.parse(Buffer.concat([decipher.update(data.subarray(28)), decipher.final()]).toString('utf8'));
  }
}

// Single-replica store for local runs and tests. RedisSessionStore (redis-store.mjs) has the same
// async interface and is what replicas share. Every operation is one statement or one transaction,
// so instances sharing a file behave like replicas sharing Redis.
export class SqliteSessionStore {
  constructor(path, key) {
    this.cipher = new SessionCipher(key);
    if (path !== ':memory:') mkdirSync(dirname(path), { recursive: true });
    this.db = new DatabaseSync(path, { timeout: 5000 });
    this.db.exec(`PRAGMA journal_mode=WAL;
      CREATE TABLE IF NOT EXISTS logins (id TEXT PRIMARY KEY, binding TEXT NOT NULL, payload TEXT NOT NULL, expires INTEGER NOT NULL);
      CREATE TABLE IF NOT EXISTS sessions (id TEXT PRIMARY KEY, payload TEXT NOT NULL, expires INTEGER NOT NULL);
      CREATE TABLE IF NOT EXISTS revoked (id TEXT PRIMARY KEY, expires INTEGER NOT NULL);
      CREATE TABLE IF NOT EXISTS locks (name TEXT PRIMARY KEY, token TEXT NOT NULL, expires INTEGER NOT NULL);`);
  }
  async cleanup(now = Date.now()) {
    for (const table of ['logins', 'sessions', 'revoked', 'locks']) this.db.prepare(`DELETE FROM ${table} WHERE expires <= ?`).run(now);
  }
  async putLogin(state, binding, value) {
    await this.cleanup();
    this.db.prepare('INSERT INTO logins VALUES (?, ?, ?, ?)').run(hash(state), hash(binding), this.cipher.encrypt(value), Date.now() + LOGIN_TTL_MS);
  }
  // Atomic, so a replayed callback cannot be accepted twice; a wrong binding leaves the login usable.
  async takeLogin(state, binding) {
    if (!state || !binding) return null;
    const row = this.db.prepare('DELETE FROM logins WHERE id = ? AND binding = ? AND expires > ? RETURNING payload')
      .get(hash(state), hash(binding), Date.now());
    return row ? this.cipher.decrypt(row.payload) : null;
  }
  async getSession(id) {
    if (!id) return null;
    const row = this.db.prepare('SELECT payload FROM sessions WHERE id = ? AND expires > ?').get(hash(id), Date.now());
    return row ? this.cipher.decrypt(row.payload) : null;
  }
  async putSession(id, session) {
    this.#putSession(id, session);
  }
  #putSession(id, session) {
    this.db.prepare('INSERT OR REPLACE INTO sessions VALUES (?, ?, ?)').run(hash(id), this.cipher.encrypt(session), session.sessionExpires);
  }
  async revoke(token, expires) {
    this.#revoke(token, expires);
  }
  #revoke(token, expires) {
    if (token) this.db.prepare('INSERT OR REPLACE INTO revoked VALUES (?, ?)').run(hash(token), Math.max(expires, Date.now() + MIN_REVOCATION_MS));
  }
  async isRevoked(token) {
    return Boolean(this.db.prepare('SELECT 1 FROM revoked WHERE id = ? AND expires > ?').get(hash(token), Date.now()));
  }
  // Revokes the previous access token and replaces (or, with next null, ends) the session.
  async rotate(id, previous, next) {
    this.db.exec('BEGIN IMMEDIATE');
    try {
      this.#revoke(previous.accessToken, previous.expiresAt);
      if (next) this.#putSession(id, next);
      else this.db.prepare('DELETE FROM sessions WHERE id = ?').run(hash(id));
      this.db.exec('COMMIT');
    } catch (error) { this.db.exec('ROLLBACK'); throw error; }
  }
  // Returns a token when the lock was free (or its holder's lease ran out), else null.
  async tryLock(name, ttlMs) {
    const token = randomSecret(), now = Date.now();
    const { changes } = this.db.prepare(`INSERT INTO locks VALUES (?, ?, ?)
      ON CONFLICT (name) DO UPDATE SET token = excluded.token, expires = excluded.expires WHERE locks.expires <= ?`)
      .run(name, token, now + ttlMs, now);
    return changes > 0 ? token : null;
  }
  async unlock(name, token) {
    this.db.prepare('DELETE FROM locks WHERE name = ? AND token = ?').run(name, token);
  }
  async isLocked(name) {
    return Boolean(this.db.prepare('SELECT 1 FROM locks WHERE name = ? AND expires > ?').get(name, Date.now()));
  }
  async close() { this.db.close(); }
}
