import { DatabaseSync } from 'node:sqlite';
import { createCipheriv, createDecipheriv, createHash, randomBytes, timingSafeEqual } from 'node:crypto';
import { mkdirSync } from 'node:fs';
import { dirname } from 'node:path';

export const randomSecret = () => randomBytes(32).toString('base64url');
export const hash = value => createHash('sha256').update(value).digest('hex');
export const equal = (a, b) => typeof a === 'string' && typeof b === 'string' && Buffer.byteLength(a) === Buffer.byteLength(b) && timingSafeEqual(Buffer.from(a), Buffer.from(b));

// Tokens and CSRF secrets are encrypted at rest; identifiers are indexed by SHA-256.
export class SessionStore {
  constructor(path, key) {
    this.key = Buffer.from(key, 'hex');
    if (this.key.length !== 32) throw new Error('Invalid session encryption key');
    if (path !== ':memory:') mkdirSync(dirname(path), { recursive: true });
    this.db = new DatabaseSync(path);
    this.db.exec(`PRAGMA journal_mode=WAL;
      CREATE TABLE IF NOT EXISTS logins (id TEXT PRIMARY KEY, binding TEXT NOT NULL, payload TEXT NOT NULL, expires INTEGER NOT NULL);
      CREATE TABLE IF NOT EXISTS sessions (id TEXT PRIMARY KEY, payload TEXT NOT NULL, expires INTEGER NOT NULL);
      CREATE TABLE IF NOT EXISTS revoked (id TEXT PRIMARY KEY, expires INTEGER NOT NULL);`);
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
  cleanup(now = Date.now()) {
    for (const table of ['logins', 'sessions', 'revoked']) this.db.prepare(`DELETE FROM ${table} WHERE expires <= ?`).run(now);
  }
  putLogin(state, binding, value) {
    this.cleanup();
    this.db.prepare('INSERT INTO logins VALUES (?, ?, ?, ?)').run(hash(state), hash(binding), this.encrypt(value), Date.now() + 600000);
  }
  takeLogin(state, binding) {
    if (!state || !binding) return null;
    const row = this.db.prepare('SELECT * FROM logins WHERE id = ? AND binding = ? AND expires > ?').get(hash(state), hash(binding), Date.now());
    if (!row) return null;
    this.db.prepare('DELETE FROM logins WHERE id = ?').run(row.id);
    return this.decrypt(row.payload);
  }
  getSession(id) {
    if (!id) return null;
    const row = this.db.prepare('SELECT payload FROM sessions WHERE id = ? AND expires > ?').get(hash(id), Date.now());
    return row ? this.decrypt(row.payload) : null;
  }
  putSession(id, session) {
    this.db.prepare('INSERT OR REPLACE INTO sessions VALUES (?, ?, ?)').run(hash(id), this.encrypt(session), session.sessionExpires);
  }
  revoke(token, expires) {
    if (token) this.db.prepare('INSERT OR REPLACE INTO revoked VALUES (?, ?)').run(hash(token), Math.max(expires, Date.now() + 60000));
  }
  isRevoked(token) {
    return Boolean(this.db.prepare('SELECT 1 FROM revoked WHERE id = ? AND expires > ?').get(hash(token), Date.now()));
  }
  rotate(id, previous, next) {
    this.db.exec('BEGIN IMMEDIATE');
    try {
      this.revoke(previous.accessToken, previous.expiresAt);
      if (next) this.putSession(id, next);
      else this.db.prepare('DELETE FROM sessions WHERE id = ?').run(hash(id));
      this.db.exec('COMMIT');
    } catch (error) { this.db.exec('ROLLBACK'); throw error; }
  }
  close() { this.db.close(); }
}
