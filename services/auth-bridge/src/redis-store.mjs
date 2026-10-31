import { createClient } from '@redis/client';
import { LOGIN_TTL_MS, MIN_REVOCATION_MS, SessionCipher, hash, randomSecret } from './store.mjs';

// Deletes and returns the login only for the browser it was issued to, so a request with the
// wrong binding cannot use up someone else's login.
const TAKE_LOGIN = `
local value = redis.call('GET', KEYS[1])
if not value then return false end
local login = cjson.decode(value)
if login.binding ~= ARGV[1] then return false end
redis.call('DEL', KEYS[1])
return login.payload`;

const UNLOCK = `
if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) end
return 0`;

const login = state => `auth:login:${hash(state)}`;
const session = id => `auth:session:${hash(id)}`;
const revoked = token => `auth:revoked:${hash(token)}`;
const lock = name => `auth:lock:${name}`;

// Shared session store for every replica. Redis runs without persistence: losing it ends every
// session (users log in again). Revoked tokens stay rejected because Zitadel introspection, not
// this denylist, is what accepts a token. Keys expire with their data, so there is no cleanup.
export class RedisSessionStore {
  static async connect(url, key, logger = console.log) {
    // Without the offline queue, commands fail at once while Redis is down (503) instead of
    // holding requests until the request timeout.
    const client = createClient({ url, disableOfflineQueue: true });
    let reported = false;
    client.on('error', error => {
      if (!reported) logger(JSON.stringify({ event: 'redis_error', error: error.message }));
      reported = true;
    });
    client.on('ready', () => { reported = false; });
    await client.connect();
    await client.ping();
    return new RedisSessionStore(client, key);
  }
  constructor(client, key) {
    this.client = client;
    this.cipher = new SessionCipher(key);
  }
  async cleanup() {}
  async putLogin(state, binding, value) {
    await this.client.set(login(state), JSON.stringify({ binding: hash(binding), payload: this.cipher.encrypt(value) }),
      { expiration: { type: 'PX', value: LOGIN_TTL_MS } });
  }
  async takeLogin(state, binding) {
    if (!state || !binding) return null;
    const payload = await this.client.eval(TAKE_LOGIN, { keys: [login(state)], arguments: [hash(binding)] });
    return payload ? this.cipher.decrypt(payload) : null;
  }
  async getSession(id) {
    if (!id) return null;
    const value = await this.client.get(session(id));
    return value ? this.cipher.decrypt(value) : null;
  }
  async putSession(id, value) {
    await this.client.set(session(id), this.cipher.encrypt(value), { expiration: { type: 'PXAT', value: value.sessionExpires } });
  }
  async revoke(token, expires) {
    if (token) await this.client.set(revoked(token), '1', { expiration: { type: 'PXAT', value: Math.max(expires, Date.now() + MIN_REVOCATION_MS) } });
  }
  async isRevoked(token) {
    return (await this.client.exists(revoked(token))) > 0;
  }
  async rotate(id, previous, next) {
    const transaction = this.client.multi();
    if (previous.accessToken) {
      transaction.set(revoked(previous.accessToken), '1',
        { expiration: { type: 'PXAT', value: Math.max(previous.expiresAt, Date.now() + MIN_REVOCATION_MS) } });
    }
    if (next) transaction.set(session(id), this.cipher.encrypt(next), { expiration: { type: 'PXAT', value: next.sessionExpires } });
    else transaction.del(session(id));
    await transaction.exec();
  }
  async tryLock(name, ttlMs) {
    const token = randomSecret();
    const reply = await this.client.set(lock(name), token, { condition: 'NX', expiration: { type: 'PX', value: ttlMs } });
    return reply === 'OK' ? token : null;
  }
  async unlock(name, token) {
    await this.client.eval(UNLOCK, { keys: [lock(name)], arguments: [token] });
  }
  async isLocked(name) {
    return (await this.client.exists(lock(name))) > 0;
  }
  async close() { await this.client.close(); }
}
