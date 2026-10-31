import { setTimeout as delay } from 'node:timers/promises';
import { AuthError } from './provider.mjs';
import { hash } from './store.mjs';

// Covers one provider refresh plus its introspection, each with a 10 s fetch timeout. A holder
// that dies only blocks its session's refreshes this long.
const LOCK_TTL_MS = 25000;
const POLL_MS = 100;
// Just under the server's 15 s request timeout.
const WAIT_MS = 14000;

const lockName = id => `refresh:${hash(id)}`;

// Refreshes a session once across every replica sharing the store. Zitadel rotates refresh
// tokens, so two concurrent refreshes with the same token would fail the second one (and end the
// session). The replica holding the lock runs refresh(); the others wait for it and use the
// session it stored. If that replica gave up without storing one, the next waiter tries itself.
export async function refreshOnce(store, id, previous, refresh) {
  const name = lockName(id);
  const until = Date.now() + WAIT_MS;
  for (;;) {
    const token = await store.tryLock(name, LOCK_TTL_MS);
    if (token) {
      try { return await refresh(); }
      finally { await store.unlock(name, token).catch(() => { /* expires with its lease */ }); }
    }
    await waitForRefresh(store, id, until);
    const current = await store.getSession(id);
    if (!current) throw new AuthError(401, 'session_expired');
    if (current.accessToken !== previous.accessToken) return current;
  }
}

// Waits while another request (on any replica) refreshes this session.
export async function waitForRefresh(store, id, until = Date.now() + WAIT_MS) {
  const name = lockName(id);
  while (await store.isLocked(name)) {
    if (Date.now() >= until) throw new Error('Session refresh still in progress');
    await delay(POLL_MS);
  }
}
