import { useEffect, useState } from 'react';
import { getDemoSession } from './auth/demo';
import { getSession, logoutSession, refreshSession, subscribeSession, type SessionPayload } from './auth/session';
import { isDemoMode, isMockDataMode } from './config';
import { createGatewayClient } from './api';
import { DashboardPage } from './pages/DashboardPage';
import { loadPush } from './push/lazy';
import { LoginPage } from './pages/LoginPage';
import type { AuthSession, Role } from './types';

const demoRoleKey = 'bnpb.demo.role';
const pushLogoutTimeoutMs = 2000;

function restoreDemoSession(): AuthSession | null {
  if (!isDemoMode || !isMockDataMode) return null;
  try {
    const role = sessionStorage.getItem(demoRoleKey);
    return role === 'MEDIA' || role === 'FIELD_TEAM' || role === 'BNPB_INTERNAL' ? getDemoSession(role) : null;
  } catch {
    return null;
  }
}

export default function App() {
  const [session, setSession] = useState(restoreDemoSession);
  const [expiresAt, setExpiresAt] = useState<string>();
  const [loading, setLoading] = useState(!isDemoMode);
  const [error, setError] = useState('');
  const [attempt, setAttempt] = useState(0);
  const [loggingOut, setLoggingOut] = useState(false);
  const [route, setRoute] = useState(() => window.location.hash);

  useEffect(() => {
    const onHashChange = () => setRoute(window.location.hash);
    window.addEventListener('hashchange', onHashChange);
    return () => window.removeEventListener('hashchange', onHashChange);
  }, []);

  // Notification clicks reach an already open window as a worker message (push/client.ts).
  useEffect(() => {
    if (!loadPush) return;
    let stop: (() => void) | undefined;
    let active = true;
    void loadPush().then(push => { if (active) stop = push.onPushOpen(next => { window.location.hash = next; }); });
    return () => { active = false; stop?.(); };
  }, []);

  useEffect(() => {
    if (isDemoMode) return;
    const controller = new AbortController();
    const update = (payload: SessionPayload | null) => {
      if (controller.signal.aborted) return;
      setExpiresAt(payload?.expires_at);
      // Refreshing cookies must not rebuild the dashboard or reset active filters.
      setSession(previous => JSON.stringify(previous) === JSON.stringify(payload?.user ?? null) ? previous : payload?.user ?? null);
    };
    const unsubscribe = subscribeSession(update);
    setLoading(true); setError('');
    getSession(controller.signal).then(update).catch((reason: unknown) => {
      if (!controller.signal.aborted) setError(reason instanceof Error ? reason.message : 'Sesi tidak dapat diperiksa.');
    }).finally(() => { if (!controller.signal.aborted) setLoading(false); });
    return () => { controller.abort(); unsubscribe(); };
  }, [attempt]);

  useEffect(() => {
    if (session?.mode !== 'oidc' || !expiresAt || loggingOut) return;
    const delay = Math.max(1000, Date.parse(expiresAt) - Date.now() - 5000);
    const refresh = () => { void refreshSession().catch(() => { setError('Sesi tidak dapat diperbarui. Silakan masuk kembali.'); }); };
    const timer = window.setTimeout(refresh, delay);
    let reconciling = false;
    const reconcile = () => {
      if (document.visibilityState === 'hidden' || reconciling) return;
      reconciling = true;
      void getSession().catch(() => {
        // Scheduled refresh still enforces expiry during a temporary connection failure.
      }).finally(() => { reconciling = false; });
    };
    window.addEventListener('focus', reconcile);
    document.addEventListener('visibilitychange', reconcile);
    return () => { clearTimeout(timer); window.removeEventListener('focus', reconcile); document.removeEventListener('visibilitychange', reconcile); };
  }, [session?.mode, expiresAt, loggingOut]);

  useEffect(() => {
    if (loading || error && !session) return;
    const path = route.split('?')[0];
    const allowed = session ? ['#/dashboard', '#/notifications', '#/webhooks'] : ['#/login'];
    if (!allowed.includes(path)) window.location.replace(allowed[0]);
  }, [route, session, loading, error]);

  function loginDemo(role: Role) {
    if (!isMockDataMode) return;
    const identity = getDemoSession(role);
    try { sessionStorage.setItem(demoRoleKey, role); } catch { /* Preview also works without browser storage. */ }
    setError(''); setSession(identity);
    window.location.hash = '/dashboard';
  }

  async function logout() {
    if (loggingOut) return;
    if (session?.mode === 'demo') {
      try { sessionStorage.removeItem(demoRoleKey); } catch { /* No persistent credentials to clear. */ }
      setSession(null); window.location.replace('#/login');
      return;
    }
    setLoggingOut(true); setError('');
    if (loadPush && session) {
      // Stop delivery to this browser, but keep the preference so the next login resumes push.
      // FCM must never block logout; the service worker also unsubscribes once the session is gone.
      const client = createGatewayClient(session);
      await Promise.race([
        loadPush().then(push => push.disablePush(client, { keepPreference: true })),
        new Promise(resolve => setTimeout(resolve, pushLogoutTimeoutMs)),
      ]).catch(() => undefined);
    }
    try { window.location.assign(await logoutSession()); }
    catch (reason) { setError(reason instanceof Error ? reason.message : 'Logout belum berhasil. Coba lagi.'); }
    finally { setLoggingOut(false); }
  }

  const [path, query = ''] = route.split('?');
  if (loading) return <main className="session-state" role="status"><p>Memeriksa sesi login...</p></main>;
  if (!session) return <>
    {error && <div className="auth-status" role="alert"><p>{error}</p>{!isDemoMode && <button className="button" onClick={() => setAttempt(value => value + 1)}>Periksa sesi lagi</button>}</div>}
    <LoginPage demoEnabled={isDemoMode && isMockDataMode} loginError={route.includes('error=login_failed') ? 'Login belum berhasil. Silakan coba kembali melalui Zitadel.' : undefined} onDemoLogin={loginDemo} onOrganizationLogin={async () => {
      if (isDemoMode) throw new Error('Pratinjau ini tidak terhubung ke Auth. Jalankan compose.auth.yaml untuk login melalui Zitadel.');
      window.location.assign('/auth/login');
    }} />
  </>;
  return <>
    {error && <div className="auth-status" role="alert"><p>{error}</p></div>}
    <DashboardPage
      session={session}
      onLogout={() => { void logout(); }}
      loggingOut={loggingOut}
      activePage={path === '#/notifications' ? 'notifications' : path === '#/webhooks' ? 'webhooks' : 'dashboard'}
      focusHazardId={path === '#/notifications' ? new URLSearchParams(query).get('hazard') ?? undefined : undefined}
    />
  </>;
}

