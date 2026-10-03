import { useEffect, useState } from 'react';
import { getDemoSession } from './auth/demo';
import { isDemoMode } from './config';
import { DashboardPage } from './pages/DashboardPage';
import { LoginPage } from './pages/LoginPage';
import type { AuthSession, Role } from './types';

const demoRoleKey = 'siaga.demo.role';

function restoreDemoSession(): AuthSession | null {
  if (!isDemoMode) return null;
  try {
    const role = sessionStorage.getItem(demoRoleKey);
    return role === 'MEDIA' || role === 'FIELD_TEAM' || role === 'BNPB_INTERNAL' ? getDemoSession(role) : null;
  } catch {
    return null;
  }
}

export default function App() {
  const [session, setSession] = useState(restoreDemoSession);
  const [route, setRoute] = useState(() => window.location.hash);

  useEffect(() => {
    const onHashChange = () => setRoute(window.location.hash);
    window.addEventListener('hashchange', onHashChange);
    return () => window.removeEventListener('hashchange', onHashChange);
  }, []);

  useEffect(() => {
    const allowed = session ? ['#/dashboard', '#/notifications'] : ['#/login'];
    if (!allowed.includes(route)) window.location.replace(allowed[0]);
  }, [route, session]);

  function loginDemo(role: Role) {
    const identity = getDemoSession(role);
    try { sessionStorage.setItem(demoRoleKey, role); } catch { /* Preview also works without browser storage. */ }
    setSession(identity);
    window.location.hash = '/dashboard';
  }

  function logout() {
    try { sessionStorage.removeItem(demoRoleKey); } catch { /* No persistent credentials to clear. */ }
    setSession(null);
    window.location.replace('#/login');
  }

  if (!session) return <LoginPage demoEnabled={isDemoMode} onDemoLogin={loginDemo} onOrganizationLogin={async () => {
    throw new Error('Login organisasi belum terhubung. Integrasi OIDC menunggu konfigurasi Auth. Gunakan akun pratinjau jika mode demo tersedia.');
  }} />;

  return <DashboardPage session={session} onLogout={logout} activePage={route === '#/notifications' ? 'notifications' : 'dashboard'} />;
}
