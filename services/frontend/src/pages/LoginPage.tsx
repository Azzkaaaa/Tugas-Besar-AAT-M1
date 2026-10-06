import { useState } from 'react';
import { ArrowUpRight, CircleAlert, LoaderCircle } from 'lucide-react';
import './login.css';

type LoginRole = 'MEDIA' | 'FIELD_TEAM' | 'BNPB_INTERNAL';

interface LoginPageProps {
  demoEnabled: boolean;
  onDemoLogin: (role: LoginRole) => void;
  onOrganizationLogin: () => Promise<void>;
  loginError?: string;
}

const demoAccounts = [
  { role: 'MEDIA' as const, title: 'Media', username: 'media-demo', description: 'Ringkasan kejadian dan notifikasi' },
  { role: 'FIELD_TEAM' as const, title: 'Tim Lapangan', username: 'lapangan-demo', description: 'Detail kejadian dan data sumber' },
  { role: 'BNPB_INTERNAL' as const, title: 'BNPB Pusat', username: 'bnpb-demo', description: 'Detail kejadian dan data sumber' },
];

export function LoginPage({ demoEnabled, onDemoLogin, onOrganizationLogin, loginError: externalError }: LoginPageProps) {
  const [isSigningIn, setIsSigningIn] = useState(false);
  const [loginError, setLoginError] = useState<string | null>(null);

  async function handleOrganizationLogin() {
    setLoginError(null);
    setIsSigningIn(true);
    try {
      await onOrganizationLogin();
    } catch (error) {
      setLoginError(error instanceof Error ? error.message : 'Login belum dapat diproses. Silakan coba lagi.');
    } finally {
      setIsSigningIn(false);
    }
  }

  return (
    <main className="login-page">
      <section className="login-card" aria-labelledby="login-heading">
        <h1 id="login-heading">Dashboard Kebencanaan</h1>
        <p className="login-introduction">Masuk untuk melihat kejadian dari BMKG dan PVMBG.</p>
        <button className="login-organization-button" type="button" onClick={() => void handleOrganizationLogin()} disabled={isSigningIn} aria-busy={isSigningIn}>
          {isSigningIn ? <><LoaderCircle className="spinning" size={18} />Menghubungkan...</> : <>Masuk dengan Zitadel<ArrowUpRight size={18} /></>}
        </button>
        <p className="login-organization-note">Pengisian akun dan kata sandi dilakukan di halaman login Zitadel.</p>
        {(loginError || externalError) && <div className="login-error" role="alert"><CircleAlert size={18} /><p>{loginError || externalError}</p></div>}

        {demoEnabled && (
          <section className="login-demo-section" aria-labelledby="demo-heading">
            <h2 id="demo-heading">Pratinjau tanpa Auth</h2>
            <p>Identitas simulasi untuk menguji tampilan. Pilihan ini tidak membuat sesi Zitadel.</p>
            <div className="login-account-list">
              {demoAccounts.map(({ role, title, username, description }) => (
                <button key={role} className="login-account" type="button" onClick={() => onDemoLogin(role)} aria-label={`Pratinjau sebagai ${title}, akun ${username}`}>
                  <strong>{title}</strong><span>{description}</span>
                </button>
              ))}
            </div>
          </section>
        )}
      </section>
    </main>
  );
}
