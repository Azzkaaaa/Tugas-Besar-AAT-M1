import { useState } from 'react';
import {
  ArrowRight,
  ArrowUpRight,
  Building2,
  ChevronRight,
  CircleAlert,
  LoaderCircle,
  Newspaper,
  Radio,
  ShieldCheck,
  UsersRound,
} from 'lucide-react';
import './login.css';

type LoginRole = 'MEDIA' | 'FIELD_TEAM' | 'BNPB_INTERNAL';

interface LoginPageProps {
  demoEnabled: boolean;
  onDemoLogin: (role: LoginRole) => void;
  onOrganizationLogin: () => Promise<void>;
}

const demoAccounts = [
  {
    role: 'MEDIA' as const,
    title: 'Media',
    username: 'media-demo',
    description: 'Ringkasan kejadian dan informasi publik',
    icon: Newspaper,
  },
  {
    role: 'FIELD_TEAM' as const,
    title: 'Tim Lapangan',
    username: 'lapangan-demo',
    description: 'Detail kejadian, koordinat, dan data sumber',
    icon: UsersRound,
  },
  {
    role: 'BNPB_INTERNAL' as const,
    title: 'BNPB Pusat',
    username: 'bnpb-demo',
    description: 'Informasi lengkap untuk koordinasi',
    icon: Building2,
  },
];

function ConnectionVisual() {
  return (
    <div className="login-connection" aria-hidden="true">
      <svg className="login-connection-svg" viewBox="0 0 520 235" fill="none">
        <defs>
          <linearGradient id="login-line-gradient" x1="80" y1="160" x2="445" y2="75" gradientUnits="userSpaceOnUse">
            <stop stopColor="#7facc3" stopOpacity="0.18" />
            <stop offset="0.5" stopColor="#e86932" stopOpacity="0.8" />
            <stop offset="1" stopColor="#7facc3" stopOpacity="0.18" />
          </linearGradient>
        </defs>
        <ellipse cx="260" cy="122" rx="204" ry="91" stroke="#9fb7c5" strokeOpacity="0.09" />
        <ellipse cx="260" cy="122" rx="164" ry="72" stroke="#9fb7c5" strokeOpacity="0.1" />
        <ellipse cx="260" cy="122" rx="122" ry="53" stroke="#9fb7c5" strokeOpacity="0.1" />
        <path d="M83 147L259 122L435 71M83 147L256 200L435 71M259 122L256 200" stroke="url(#login-line-gradient)" />
        <path d="M83 147L259 122L435 71" stroke="#dfe7ea" strokeOpacity="0.5" strokeDasharray="3 7" />
        <circle cx="260" cy="122" r="32" fill="#e86932" fillOpacity="0.08" stroke="#e86932" strokeOpacity="0.22" />
        <circle cx="260" cy="122" r="21" fill="#e86932" fillOpacity="0.12" stroke="#e86932" strokeOpacity="0.55" />
        <path d="M248 123L255 116L262 123L272 112M248 131L255 124L262 131L272 120" stroke="#f29060" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" />
        <circle cx="83" cy="147" r="5" fill="#a5c3d1" />
        <circle cx="83" cy="147" r="13" stroke="#a5c3d1" strokeOpacity="0.3" />
        <circle cx="435" cy="71" r="5" fill="#a5c3d1" />
        <circle cx="435" cy="71" r="13" stroke="#a5c3d1" strokeOpacity="0.3" />
        <circle cx="256" cy="200" r="3" fill="#e86932" fillOpacity="0.65" />
        <text x="42" y="115" fill="#dce7ed" fontSize="12" letterSpacing="2" fontFamily="inherit">BMKG</text>
        <text x="387" y="38" fill="#dce7ed" fontSize="12" letterSpacing="2" fontFamily="inherit">PVMBG</text>
        <text x="241" y="172" fill="#f29060" fontSize="10" letterSpacing="2" fontFamily="inherit">BNPB</text>
        <text x="294" y="210" fill="#8396a7" fontSize="9" letterSpacing="1.5" fontFamily="inherit">SATU INFORMASI BERSAMA</text>
      </svg>
    </div>
  );
}

export function LoginPage({ demoEnabled, onDemoLogin, onOrganizationLogin }: LoginPageProps) {
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
      <section className="login-story" aria-label="Tentang SIAGA">
        <div className="login-topography" aria-hidden="true" />
        <a className="login-brand" href="#/login" aria-label="SIAGA, halaman masuk">
          <span className="login-brand-mark"><Radio size={23} strokeWidth={1.8} /></span>
          <span><strong>SIAGA<span className="login-brand-dot">.</span></strong><small>PLATFORM KOORDINASI KEBENCANAAN</small></span>
        </a>

        <div className="login-story-content">
          <div className="login-overline"><span /> TERHUBUNG UNTUK KESIAPSIAGAAN</div>
          <h1>Informasi terhubung.<br /><span>Respons terarah.</span></h1>
          <p className="login-story-description">Satu ruang pantau untuk memahami situasi, mengikuti perkembangan, dan mendukung koordinasi kebencanaan.</p>
          <ConnectionVisual />
          <div className="login-principles">
            <div><span>01</span><strong>Sumber terpadu</strong><p>Informasi BMKG dan PVMBG dalam satu tampilan.</p></div>
            <div><span>02</span><strong>Konteks yang jelas</strong><p>Kenali tingkat bahaya dan kebaruan setiap informasi.</p></div>
            <div><span>03</span><strong>Respons bersama</strong><p>Informasi sesuai kebutuhan dan peran pengguna.</p></div>
          </div>
        </div>

        <footer className="login-story-footer"><span>BNPB <i /> Pusat informasi kebencanaan</span><span>INDONESIA</span></footer>
      </section>

      <section className="login-access" aria-labelledby="login-heading">
        <div className="login-access-top"><span>RUANG PANTAU KEBENCANAAN</span>{demoEnabled && <span className="login-demo-badge"><span /> Mode demo</span>}</div>

        <div className="login-form-area">
          <span className="login-eyebrow">AKSES PLATFORM</span>
          <h2 id="login-heading">Selamat datang.</h2>
          <p className="login-introduction">Masuk untuk memantau situasi dan mengikuti informasi kebencanaan terkini.</p>

          <button className="login-organization-button" type="button" onClick={() => void handleOrganizationLogin()} disabled={isSigningIn} aria-busy={isSigningIn}>
            <span>{isSigningIn ? <LoaderCircle className="login-spinner" size={19} /> : <Building2 size={19} />} {isSigningIn ? 'Menghubungkan…' : 'Masuk dengan organisasi'}</span>
            {!isSigningIn && <ArrowUpRight size={20} />}
          </button>
          <p className="login-organization-note">Anda akan diarahkan ke layanan login organisasi.</p>
          {loginError && <div className="login-error" role="alert"><CircleAlert size={18} /><p>{loginError}</p></div>}

          {demoEnabled && (
            <div className="login-demo-section">
              <div className="login-divider"><span />Pratinjau pengembangan<span /></div>
              <p className="login-demo-description">Jelajahi dashboard dengan <strong>data dan identitas simulasi</strong>. Pilih peran untuk melihat tampilan yang tersedia.</p>
              <div className="login-account-list">
                {demoAccounts.map(({ role, title, username, description, icon: Icon }) => (
                  <button key={role} className="login-account" type="button" onClick={() => onDemoLogin(role)} aria-label={`Pratinjau sebagai ${title}, akun ${username}`}>
                    <span className={`login-account-icon login-account-icon-${role.toLowerCase()}`}><Icon size={21} strokeWidth={1.7} /></span>
                    <span className="login-account-copy"><span className="login-account-title">{title}<small>{username}</small></span><span className="login-account-description">{description}</span></span>
                    <ChevronRight className="login-account-arrow" size={18} />
                  </button>
                ))}
              </div>
              <div className="login-demo-note"><ArrowRight size={13} /><span>Pratinjau ini tidak membuat sesi login organisasi.</span></div>
            </div>
          )}

          <div className="login-security-note"><ShieldCheck size={19} /><div><strong>Akses sesuai peran</strong><p>Informasi ditampilkan sesuai hak akses pengguna.</p></div></div>
        </div>

        <footer className="login-access-footer"><span>SIAGA · Platform Koordinasi Kebencanaan</span><span>Milestone 1</span></footer>
      </section>
    </main>
  );
}
