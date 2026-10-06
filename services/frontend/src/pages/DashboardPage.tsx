import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { Activity, ArrowDown, ArrowRight, ArrowUpRight, Bell, Check, ChevronLeft, ChevronRight, Clock3, Database, Info, LayoutDashboard, LoaderCircle, LogOut, MapPin, Mountain, Radio, RefreshCw, Search, ShieldCheck, SlidersHorizontal, TriangleAlert, Waves, X } from 'lucide-react';
import { createGatewayClient } from '../api';
import { isMockDataMode } from '../config';
import { canReadFull, roleLabels } from '../auth/demo';
import type { AuthSession, DemoScenario, FeedStatus, Freshness, GatewayClient, Hazard, HazardFilters, HazardPage, HazardResult, Notification, NotificationPage, Severity, Source } from '../types';

const severityLabels: Record<Severity, string> = { NORMAL: 'Normal', WASPADA: 'Waspada', SIAGA: 'Siaga', AWAS: 'Awas' };
const timeFormatter = new Intl.DateTimeFormat('id-ID', { hour: '2-digit', minute: '2-digit', timeZone: 'Asia/Jakarta' });
const dateFormatter = new Intl.DateTimeFormat('id-ID', { day: 'numeric', month: 'short', timeZone: 'Asia/Jakarta' });
const fullDateFormatter = new Intl.DateTimeFormat('id-ID', { day: 'numeric', month: 'long', year: 'numeric', hour: '2-digit', minute: '2-digit', timeZone: 'Asia/Jakarta' });
const formatFull = (value: string) => `${fullDateFormatter.format(new Date(value))} WIB`;

function SeverityBadge({ severity }: { severity: Severity }) {
  return <span className={`severity severity-${severity.toLowerCase()}`}><span />{severityLabels[severity]}</span>;
}

function SourceCard({ source, status, loading }: { source: Source; status?: Freshness; loading: boolean }) {
  const isFresh = status?.status === 'FRESH';
  const Icon = source === 'BMKG' ? Waves : Mountain;
  return <article className="source-card">
    <div className={`source-symbol source-${source.toLowerCase()}`}><Icon size={23} strokeWidth={1.6} /></div>
    <div className="source-copy"><div className="source-title"><strong>{source}</strong><span className={`source-status ${isFresh ? 'fresh' : status ? 'stale' : ''}`}><span />{status ? isFresh ? 'Terkini' : status.status === 'STALE' ? 'Terlambat' : 'Tidak tersedia' : loading ? 'Memuat' : 'Belum diketahui'}</span></div>
      <p>{source === 'BMKG' ? 'Informasi gempa bumi & tsunami' : 'Aktivitas & status gunung api'}</p>
      <small><Clock3 size={12} />{status?.last_success_at ? `Pembaruan ${timeFormatter.format(new Date(status.last_success_at))} WIB` : 'Belum ada pembaruan sumber'}</small>
    </div>
    <span className={`source-check ${isFresh ? 'fresh' : ''}`}>{isFresh ? <Check size={17} /> : <Clock3 size={17} />}</span>
  </article>;
}

function HazardDetail({ client, id, full, onClose }: { client: GatewayClient; id: string; full: boolean; onClose: () => void }) {
  const dialogRef = useRef<HTMLDialogElement>(null);
  const [result, setResult] = useState<HazardResult>();
  const [error, setError] = useState('');
  const [attempt, setAttempt] = useState(0);
  useEffect(() => {
    const dialog = dialogRef.current;
    const previous = document.activeElement as HTMLElement | null;
    dialog?.showModal();
    return () => { dialog?.close(); previous?.focus(); };
  }, []);
  useEffect(() => {
    const controller = new AbortController();
    setError('');
    setResult(undefined);
    client.getHazard(id, full ? 'full' : 'summary', { signal: controller.signal }).then(setResult).catch((reason: unknown) => {
      if (!controller.signal.aborted) setError(reason instanceof Error ? reason.message : 'Detail tidak dapat dimuat.');
    });
    return () => controller.abort();
  }, [client, id, full, attempt]);
  const hazard = result?.item;
  return <dialog ref={dialogRef} className="detail-dialog" aria-labelledby="detail-heading" onCancel={onClose} onClick={event => { if (event.target === event.currentTarget) onClose(); }}>
    <div className="detail-head"><span className="eyebrow">INFORMASI KEJADIAN</span><button className="icon-button" aria-label="Tutup detail" onClick={onClose}><X size={20} /></button></div>
    <h2 id="detail-heading">Detail kejadian</h2>
    {!hazard && !error && <div className="state-panel" role="status"><LoaderCircle className="spinning" size={25} /><p>Memuat detail kejadian…</p></div>}
    {error && <div className="inline-error" role="alert"><p>{error}</p><button className="button" onClick={() => setAttempt(value => value + 1)}>Coba lagi</button></div>}
    {hazard && <>
      <div className="detail-summary"><SeverityBadge severity={hazard.severity} /><span className="source-tag">{hazard.source}</span><h3>{hazard.area_name}</h3><p>{hazard.hazard_type === 'SEISMIC' ? 'Kejadian seismik' : 'Aktivitas vulkanik'}</p></div>
      {result.partial && <div className="notice notice-warning"><TriangleAlert size={17} /><span>Sebagian sumber terlambat. Periksa waktu pembaruan sebelum menggunakan informasi.</span></div>}
      <dl className="detail-fields"><div><dt>Waktu kejadian</dt><dd>{formatFull(hazard.occurred_at)}</dd></div><div><dt>Diterima platform</dt><dd>{formatFull(hazard.ingested_at)}</dd></div><div><dt>ID kejadian</dt><dd className="mono">{hazard.hazard_id}</dd></div>
        {full && 'latitude' in hazard && <><div><dt>Koordinat</dt><dd>{hazard.latitude.toFixed(4)}, {hazard.longitude.toFixed(4)}</dd></div><div><dt>Referensi sumber</dt><dd className="mono">{hazard.source_ref_id}</dd></div></>}
      </dl>
      {full && 'attributes' in hazard ? <section className="attribute-section"><h3>Data sumber</h3><p>Atribut yang disediakan sumber untuk kejadian ini.</p><dl className="attribute-list">{Object.entries(hazard.attributes).map(([key, value]) => <div key={key}><dt>{key.replaceAll('_', ' ')}</dt><dd>{typeof value === 'boolean' ? value ? 'Ya' : 'Tidak' : typeof value === 'object' ? JSON.stringify(value) : String(value)}</dd></div>)}</dl></section> : <div className="notice"><ShieldCheck size={18} /><span>Peran Media menampilkan ringkasan informasi publik.</span></div>}
      {isMockDataMode && <p className="detail-disclaimer">Data simulasi untuk pengembangan. Bukan informasi bencana aktual.</p>}
    </>}
  </dialog>;
}

function NotificationList({ items, onOpen, expanded = false }: { items: Notification[]; onOpen: (id: string) => void; expanded?: boolean }) {
  if (!items.length) return <div className="notification-empty"><Bell size={26} /><p>Belum ada notifikasi</p><small>Notifikasi muncul untuk kejadian Siaga dan Awas.</small></div>;
  return <div className={expanded ? 'notification-list expanded' : 'notification-list'}>{items.map(item => <button key={item.notification_id} className="notification-item" onClick={() => onOpen(item.hazard.hazard_id)} aria-label={`Buka notifikasi ${item.hazard.area_name}`}>
    <span className={`notification-marker marker-${item.hazard.severity.toLowerCase()}`}><TriangleAlert size={16} /></span>
    <span className="notification-copy"><span className="notification-meta">{item.hazard.source}<time>{timeFormatter.format(new Date(item.created_at))} WIB</time></span><strong>{item.hazard.area_name}</strong><span className="notification-message">{item.message}</span><span className="notification-level">{severityLabels[item.hazard.severity]}<ArrowUpRight size={12} /></span></span>
  </button>)}</div>;
}

export function DashboardPage({ session, onLogout, activePage, loggingOut = false }: { session: AuthSession; onLogout: () => void; activePage: 'dashboard' | 'notifications'; loggingOut?: boolean }) {
  const client = useMemo(() => createGatewayClient(session), [session]);
  const full = canReadFull(session);
  const [source, setSource] = useState<Source | ''>('');
  const [severity, setSeverity] = useState<Severity | ''>('');
  const [period, setPeriod] = useState('24');
  const [search, setSearch] = useState('');
  const [scenario, setScenario] = useState<DemoScenario>('normal');
  const [cursors, setCursors] = useState<Array<string | undefined>>([undefined]);
  const [pageIndex, setPageIndex] = useState(0);
  const [revision, setRevision] = useState(0);
  const [data, setData] = useState<HazardPage>();
  const [notifications, setNotifications] = useState<NotificationPage>();
  const [freshness, setFreshness] = useState<Freshness[]>([]);
  const [feedStatus, setFeedStatus] = useState<FeedStatus>('connecting');
  const [hasUpdates, setHasUpdates] = useState(false);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const [updatedAt, setUpdatedAt] = useState<string>();
  const [detailId, setDetailId] = useState<string>();
  // Keep a fixed time boundary while navigating a snapshot; changing filters starts a new one.
  const [filterAnchor, setFilterAnchor] = useState(Date.now);
  const filters: HazardFilters = useMemo(() => ({
    ...(source && { source }), ...(severity && { severity }),
    ...(period !== 'all' && { since: new Date(filterAnchor - Number(period) * 3_600_000).toISOString() }),
    view: full ? 'full' : 'summary', page_size: 8, page_token: cursors[pageIndex],
  }), [source, severity, period, filterAnchor, full, cursors, pageIndex]);

  const refresh = useCallback(() => {
    setCursors([undefined]); setPageIndex(0); setHasUpdates(false); setFilterAnchor(Date.now()); setRevision(value => value + 1);
  }, []);

  useEffect(() => client.subscribe(() => setHasUpdates(true), setFeedStatus, refresh), [client, refresh]);
  useEffect(() => {
    const controller = new AbortController();
    setLoading(true); setError('');
    // Notifications have their own feed; dashboard table filters do not hide alerts.
    Promise.all([
      client.listHazards(filters, { signal: controller.signal }),
      client.listNotifications({ view: full ? 'full' : 'summary', page_size: 100 }, { signal: controller.signal }),
    ]).then(([hazards, alerts]) => {
      if (controller.signal.aborted) return;
      setData(hazards); setNotifications(alerts);
      setUpdatedAt(new Date().toISOString());
    }).catch((reason: unknown) => {
      if (!controller.signal.aborted) setError(reason instanceof Error ? reason.message : 'Data tidak dapat dimuat.');
    }).finally(() => { if (!controller.signal.aborted) setLoading(false); });
    client.getSourceStatuses({ signal: controller.signal }).then(statuses => {
      if (!controller.signal.aborted) setFreshness(statuses.freshness);
    }).catch(() => { if (!controller.signal.aborted) setFreshness([]); });
    return () => controller.abort();
  }, [client, filters, full, revision]);

  function changeScenario(value: DemoScenario) {
    client.setScenario(value); setScenario(value); setDetailId(undefined); setData(undefined); setNotifications(undefined); refresh();
  }
  function changeFilter(callback: () => void) { callback(); setSearch(''); refresh(); }
  function navigate(page: 'dashboard' | 'notifications') { window.location.hash = page === 'dashboard' ? '/dashboard' : '/notifications'; }
  const items = data?.items ?? [];
  const visibleItems = items.filter(item => item.area_name.toLocaleLowerCase('id').includes(search.toLocaleLowerCase('id')));
  const attention = items.filter(item => item.severity === 'SIAGA' || item.severity === 'AWAS').length;
  const seismic = items.filter(item => item.hazard_type === 'SEISMIC').length;
  const alerts = notifications?.items ?? [];
  const partial = data?.partial || notifications?.partial;
  const initialLoading = loading && !data;

  return <div className="app-shell">
    <a className="skip-link" href="#main-content" onClick={event => { event.preventDefault(); document.getElementById('main-content')?.focus(); }}>Langsung ke konten</a>
    <div className="app-body">
      <header className="topbar">
        <nav aria-label="Menu utama">
          <button className={`nav-item ${activePage === 'dashboard' ? 'active' : ''}`} onClick={() => navigate('dashboard')} aria-current={activePage === 'dashboard' ? 'page' : undefined}><LayoutDashboard size={17} />Dashboard</button>
          <button className={`nav-item ${activePage === 'notifications' ? 'active' : ''}`} onClick={() => navigate('notifications')} aria-current={activePage === 'notifications' ? 'page' : undefined}><Bell size={17} />Notifikasi<span className="nav-count">{alerts.length}</span></button>
        </nav>
        <div className="user-area">
          {session.mode === 'demo' && <span className="demo-chip">Akun simulasi</span>}
          <div className="user-identity"><strong>{session.name}</strong><span>{roleLabels[session.role]}</span></div>
          <button className="button logout-button" onClick={onLogout} disabled={loggingOut} aria-label="Keluar"><LogOut size={16} /><span>{loggingOut ? 'Memproses...' : 'Keluar'}</span></button>
        </div>
      </header>

      <main id="main-content" className="dashboard-main" tabIndex={-1}>
        <div className="page-heading">
          <div><h1>{activePage === 'dashboard' ? 'Dashboard Kebencanaan' : 'Notifikasi'}</h1><p>{activePage === 'dashboard' ? 'Daftar kejadian dan status pembaruan sumber BMKG / PVMBG.' : 'Kejadian dengan tingkat bahaya Siaga dan Awas.'}</p></div>
          <div className="heading-actions"><span className={`live-status ${feedStatus === 'connected' ? 'connected' : ''}`}><span />{feedStatus === 'connected' ? isMockDataMode ? 'Feed simulasi aktif' : 'Feed aktif' : feedStatus === 'connecting' ? 'Menghubungkan feed' : 'Feed terputus'}</span><button className="button" onClick={refresh} disabled={loading}><RefreshCw size={15} className={loading ? 'spinning' : ''} />Perbarui</button></div>
        </div>

        {isMockDataMode && <div className="demo-toolbar"><div><Info size={17} /><span><strong>Data simulasi.</strong> Event berasal dari contoh di frontend, bukan dari service BMKG / PVMBG.</span></div><label>Skenario demo<select aria-label="Skenario demo" value={scenario} onChange={event => changeScenario(event.target.value as DemoScenario)}><option value="normal">Normal</option><option value="stale">PVMBG terlambat</option><option value="empty">Data kosong</option><option value="error">Gagal memuat</option></select></label></div>}

        {partial && !error && <div className="notice notice-warning" role="status"><TriangleAlert size={18} /><span><strong>Sebagian data belum diperbarui.</strong> Data terakhir tetap ditampilkan. Periksa status dan waktu pembaruan sumber.</span></div>}
        {hasUpdates && <button className="update-banner" onClick={refresh}><Radio size={16} /><span>Ada pembaruan baru. Muat kejadian terbaru</span><ArrowRight size={16} /></button>}
        {error && <div className="error-panel" role="alert"><TriangleAlert size={22} /><div><strong>Informasi belum dapat dimuat</strong><p>{error}</p>{data && <small>Data sebelumnya disimpan. Perbarui untuk memastikan kebaruannya.</small>}</div><button className="button" onClick={refresh} disabled={loading}>Coba lagi</button></div>}

        <section className="sources-grid" aria-label="Status sumber informasi"><SourceCard source="BMKG" status={freshness.find(item => item.source === 'BMKG')} loading={loading} /><SourceCard source="PVMBG" status={freshness.find(item => item.source === 'PVMBG')} loading={loading} /></section>

        {activePage === 'dashboard' ? <>
          <section className="stats-grid" aria-label="Ringkasan halaman yang dimuat"><article className="stat-card"><span className="stat-icon"><Database size={19} /></span><div><p>Kejadian dimuat</p><strong>{initialLoading ? '—' : items.length}</strong><small>Pada halaman ini</small></div></article><article className="stat-card attention"><span className="stat-icon"><TriangleAlert size={19} /></span><div><p>Perlu perhatian</p><strong>{initialLoading ? '—' : attention}</strong><small>Siaga & Awas pada halaman ini</small></div></article><article className="stat-card"><span className="stat-icon"><Activity size={19} /></span><div><p>Jenis kejadian</p><strong className="stat-split">{initialLoading ? '—' : seismic}<span>seismik</span><i />{initialLoading ? '—' : items.length - seismic}<span>vulkanik</span></strong><small>Pada halaman ini</small></div></article></section>

          <div className="dashboard-columns"><section className="events-panel panel" aria-labelledby="events-heading"><div className="panel-heading"><div><h2 id="events-heading">Daftar kejadian<span className="count-pill">{visibleItems.length}</span></h2><p>Urutan waktu kejadian terbaru</p></div><span className="view-label"><ShieldCheck size={13} />{full ? 'Akses lengkap' : 'Ringkasan publik'}</span></div>
            <div className="filters"><label className="search-field"><Search size={16} /><input aria-label="Cari di halaman ini" placeholder="Cari di halaman ini…" value={search} onChange={event => setSearch(event.target.value)} /></label><div className="filter-selects"><SlidersHorizontal size={15} /><label><span className="sr-only">Sumber</span><select aria-label="Sumber" value={source} onChange={event => changeFilter(() => setSource(event.target.value as Source | ''))}><option value="">Semua sumber</option><option>BMKG</option><option>PVMBG</option></select></label><label><span className="sr-only">Tingkat bahaya</span><select aria-label="Tingkat bahaya" value={severity} onChange={event => changeFilter(() => setSeverity(event.target.value as Severity | ''))}><option value="">Semua tingkat</option>{Object.entries(severityLabels).map(([key, label]) => <option key={key} value={key}>{label}</option>)}</select></label><label><span className="sr-only">Rentang waktu</span><select aria-label="Rentang waktu" value={period} onChange={event => changeFilter(() => setPeriod(event.target.value))}><option value="24">24 jam terakhir</option><option value="6">6 jam terakhir</option><option value="1">1 jam terakhir</option><option value="all">Semua waktu</option></select></label></div></div>
            <div className="event-table-container" aria-busy={loading}>{initialLoading ? <div className="state-panel" role="status"><LoaderCircle className="spinning" size={26} /><h3>Memuat kejadian</h3><p>Mengambil daftar kejadian.</p></div> : visibleItems.length ? <table className={`event-table ${loading ? 'is-loading' : ''}`}><thead><tr><th scope="col">KEJADIAN / LOKASI</th><th scope="col">SUMBER</th><th scope="col">TINGKAT</th><th scope="col">WAKTU <ArrowDown size={11} /></th><th scope="col"><span className="sr-only">Detail</span></th></tr></thead><tbody>{visibleItems.map((item: Hazard) => <tr key={item.hazard_id}><td><div className="event-location"><span className={`event-symbol source-${item.source.toLowerCase()}`}>{item.hazard_type === 'SEISMIC' ? <Activity size={18} /> : <Mountain size={18} />}</span><div><strong>{item.area_name}</strong><small>{item.hazard_type === 'SEISMIC' ? 'Gempa bumi' : 'Aktivitas vulkanik'}</small></div></div></td><td><span className="source-tag">{item.source}</span></td><td><SeverityBadge severity={item.severity} /></td><td><time dateTime={item.occurred_at} title={formatFull(item.occurred_at)}>{timeFormatter.format(new Date(item.occurred_at))}<small>{dateFormatter.format(new Date(item.occurred_at))} · WIB</small></time></td><td><button className="icon-button detail-button" aria-label={`Lihat detail ${item.area_name}`} onClick={() => setDetailId(item.hazard_id)}><ArrowUpRight size={17} /></button></td></tr>)}</tbody></table> : <div className="state-panel"><MapPin size={29} /><h3>{search ? 'Lokasi tidak ditemukan' : error ? 'Data belum tersedia' : 'Belum ada kejadian'}</h3><p>{search ? 'Coba kata lain. Pencarian berlaku pada halaman yang sedang dimuat.' : error ? 'Coba perbarui untuk memuat daftar kejadian.' : 'Tidak ada kejadian yang sesuai dengan filter saat ini.'}</p>{(source || severity || period !== '24' || search) && <button className="button" onClick={() => changeFilter(() => { setSource(''); setSeverity(''); setPeriod('24'); })}>Reset filter</button>}</div>}</div>
            <div className="table-footer"><span>{visibleItems.length} kejadian di halaman {pageIndex + 1}{search && ' · hasil pencarian lokal'}</span><div className="pagination"><button className="icon-button" aria-label="Halaman sebelumnya" disabled={pageIndex === 0 || loading} onClick={() => { setSearch(''); setPageIndex(value => value - 1); }}><ChevronLeft size={17} /></button><span>{pageIndex + 1}</span><button className="icon-button" aria-label="Halaman berikutnya" disabled={!data?.next_page_token || loading} onClick={() => { setSearch(''); setCursors(previous => [...previous.slice(0, pageIndex + 1), data?.next_page_token]); setPageIndex(value => value + 1); }}><ChevronRight size={17} /></button></div></div>
          </section>

          <aside className="alerts-rail"><section className="panel alerts-panel" aria-labelledby="alerts-heading"><div className="panel-heading"><div><h2 id="alerts-heading">Notifikasi terbaru</h2></div></div><NotificationList items={alerts.slice(0, 3)} onOpen={setDetailId} /><button className="all-notifications" onClick={() => navigate('notifications')}>Lihat semua notifikasi<ArrowRight size={15} /></button></section><section className="legend-panel"><div><Info size={16} /><h3>Kenali tingkat bahaya</h3></div><p>Kategori pada data kejadian membantu membaca tingkat perhatian yang diperlukan.</p><div className="legend-levels">{Object.keys(severityLabels).map(key => <SeverityBadge key={key} severity={key as Severity} />)}</div><small>Gunakan informasi resmi untuk tindakan di lapangan.</small></section></aside>
          </div>
        </> : <section className="panel notifications-page" aria-labelledby="notifications-heading"><div className="panel-heading"><div><h2 id="notifications-heading">Notifikasi kejadian<span className="count-pill">{alerts.length}</span></h2><p>Hingga 100 notifikasi terbaru yang dimuat · tidak mengikuti filter tabel kejadian</p></div><Bell size={21} /></div>{loading && !notifications ? <div className="state-panel" role="status"><LoaderCircle size={25} className="spinning" /><p>Memuat notifikasi…</p></div> : <NotificationList items={alerts} onOpen={setDetailId} expanded />}{notifications?.next_page_token && <p className="notification-limit">Ada notifikasi lebih lama. Tampilan ini memuat 100 notifikasi terbaru.</p>}</section>}

        <footer className="dashboard-footer"><span>{updatedAt ? `Terakhir dimuat ${timeFormatter.format(new Date(updatedAt))} WIB` : 'Menunggu data'}</span>{isMockDataMode && <span>Data contoh untuk pengembangan</span>}</footer>
      </main>
    </div>
    {detailId && <HazardDetail key={detailId} client={client} id={detailId} full={full} onClose={() => setDetailId(undefined)} />}
  </div>;
}
