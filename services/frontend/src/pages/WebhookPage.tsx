import { useCallback, useEffect, useMemo, useState } from 'react';
import {
  AlertCircle,
  Check,
  Clock3,
  Copy,
  Globe,
  Key,
  LoaderCircle,
  Play,
  Plus,
  RefreshCw,
  Send,
  Shield,
  ShieldAlert,
  ShieldCheck,
  Terminal,
  Trash2,
  TriangleAlert,
  Webhook,
  X,
} from 'lucide-react';
import { canManageWebhooks, canReadFull, getAllowedWebhookViews, getDefaultWebhookView, roleLabels } from '../auth/demo';
import type {
  AuthSession,
  CreateWebhookPayload,
  GatewayClient,
  HazardType,
  PingWebhookResult,
  Severity,
  Source,
  WebhookDelivery,
  WebhookSubscription,
  WebhookView,
} from '../types';

const defaultDockerUrl = 'http://host.docker.internal:9098/webhook';

const fullDateFormatter = new Intl.DateTimeFormat('id-ID', {
  day: 'numeric',
  month: 'short',
  year: 'numeric',
  hour: '2-digit',
  minute: '2-digit',
  second: '2-digit',
  timeZone: 'Asia/Jakarta',
});
const formatFull = (value: string) => `${fullDateFormatter.format(new Date(value))} WIB`;

interface DeliveryDialogProps {
  webhook: WebhookSubscription;
  client: GatewayClient;
  onClose: () => void;
}

function DeliveryDialog({ webhook, client, onClose }: DeliveryDialogProps) {
  const [deliveries, setDeliveries] = useState<WebhookDelivery[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');

  const loadDeliveries = useCallback(() => {
    setLoading(true);
    setError('');
    client
      .listWebhookDeliveries(webhook.webhook_id)
      .then((data) => setDeliveries(data.items))
      .catch((err: unknown) => setError(err instanceof Error ? err.message : 'Gagal memuat riwayat pengiriman.'))
      .finally(() => setLoading(false));
  }, [client, webhook.webhook_id]);

  useEffect(() => {
    loadDeliveries();
  }, [loadDeliveries]);

  return (
    <dialog className="detail-dialog" open onClick={(e) => { if (e.target === e.currentTarget) onClose(); }}>
      <div className="detail-head">
        <span className="eyebrow">RIWAYAT PENGIRIMAN WEBHOOK</span>
        <button className="icon-button" aria-label="Tutup log pengiriman" onClick={onClose}>
          <X size={20} />
        </button>
      </div>
      <h2>Pengiriman untuk Webhook</h2>
      <p className="mono" style={{ fontSize: '0.85rem', color: 'var(--color-muted, #94a3b8)', marginBottom: '1rem' }}>
        {webhook.url} ({webhook.webhook_id})
      </p>

      {loading && (
        <div className="state-panel" role="status">
          <LoaderCircle className="spinning" size={24} />
          <p>Memuat riwayat pengiriman webhook…</p>
        </div>
      )}

      {error && (
        <div className="inline-error" role="alert">
          <p>{error}</p>
          <button className="button" onClick={loadDeliveries}>Coba lagi</button>
        </div>
      )}

      {!loading && !error && (
        <>
          {deliveries.length === 0 ? (
            <div className="state-panel">
              <Clock3 size={28} />
              <h3>Belum ada pengiriman</h3>
              <p>Pengiriman akan dicatat otomatis saat webhook menerima notifikasi bahaya atau uji coba ping.</p>
            </div>
          ) : (
            <div style={{ overflowX: 'auto', marginTop: '1rem' }}>
              <table className="event-table">
                <thead>
                  <tr>
                    <th scope="col">STATUS</th>
                    <th scope="col">KODE HTTP</th>
                    <th scope="col">DURASI</th>
                    <th scope="col">PERCOBAAN</th>
                    <th scope="col">WAKTU PENGIRIMAN</th>
                  </tr>
                </thead>
                <tbody>
                  {deliveries.map((del) => (
                    <tr key={del.delivery_id}>
                      <td>
                        <span className={`severity severity-${del.status === 'SUCCESS' ? 'normal' : 'awas'}`}>
                          <span />
                          {del.status}
                        </span>
                      </td>
                      <td>
                        <strong className="mono">{del.status_code || '—'}</strong>
                      </td>
                      <td>{del.duration_ms} ms</td>
                      <td>Percobaan #{del.attempt}</td>
                      <td>
                        <time title={del.delivered_at}>{formatFull(del.delivered_at)}</time>
                        {del.error_message && (
                          <div style={{ color: 'var(--color-danger, #ef4444)', fontSize: '0.75rem', marginTop: '2px' }}>
                            {del.error_message}
                          </div>
                        )}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </>
      )}

      <div style={{ marginTop: '1.5rem', display: 'flex', justifyContent: 'flex-end', gap: '0.5rem' }}>
        <button className="button" onClick={loadDeliveries} disabled={loading}>
          <RefreshCw size={15} className={loading ? 'spinning' : ''} />
          Segarkan
        </button>
        <button className="button" onClick={onClose}>
          Tutup
        </button>
      </div>
    </dialog>
  );
}

interface PingModalProps {
  webhook: WebhookSubscription;
  result: PingWebhookResult | null;
  loading: boolean;
  error: string;
  onClose: () => void;
  onRePing: () => void;
}

function PingModal({ webhook, result, loading, error, onClose, onRePing }: PingModalProps) {
  return (
    <dialog className="detail-dialog" open onClick={(e) => { if (e.target === e.currentTarget) onClose(); }}>
      <div className="detail-head">
        <span className="eyebrow">UJI COBA PING WEBHOOK</span>
        <button className="icon-button" aria-label="Tutup hasil uji coba" onClick={onClose}>
          <X size={20} />
        </button>
      </div>
      <h2>Hasil Ping Webhook</h2>
      <p className="mono" style={{ fontSize: '0.85rem', color: 'var(--color-muted, #94a3b8)', marginBottom: '1.25rem' }}>
        {webhook.url}
      </p>

      {loading && (
        <div className="state-panel" role="status">
          <LoaderCircle className="spinning" size={26} />
          <h3>Mengirim payload uji coba…</h3>
          <p>Menghubungi endpoint via gRPC WebhookService/PingWebhook</p>
        </div>
      )}

      {error && (
        <div className="error-panel" role="alert">
          <TriangleAlert size={22} />
          <div>
            <strong>Uji coba webhook gagal</strong>
            <p>{error}</p>
            {error.includes('connection refused') && (
              <small style={{ display: 'block', marginTop: '0.5rem', color: 'var(--color-muted, #94a3b8)' }}>
                <strong>Petunjuk:</strong> Endpoint belum aktif pada port tujuan. Pastikan script listener lokal (<code>python tools/webhook_listener.py</code>) sedang berjalan di terminal Anda, atau gunakan URL endpoint aktif seperti <code>webhook.site</code>.
              </small>
            )}
          </div>
        </div>
      )}

      {result && !loading && (
        <div style={{ display: 'flex', flexDirection: 'column', gap: '1rem' }}>
          <div className={`notice ${result.success ? 'notice-success' : 'notice-warning'}`} style={{ display: 'flex', alignItems: 'center', gap: '0.75rem', padding: '1rem', borderRadius: '8px', background: result.success ? 'rgba(34, 197, 94, 0.1)' : 'rgba(239, 68, 68, 0.1)', border: result.success ? '1px solid #22c55e' : '1px solid #ef4444' }}>
            {result.success ? <ShieldCheck size={24} color="#22c55e" /> : <AlertCircle size={24} color="#ef4444" />}
            <div>
              <strong style={{ fontSize: '1rem', color: result.success ? '#22c55e' : '#ef4444' }}>
                {result.success ? 'Pengiriman Berhasil (Status 2xx)' : 'Pengiriman Tidak Berhasil'}
              </strong>
              <p style={{ margin: 0, fontSize: '0.875rem' }}>{result.message || 'Payload uji coba berhasil dikirim dan direspons oleh endpoint.'}</p>
              {!result.success && result.message?.includes('connection refused') && (
                <small style={{ display: 'block', marginTop: '0.35rem', color: 'var(--color-muted, #94a3b8)' }}>
                  Pastikan listener lokal (<code>python tools/webhook_listener.py</code>) aktif di mesin Anda pada port yang sesuai.
                </small>
              )}
            </div>
          </div>


          <dl className="detail-fields">
            <div>
              <dt>Status Code</dt>
              <dd className="mono"><strong>HTTP {result.status_code}</strong></dd>
            </div>
            <div>
              <dt>Latency / Durasi</dt>
              <dd>{result.duration_ms} ms</dd>
            </div>
            <div>
              <dt>ID Webhook</dt>
              <dd className="mono">{webhook.webhook_id}</dd>
            </div>
            <div>
              <dt>Mode Tampilan (View)</dt>
              <dd><span className="source-tag">{webhook.view === 'full' ? 'VIEW_FULL' : 'VIEW_SUMMARY'}</span></dd>
            </div>
          </dl>
        </div>
      )}

      <div style={{ marginTop: '1.5rem', display: 'flex', justifyContent: 'flex-end', gap: '0.5rem' }}>
        <button className="button" onClick={onRePing} disabled={loading}>
          <Send size={15} />
          Uji Lagi (Ping)
        </button>
        <button className="button" onClick={onClose}>
          Tutup
        </button>
      </div>
    </dialog>
  );
}

export function WebhookPage({ session, client }: { session: AuthSession; client: GatewayClient }) {
  const [webhooks, setWebhooks] = useState<WebhookSubscription[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const [creating, setCreating] = useState(false);
  const [createSuccess, setCreateSuccess] = useState('');
  const [copiedId, setCopiedId] = useState<string>();

  // Webhook Creation Form
  const allowedViews = useMemo(() => getAllowedWebhookViews(session), [session]);
  const defaultView = useMemo(() => getDefaultWebhookView(session), [session]);
  const [url, setUrl] = useState(defaultDockerUrl);
  const [secret, setSecret] = useState('');
  const [description, setDescription] = useState('Local Webhook Listener');
  const [view, setView] = useState<WebhookView>(defaultView);
  const [filterSource, setFilterSource] = useState<Source | ''>('');
  const [filterHazard, setFilterHazard] = useState<HazardType | ''>('');
  const [filterSeverity, setFilterSeverity] = useState<Severity | ''>('');

  // Ping Modal State
  const [pingingWebhook, setPingingWebhook] = useState<WebhookSubscription | null>(null);
  const [pingLoading, setPingLoading] = useState(false);
  const [pingResult, setPingResult] = useState<PingWebhookResult | null>(null);
  const [pingError, setPingError] = useState('');

  // Delivery Dialog State
  const [deliveryWebhook, setDeliveryWebhook] = useState<WebhookSubscription | null>(null);

  // Deleting State
  const [deletingId, setDeletingId] = useState<string>();

  const canWrite = canManageWebhooks(session);
  const canFull = canReadFull(session) || session.role === 'FIELD_TEAM' || session.role === 'BNPB_INTERNAL';

  // Ensure view remains valid if session changes
  useEffect(() => {
    if (!allowedViews.includes(view)) {
      setView(defaultView);
    }
  }, [allowedViews, defaultView, view]);

  const loadWebhooks = useCallback(() => {
    setLoading(true);
    setError('');
    client
      .listWebhooks()
      .then((data) => setWebhooks(data.items))
      .catch((err: unknown) => setError(err instanceof Error ? err.message : 'Gagal mengambil daftar webhook.'))
      .finally(() => setLoading(false));
  }, [client]);

  useEffect(() => {
    loadWebhooks();
  }, [loadWebhooks]);

  const handleCreateWebhook = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!url.trim()) return;

    // Strict derivation of view based on session role
    const derivedView: WebhookView = canFull ? view : 'summary';

    setCreating(true);
    setError('');
    setCreateSuccess('');

    const payload: CreateWebhookPayload = {
      url: url.trim(),
      description: description.trim() || undefined,
      secret: secret.trim() || undefined,
      view: derivedView,
      filter: {
        ...(filterSource ? { source: filterSource } : {}),
        ...(filterHazard ? { hazard_type: filterHazard } : {}),
        ...(filterSeverity ? { severity: filterSeverity } : {}),
      },
    };

    try {
      const created = await client.createWebhook(payload);
      setCreateSuccess(`Webhook berhasil didaftarkan dengan ID: ${created.webhook_id}`);
      loadWebhooks();
      // Reset optional fields
      setSecret('');
    } catch (err: unknown) {
      setError(err instanceof Error ? err.message : 'Gagal mendaftarkan webhook.');
    } finally {
      setCreating(false);
    }
  };

  const handleDeleteWebhook = async (id: string) => {
    if (!confirm('Apakah Anda yakin ingin menghapus langganan webhook ini (Unhook)?')) return;
    setDeletingId(id);
    setError('');
    try {
      await client.deleteWebhook(id);
      setWebhooks((prev) => prev.filter((w) => w.webhook_id !== id));
    } catch (err: unknown) {
      setError(err instanceof Error ? err.message : 'Gagal menghapus webhook.');
    } finally {
      setDeletingId(undefined);
    }
  };

  const handlePingWebhook = async (webhook: WebhookSubscription) => {
    setPingingWebhook(webhook);
    setPingLoading(true);
    setPingResult(null);
    setPingError('');

    try {
      const res = await client.pingWebhook(webhook.webhook_id);
      setPingResult(res);
    } catch (err: unknown) {
      setPingError(err instanceof Error ? err.message : 'Uji coba ping gagal.');
    } finally {
      setPingLoading(false);
    }
  };

  const copyToClipboard = (text: string, id: string) => {
    navigator.clipboard.writeText(text);
    setCopiedId(id);
    setTimeout(() => setCopiedId(undefined), 2000);
  };

  return (
    <div className="webhook-page">
      {/* Role & Permissions Banner */}
      <div
        className="notice"
        style={{
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between',
          gap: '1rem',
          marginBottom: '1.5rem',
          padding: '1rem 1.25rem',
          borderRadius: '10px',
          background: canFull ? 'rgba(59, 130, 246, 0.08)' : 'rgba(234, 179, 8, 0.08)',
          border: canFull ? '1px solid rgba(59, 130, 246, 0.3)' : '1px solid rgba(234, 179, 8, 0.3)',
        }}
      >
        <div style={{ display: 'flex', alignItems: 'center', gap: '0.85rem' }}>
          {canFull ? (
            <ShieldCheck size={22} color="#3b82f6" />
          ) : (
            <ShieldAlert size={22} color="#eab308" />
          )}
          <div>
            <strong>
              Hak Akses Webhook: Peran {roleLabels[session.role]} ({session.role})
            </strong>
            <p style={{ margin: 0, fontSize: '0.875rem' }}>
              {canFull ? (
                <>
                  Akun Anda berhak mengakses proyeksi <strong>VIEW_FULL</strong> (Data Lengkap) serta <strong>VIEW_SUMMARY</strong> untuk pengiriman payload webhook.
                </>
              ) : (
                <>
                  Peran Media dibatasi hanya dapat menggunakan proyeksi <strong>VIEW_SUMMARY</strong> (Ringkasan Publik) sesuai spesifikasi keamanan BNPB.
                </>
              )}
            </p>
          </div>
        </div>
        <span className="view-label" style={{ whiteSpace: 'nowrap' }}>
          <Shield size={14} />
          {canFull ? 'VIEW_FULL Authorized' : 'VIEW_SUMMARY Only'}
        </span>
      </div>

      {error && (
        <div className="error-panel" role="alert" style={{ marginBottom: '1.5rem' }}>
          <TriangleAlert size={22} />
          <div>
            <strong>Terjadi Kesalahan</strong>
            <p>{error}</p>
          </div>
        </div>
      )}

      {createSuccess && (
        <div
          className="notice notice-success"
          style={{
            display: 'flex',
            alignItems: 'center',
            gap: '0.75rem',
            padding: '0.875rem 1.25rem',
            borderRadius: '8px',
            background: 'rgba(34, 197, 94, 0.1)',
            border: '1px solid #22c55e',
            marginBottom: '1.5rem',
          }}
        >
          <Check size={18} color="#22c55e" />
          <span style={{ color: '#22c55e', fontWeight: 500 }}>{createSuccess}</span>
        </div>
      )}

      <div className="dashboard-columns">
        {/* Form: Register Webhook (Hook) */}
        <section className="panel" aria-labelledby="hook-heading" style={{ height: 'fit-content' }}>
          <div className="panel-heading">
            <div>
              <h2 id="hook-heading" style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
                <Plus size={18} />
                Daftarkan Webhook (Hook)
              </h2>
              <p>Mendaftarkan endpoint HTTP callback untuk menerima pengiriman data hazard secara otomatis.</p>
            </div>
          </div>

          <form onSubmit={handleCreateWebhook} style={{ display: 'flex', flexDirection: 'column', gap: '1rem', marginTop: '0.5rem' }}>
            <div>
              <label style={{ display: 'block', marginBottom: '0.35rem', fontWeight: 600, fontSize: '0.875rem' }}>
                URL Endpoint Tujuan <span style={{ color: '#ef4444' }}>*</span>
              </label>
              <div style={{ display: 'flex', gap: '0.5rem' }}>
                <input
                  type="text"
                  required
                  placeholder="http://host.docker.internal:9098/webhook"
                  value={url}
                  onChange={(e) => setUrl(e.target.value)}
                  style={{
                    flex: 1,
                    padding: '0.6rem 0.85rem',
                    borderRadius: '6px',
                    border: '1px solid var(--color-border, #334155)',
                    background: 'var(--color-input-bg, #0f172a)',
                    color: 'inherit',
                    fontFamily: 'monospace',
                    fontSize: '0.875rem',
                  }}
                />
                <button
                  type="button"
                  className="button"
                  onClick={() => setUrl(defaultDockerUrl)}
                  title="Gunakan URL standar Docker test listener"
                  style={{ whiteSpace: 'nowrap', fontSize: '0.8rem' }}
                >
                  Docker URL
                </button>
              </div>
              <small style={{ display: 'block', color: 'var(--color-muted, #94a3b8)', marginTop: '0.25rem' }}>
                Gunakan <code>http://host.docker.internal:9098/webhook</code> agar container Docker dapat memanggil listener di host.
              </small>
            </div>

            <div>
              <label style={{ display: 'block', marginBottom: '0.35rem', fontWeight: 600, fontSize: '0.875rem' }}>
                Deskripsi
              </label>
              <input
                type="text"
                placeholder="Contoh: Local Automated Webhook Runner"
                value={description}
                onChange={(e) => setDescription(e.target.value)}
                style={{
                  width: '100%',
                  padding: '0.6rem 0.85rem',
                  borderRadius: '6px',
                  border: '1px solid var(--color-border, #334155)',
                  background: 'var(--color-input-bg, #0f172a)',
                  color: 'inherit',
                  fontSize: '0.875rem',
                }}
              />
            </div>

            <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '1rem' }}>
              <div>
                <label style={{ display: 'block', marginBottom: '0.35rem', fontWeight: 600, fontSize: '0.875rem' }}>
                  HMAC Secret (Opsional)
                </label>
                <div style={{ display: 'flex', gap: '0.5rem' }}>
                  <input
                    type="text"
                    placeholder="my-test-hmac-secret-123"
                    value={secret}
                    onChange={(e) => setSecret(e.target.value)}
                    style={{
                      flex: 1,
                      padding: '0.6rem 0.85rem',
                      borderRadius: '6px',
                      border: '1px solid var(--color-border, #334155)',
                      background: 'var(--color-input-bg, #0f172a)',
                      color: 'inherit',
                      fontSize: '0.875rem',
                    }}
                  />
                  <button
                    type="button"
                    className="button"
                    onClick={() => setSecret(`sec-${Math.random().toString(36).slice(2, 10)}${Math.random().toString(36).slice(2, 8)}`)}
                    title="Generate secret acak"
                  >
                    <Key size={14} />
                  </button>
                </div>
              </div>

              <div>
                <label style={{ display: 'block', marginBottom: '0.35rem', fontWeight: 600, fontSize: '0.875rem' }}>
                  Proyeksi Data (View) <span style={{ color: '#ef4444' }}>*</span>
                </label>
                <select
                  value={view}
                  disabled={!canFull}
                  onChange={(e) => setView(e.target.value as WebhookView)}
                  style={{
                    width: '100%',
                    padding: '0.6rem 0.85rem',
                    borderRadius: '6px',
                    border: '1px solid var(--color-border, #334155)',
                    background: 'var(--color-input-bg, #0f172a)',
                    color: 'inherit',
                    fontSize: '0.875rem',
                    cursor: canFull ? 'pointer' : 'not-allowed',
                    opacity: canFull ? 1 : 0.8,
                  }}
                >
                  <option value="summary">VIEW_SUMMARY (Ringkasan Publik)</option>
                  {canFull && <option value="full">VIEW_FULL (Data Lengkap & Koordinat)</option>}
                </select>
                {!canFull && (
                  <small style={{ display: 'block', color: 'var(--color-muted, #94a3b8)', marginTop: '0.25rem' }}>
                    Terkunci ke VIEW_SUMMARY untuk peran Media.
                  </small>
                )}
              </div>
            </div>

            {/* Optional Hazard Filters */}
            <div style={{ borderTop: '1px solid var(--color-border, #334155)', paddingTop: '0.85rem' }}>
              <span style={{ fontSize: '0.8rem', fontWeight: 600, color: 'var(--color-muted, #94a3b8)', textTransform: 'uppercase', letterSpacing: '0.05em' }}>
                Filter Kejadian (Opsional)
              </span>
              <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: '0.5rem', marginTop: '0.5rem' }}>
                <select
                  value={filterSource}
                  onChange={(e) => setFilterSource(e.target.value as Source | '')}
                  style={{
                    padding: '0.5rem',
                    borderRadius: '6px',
                    border: '1px solid var(--color-border, #334155)',
                    background: 'var(--color-input-bg, #0f172a)',
                    color: 'inherit',
                    fontSize: '0.8rem',
                  }}
                >
                  <option value="">Semua Sumber</option>
                  <option value="BMKG">BMKG</option>
                  <option value="PVMBG">PVMBG</option>
                </select>

                <select
                  value={filterHazard}
                  onChange={(e) => setFilterHazard(e.target.value as HazardType | '')}
                  style={{
                    padding: '0.5rem',
                    borderRadius: '6px',
                    border: '1px solid var(--color-border, #334155)',
                    background: 'var(--color-input-bg, #0f172a)',
                    color: 'inherit',
                    fontSize: '0.8rem',
                  }}
                >
                  <option value="">Semua Tipe</option>
                  <option value="SEISMIC">Seismik (Gempa)</option>
                  <option value="VOLCANIC">Vulkanik (Gunung)</option>
                </select>

                <select
                  value={filterSeverity}
                  onChange={(e) => setFilterSeverity(e.target.value as Severity | '')}
                  style={{
                    padding: '0.5rem',
                    borderRadius: '6px',
                    border: '1px solid var(--color-border, #334155)',
                    background: 'var(--color-input-bg, #0f172a)',
                    color: 'inherit',
                    fontSize: '0.8rem',
                  }}
                >
                  <option value="">Semua Tingkat</option>
                  <option value="NORMAL">Normal</option>
                  <option value="WASPADA">Waspada</option>
                  <option value="SIAGA">Siaga</option>
                  <option value="AWAS">Awas</option>
                </select>
              </div>
            </div>

            <button
              type="submit"
              className="button"
              disabled={creating || !canWrite}
              style={{
                marginTop: '0.5rem',
                justifyContent: 'center',
                background: 'var(--color-primary, #3b82f6)',
                color: '#ffffff',
                fontWeight: 600,
                padding: '0.75rem',
              }}
            >
              {creating ? (
                <>
                  <LoaderCircle size={16} className="spinning" />
                  Mendaftarkan Webhook…
                </>
              ) : (
                <>
                  <Webhook size={16} />
                  Hubungkan Webhook (Hook)
                </>
              )}
            </button>
          </form>
        </section>

        {/* List of Registered Webhooks */}
        <section className="panel" aria-labelledby="list-heading">
          <div className="panel-heading">
            <div>
              <h2 id="list-heading" style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
                <Globe size={18} />
                Daftar Langganan Webhook
                <span className="count-pill">{webhooks.length}</span>
              </h2>
              <p>Endpoint aktif yang menerima push hazard saat event diterbitkan.</p>
            </div>
            <button className="button" onClick={loadWebhooks} disabled={loading} title="Segarkan daftar">
              <RefreshCw size={15} className={loading ? 'spinning' : ''} />
              Segarkan
            </button>
          </div>

          <div className="event-table-container" aria-busy={loading} style={{ minHeight: '260px' }}>
            {loading && webhooks.length === 0 ? (
              <div className="state-panel" role="status">
                <LoaderCircle className="spinning" size={26} />
                <h3>Memuat daftar webhook…</h3>
              </div>
            ) : webhooks.length === 0 ? (
              <div className="state-panel">
                <Webhook size={32} />
                <h3>Belum ada webhook terdaftar</h3>
                <p>Gunakan formulir di sebelah kiri untuk mendaftarkan endpoint webhook pertama Anda.</p>
              </div>
            ) : (
              <table className={`event-table ${loading ? 'is-loading' : ''}`}>
                <thead>
                  <tr>
                    <th scope="col">ENDPOINT / URL</th>
                    <th scope="col">VIEW</th>
                    <th scope="col">STATUS</th>
                    <th scope="col">TERDAFTAR</th>
                    <th scope="col" style={{ textAlign: 'right' }}>AKSI</th>
                  </tr>
                </thead>
                <tbody>
                  {webhooks.map((sub) => (
                    <tr key={sub.webhook_id}>
                      <td>
                        <div style={{ display: 'flex', flexDirection: 'column', gap: '0.2rem' }}>
                          <strong className="mono" style={{ fontSize: '0.9rem', wordBreak: 'break-all' }}>
                            {sub.url}
                          </strong>
                          <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', color: 'var(--color-muted, #94a3b8)', fontSize: '0.75rem' }}>
                            <span>ID: {sub.webhook_id.slice(0, 16)}…</span>
                            <button
                              className="icon-button"
                              onClick={() => copyToClipboard(sub.webhook_id, sub.webhook_id)}
                              title="Salin Webhook ID"
                              style={{ width: '20px', height: '20px', padding: 0 }}
                            >
                              {copiedId === sub.webhook_id ? <Check size={12} color="#22c55e" /> : <Copy size={12} />}
                            </button>
                            {sub.description && <span>· {sub.description}</span>}
                          </div>
                          {(sub.filter?.source || sub.filter?.hazard_type || sub.filter?.severity) && (
                            <div style={{ display: 'flex', gap: '0.35rem', marginTop: '0.2rem' }}>
                              {sub.filter.source && <span className="source-tag" style={{ fontSize: '0.7rem' }}>{sub.filter.source}</span>}
                              {sub.filter.hazard_type && <span className="source-tag" style={{ fontSize: '0.7rem' }}>{sub.filter.hazard_type}</span>}
                              {sub.filter.severity && <span className="source-tag" style={{ fontSize: '0.7rem' }}>{sub.filter.severity}</span>}
                            </div>
                          )}
                        </div>
                      </td>
                      <td>
                        <span
                          className={`source-tag ${sub.view === 'full' ? 'source-bmkg' : ''}`}
                          style={{
                            fontWeight: 600,
                            letterSpacing: '0.02em',
                            fontSize: '0.75rem',
                          }}
                        >
                          {sub.view === 'full' ? 'VIEW_FULL' : 'VIEW_SUMMARY'}
                        </span>
                      </td>
                      <td>
                        <span className="live-status connected" style={{ fontSize: '0.8rem' }}>
                          <span />
                          {sub.is_active ? 'Aktif' : 'Nonaktif'}
                        </span>
                      </td>
                      <td>
                        <time dateTime={sub.created_at} title={formatFull(sub.created_at)} style={{ fontSize: '0.8rem', color: 'var(--color-muted, #94a3b8)' }}>
                          {formatFull(sub.created_at)}
                        </time>
                      </td>
                      <td>
                        <div style={{ display: 'flex', justifyContent: 'flex-end', gap: '0.4rem' }}>
                          <button
                            className="button"
                            onClick={() => handlePingWebhook(sub)}
                            title="Uji coba ping webhook sekarang"
                            style={{ padding: '0.35rem 0.65rem', fontSize: '0.8rem' }}
                          >
                            <Play size={13} />
                            Ping
                          </button>
                          <button
                            className="button"
                            onClick={() => setDeliveryWebhook(sub)}
                            title="Lihat riwayat pengiriman"
                            style={{ padding: '0.35rem 0.65rem', fontSize: '0.8rem' }}
                          >
                            <Clock3 size={13} />
                            Log
                          </button>
                          <button
                            className="icon-button"
                            onClick={() => handleDeleteWebhook(sub.webhook_id)}
                            disabled={deletingId === sub.webhook_id || !canWrite}
                            aria-label="Hapus webhook (Unhook)"
                            title="Hapus webhook (Unhook)"
                            style={{ color: 'var(--color-danger, #ef4444)' }}
                          >
                            {deletingId === sub.webhook_id ? <LoaderCircle size={15} className="spinning" /> : <Trash2 size={15} />}
                          </button>
                        </div>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>
        </section>
      </div>

      {/* Developer Reference Section: Matches tools/webhook_listener.py */}
      <section className="panel" style={{ marginTop: '1.5rem' }}>
        <div className="panel-heading">
          <div>
            <h3 style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', fontSize: '1rem', margin: 0 }}>
              <Terminal size={17} />
              Referensi Integrasi Python & gRPC
            </h3>
            <p style={{ fontSize: '0.85rem', margin: '0.25rem 0 0' }}>
              Fungsionalitas halaman ini mencerminkan script pengujian <code>tools/webhook_listener.py</code> dan kontrak proto <code>bnpb.webhook.v1.WebhookService</code>.
            </p>
          </div>
        </div>
        <div style={{ padding: '1rem 0 0.5rem' }}>
          <pre
            style={{
              background: 'var(--color-input-bg, #0b1120)',
              padding: '1rem',
              borderRadius: '8px',
              overflowX: 'auto',
              fontSize: '0.8rem',
              color: '#e2e8f0',
              border: '1px solid var(--color-border, #1e293b)',
            }}
          >
            {`# Jalankan test listener lokal di mesin Anda:
python tools/webhook_listener.py

# Atau uji via gRPC (port 9094):
grpcurl -plaintext -H "authorization: Bearer <TOKEN>" \\
  -d '{"url":"http://host.docker.internal:9098/webhook","view":"${canFull ? 'VIEW_FULL' : 'VIEW_SUMMARY'}"}' \\
  localhost:9094 bnpb.webhook.v1.WebhookService/CreateWebhook`}
          </pre>
        </div>
      </section>


      {/* Ping Test Modal */}
      {pingingWebhook && (
        <PingModal
          webhook={pingingWebhook}
          result={pingResult}
          loading={pingLoading}
          error={pingError}
          onClose={() => setPingingWebhook(null)}
          onRePing={() => handlePingWebhook(pingingWebhook)}
        />
      )}

      {/* Delivery Logs Modal */}
      {deliveryWebhook && (
        <DeliveryDialog
          webhook={deliveryWebhook}
          client={client}
          onClose={() => setDeliveryWebhook(null)}
        />
      )}
    </div>
  );
}
