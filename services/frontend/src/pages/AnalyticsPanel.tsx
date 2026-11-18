import { useEffect, useState } from 'react';
import type { AnalyticsCategory, AnalyticsResult, GatewayClient } from '../types';

const categories: Array<{ key: AnalyticsCategory; label: string; color: string }> = [
  { key: 'SEISMIC', label: 'Gempa', color: '#2563eb' },
  { key: 'TSUNAMI', label: 'Tsunami', color: '#0891b2' },
  { key: 'VOLCANIC', label: 'Gunung berapi', color: '#9333ea' },
  { key: 'WILDFIRE', label: 'Kebakaran hutan', color: '#d97706' },
  { key: 'COASTAL', label: 'Pesisir', color: '#059669' },
];
const format = new Intl.NumberFormat('id-ID');
const day = 86400000;

/** Missing buckets are zero, so the line never bridges an empty event period. */
export function completeAnalyticsSeries(data: Pick<AnalyticsResult, 'series'>, interval: string) {
  const rows = data.series;
  if (!rows.length) return [];
  const output: typeof rows = [];
  const advance = (text: string, direction = 1) => {
    const cursor = new Date(text);
    if (interval === 'year') cursor.setUTCFullYear(cursor.getUTCFullYear() + direction);
    else if (interval === 'month') cursor.setUTCMonth(cursor.getUTCMonth() + direction);
    else cursor.setTime(cursor.getTime() + direction * (interval === 'hour' ? 3600000 : day));
    return cursor.toISOString();
  };
  for (const row of rows) {
    const previous = output[output.length - 1];
    if (previous) {
      const first = advance(previous.bucket), last = advance(row.bucket, -1);
      if (Date.parse(first) < Date.parse(row.bucket)) {
        output.push({ bucket: first, counts: {} });
        if (last !== first) output.push({ bucket: last, counts: {} });
      }
    }
    output.push(row);
  }
  return output;
}

export function AnalyticsPanel({ client, revision }: { client: GatewayClient; revision: number }) {
  const [range, setRange] = useState('30');
  const [interval, setInterval] = useState<'hour' | 'day' | 'month' | 'year'>('day');
  const [data, setData] = useState<AnalyticsResult>();
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(false);
  const [attempt, setAttempt] = useState(0);
  const [areaSearch, setAreaSearch] = useState('');
  const [areaPage, setAreaPage] = useState(0);
  useEffect(() => {
    const controller = new AbortController();
    setLoading(true); setError(''); setData(undefined);
    const until = new Date();
    client.getAnalytics({ interval, ...(range !== 'all' && { since: new Date(until.getTime() - Number(range) * day).toISOString(), until: until.toISOString() }) }, { signal: controller.signal })
      .then(result => { if (!controller.signal.aborted) { setData(result); setAreaPage(0); } })
      .catch((reason: unknown) => { if (!controller.signal.aborted) setError(reason instanceof Error ? reason.message : 'Analitik belum dapat dimuat.'); })
      .finally(() => { if (!controller.signal.aborted) setLoading(false); });
    return () => controller.abort();
  }, [client, range, interval, revision, attempt]);
  const points = data ? completeAnalyticsSeries(data, interval) : [];
  const max = points.reduce((maximum, point) => Math.max(maximum, ...categories.map(category => point.counts[category.key] ?? 0)), 1);
  const firstTime = points.length ? Date.parse(points[0].bucket) : 0;
  const span = points.length ? Math.max(1, Date.parse(points[points.length - 1].bucket) - firstTime) : 1;
  const areas = (data?.areas ?? []).filter(area => area.area_name.toLocaleLowerCase('id').includes(areaSearch.toLocaleLowerCase('id')));
  const dateLabel = (value: string) => new Date(value).toLocaleDateString('id-ID', { timeZone: 'UTC', day: 'numeric', month: 'short', year: 'numeric' });
  return <section className="panel analytics-panel" aria-label="Analitik seluruh kejadian" aria-busy={loading}>
    <div className="panel-heading"><div><h2 id="analytics-heading">Analitik seluruh kejadian</h2><p>Agregasi katalog dari seluruh penyimpanan · waktu UTC</p></div>
      <div className="analytics-controls"><label>Rentang analitik<select value={range} onChange={event => setRange(event.target.value)}><option value="7">7 hari</option><option value="30">30 hari</option><option value="365">1 tahun</option><option value="all">Semua waktu</option></select></label>
        <label>Interval<select value={interval} onChange={event => setInterval(event.target.value as typeof interval)}><option value="hour">Jam</option><option value="day">Hari</option><option value="month">Bulan</option><option value="year">Tahun</option></select></label>
        <button className="button" disabled={loading} onClick={() => setAttempt(value => value + 1)}>Muat analitik</button></div>
    </div>
    {loading && <p role="status">Menghitung seluruh katalog…</p>}
    {error && <p role="alert">{error}</p>}
    {data && <>
      {data.partial && <div className="notice notice-warning" role="alert"><strong>Angka belum lengkap.</strong><span>Sebagian penyimpanan atau pemindaian belum tersedia. Muat ulang sebelum memakai total: {data.unavailable_units.join(', ')}.</span></div>}
      <p><strong>{format.format(data.total)}</strong> kejadian {data.partial ? 'yang berhasil dibaca' : 'dalam rentang terpilih'} · dihitung {new Date(data.as_of).toLocaleString('id-ID', { timeZone: 'UTC' })} UTC</p>
      <div className="analytics-legend">{categories.map(category => <span key={category.key}><i style={{ background: category.color }} />{category.label}: <strong>{format.format(data.categories[category.key] ?? 0)}</strong></span>)}</div>
      <p className="analytics-note">Tsunami adalah kejadian gempa dengan minimal satu peringatan tsunami, dihitung sekali per gempa. Kategori tsunami termasuk dalam gempa; total, severity, dan wilayah menghitung setiap kejadian sekali.</p>
      {points.length ? <figure className="analytics-chart"><svg viewBox="0 0 960 260" role="img" aria-label="Grafik garis jumlah kejadian per jenis bahaya terhadap waktu">
        {[0, 0.5, 1].map(level => <g key={level}><line x1="48" x2="940" y1={220 - level * 190} y2={220 - level * 190} stroke="#dce3ed" /><text x="40" y={224 - level * 190} textAnchor="end" fontSize="12" fill="#56647a">{Math.ceil(max * level)}</text></g>)}
        {categories.map(category => <polyline key={category.key} fill="none" stroke={category.color} strokeWidth="2.5" points={points.map(point => `${48 + (Date.parse(point.bucket) - firstTime) * 892 / span},${220 - (point.counts[category.key] ?? 0) * 190 / max}`).join(' ')} />)}
        {points.length === 1 && categories.map(category => <circle key={category.key} cx="48" cy={220 - (points[0].counts[category.key] ?? 0) * 190 / max} r="3" fill={category.color} />)}
        <text x="48" y="250" fontSize="12" fill="#56647a">{dateLabel(points[0].bucket)}</text><text x="940" y="250" textAnchor="end" fontSize="12" fill="#56647a">{dateLabel(points[points.length - 1].bucket)}</text>
      </svg><figcaption>Jumlah kejadian per {interval === 'hour' ? 'jam' : interval === 'month' ? 'bulan' : interval === 'year' ? 'tahun' : 'hari'}. Periode kosong bernilai nol.</figcaption></figure> : <p>Belum ada kejadian dalam rentang ini.</p>}
      <details><summary>Data grafik per waktu</summary><div className="analytics-table-scroll"><table className="event-table"><thead><tr><th>Waktu UTC</th>{categories.map(category => <th key={category.key}>{category.label}</th>)}</tr></thead><tbody>{points.map(point => <tr key={point.bucket}><td>{point.bucket}</td>{categories.map(category => <td key={category.key}>{format.format(point.counts[category.key] ?? 0)}</td>)}</tr>)}</tbody></table></div></details>
      <div className="analytics-breakdowns"><section><h3>Jumlah per severity</h3><table className="event-table"><thead><tr><th>Tingkat</th><th>Jumlah</th></tr></thead><tbody>{['NORMAL', 'WASPADA', 'SIAGA', 'AWAS'].map(severity => <tr key={severity}><td>{severity}</td><td>{format.format(data.severity[severity] ?? 0)}</td></tr>)}</tbody></table></section>
        <section><h3>Jumlah per wilayah</h3><label>Cari wilayah analitik<input value={areaSearch} onChange={event => { setAreaSearch(event.target.value); setAreaPage(0); }} /></label><table className="event-table"><thead><tr><th>Wilayah</th><th>Jumlah</th></tr></thead><tbody>{areas.slice(areaPage * 20, (areaPage + 1) * 20).map(area => <tr key={area.area_name}><td>{area.area_name || '(tanpa nama)'}</td><td>{format.format(area.count)}</td></tr>)}</tbody></table><div className="analytics-controls"><button className="button" disabled={areaPage === 0} onClick={() => setAreaPage(value => value - 1)}>Sebelumnya</button><span>{areas.length} wilayah · halaman {areaPage + 1}</span><button className="button" disabled={(areaPage + 1) * 20 >= areas.length} onClick={() => setAreaPage(value => value + 1)}>Berikutnya</button></div></section></div>
    </>}
  </section>;
}
