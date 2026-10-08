import type { HazardFull, Severity } from '../types';

const locations = [
  { area: 'Gunung Merapi, DI Yogyakarta', latitude: -7.54, longitude: 110.446, volcano: 'Merapi' },
  { area: 'Laut Banda, Maluku', latitude: -6.37, longitude: 129.84 },
  { area: 'Gunung Semeru, Jawa Timur', latitude: -8.108, longitude: 112.922, volcano: 'Semeru' },
  { area: 'Pesisir Pangandaran, Jawa Barat', latitude: -8.12, longitude: 108.65 },
  { area: 'Gunung Lewotobi Laki-laki, NTT', latitude: -8.542, longitude: 122.775, volcano: 'Lewotobi Laki-laki' },
  { area: 'Kepulauan Mentawai, Sumatera Barat', latitude: -1.82, longitude: 99.38 },
  { area: 'Gunung Ibu, Maluku Utara', latitude: 1.488, longitude: 127.63, volcano: 'Ibu' },
  { area: 'Donggala, Sulawesi Tengah', latitude: -0.58, longitude: 119.61 },
  { area: 'Gunung Anak Krakatau, Lampung', latitude: -6.102, longitude: 105.423, volcano: 'Anak Krakatau' },
  { area: 'Lombok Utara, Nusa Tenggara Barat', latitude: -8.28, longitude: 116.47 },
  { area: 'Gunung Marapi, Sumatera Barat', latitude: -0.381, longitude: 100.473, volcano: 'Marapi' },
  { area: 'Jayapura, Papua', latitude: -2.42, longitude: 140.73 },
];

const severities: Severity[] = ['SIAGA', 'WASPADA', 'AWAS', 'NORMAL', 'SIAGA', 'WASPADA', 'SIAGA', 'NORMAL', 'WASPADA', 'SIAGA', 'NORMAL', 'AWAS'];

export function createFixture(index: number, occurredAt: Date, live = false): HazardFull {
  const location = locations[index % locations.length];
  const volcanic = Boolean(location.volcano);
  const severity = severities[index % severities.length];
  const source = volcanic ? 'PVMBG' : 'BMKG';
  const canonicalId = `haz-demo-${live ? 'live-' : ''}${String(index + 1).padStart(3, '0')}`;
  return {
    hazard_id: canonicalId,
    source,
    hazard_type: volcanic ? 'VOLCANIC' : 'SEISMIC',
    severity,
    area_name: location.area,
    occurred_at: occurredAt.toISOString(),
    ingested_at: new Date(occurredAt.getTime() + 1500).toISOString(),
    source_ref_id: `${source.toLowerCase()}-demo-${index + 1}`,
    latitude: location.latitude,
    longitude: location.longitude,
    attributes: volcanic
      ? {
          volcano_name: location.volcano,
          activity_level: { NORMAL: 1, WASPADA: 2, SIAGA: 3, AWAS: 4 }[severity],
          eruption_count: severity === 'NORMAL' ? 0 : (index % 5) + 1,
          ash_height_m: severity === 'NORMAL' ? 0 : 500 + (index % 8) * 250,
          ...(index % 3 === 0 ? { confidence_level: 0.82 + (index % 4) * 0.03 } : {}),
        }
      : {
          magnitude: Number((4.2 + (index % 7) * 0.4).toFixed(1)),
          depth_km: 10 + (index % 8) * 12,
          tsunami_potential: severity === 'AWAS',
          felt_report: `Laporan guncangan di ${location.area}`,
        },
  };
}

export function createFixtures(now = Date.now()): HazardFull[] {
  return Array.from({ length: 24 }, (_, index) => createFixture(index, new Date(now - 180_000 - index * 660_000)));
}
