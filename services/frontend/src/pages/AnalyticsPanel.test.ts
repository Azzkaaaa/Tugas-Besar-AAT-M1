import { describe, expect, it } from 'vitest';
import { completeAnalyticsSeries } from './AnalyticsPanel';
import type { AnalyticsResult } from '../types';

describe('analytics time series', () => {
  it('preserves distant history while showing empty periods as zero', () => {
    const data: Pick<AnalyticsResult, 'series'> = { series: [{ bucket: '1900-01-01T00:00:00Z', counts: { SEISMIC: 4 } }, { bucket: '2026-11-18T00:00:00Z', counts: { COASTAL: 2 } }] };
    const points = completeAnalyticsSeries(data, 'day');
    expect(points).toHaveLength(4);
    expect(points[1].bucket).toBe('1900-01-02T00:00:00.000Z');
    expect(points[2].bucket).toBe('2026-11-17T00:00:00.000Z');
    expect(points[1].counts).toEqual({});
    expect(points.at(-1)?.counts.COASTAL).toBe(2);
  });
  it('uses calendar months across leap years and inserts no duplicate buckets', () => {
    const data: Pick<AnalyticsResult, 'series'> = { series: [{ bucket: '2024-01-01T00:00:00Z', counts: {} }, { bucket: '2024-03-01T00:00:00Z', counts: {} }] };
    expect(completeAnalyticsSeries(data, 'month').map(point => point.bucket)).toEqual(['2024-01-01T00:00:00Z', '2024-02-01T00:00:00.000Z', '2024-03-01T00:00:00Z']);
  });
});
