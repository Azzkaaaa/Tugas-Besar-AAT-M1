import type { Problem } from '../types';

export class ApiError extends Error {
  readonly status: number;
  readonly problem: Problem;

  constructor(status: number, detail: string, problem?: Partial<Problem>) {
    super(detail);
    this.name = 'ApiError';
    this.status = status;
    this.problem = {
      type: 'about:blank',
      title: status === 503 ? 'Layanan belum tersedia' : 'Permintaan tidak dapat diproses',
      status,
      detail,
      instance: '/api/v1/hazards',
      correlation_id: 'frontend',
      ...problem,
    };
  }
}
