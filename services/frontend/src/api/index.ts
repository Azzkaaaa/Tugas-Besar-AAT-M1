import { isDemoMode } from '../config';
import { createMockGatewayClient } from '../mocks/gateway';
import type { AuthSession, GatewayClient } from '../types';
import { ApiError } from './errors';

export { ApiError } from './errors';
export { createHttpGatewayClient } from './http';

/** A demo identity is never forwarded as authentication to a real endpoint. */
export function createGatewayClient(session: AuthSession): GatewayClient {
  if (!isDemoMode || session.mode !== 'demo') throw new ApiError(401, 'Login organisasi belum tersedia. Konfigurasi Auth diperlukan.');
  return createMockGatewayClient(session);
}
