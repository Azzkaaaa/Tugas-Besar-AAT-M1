import { isDemoMode, isMockDataMode } from '../config';
import { createMockGatewayClient } from '../mocks/gateway';
import type { AuthSession, GatewayClient } from '../types';
import { ApiError } from './errors';
import { createHttpGatewayClient } from './http';

export { ApiError } from './errors';
export { createHttpGatewayClient } from './http';

/** A demo identity is never forwarded as authentication to a real endpoint. */
export function createGatewayClient(session: AuthSession): GatewayClient {
  if (session.mode === 'demo' && (!isDemoMode || !isMockDataMode)) throw new ApiError(401, 'Identitas pratinjau hanya boleh mengakses data simulasi.');
  return isMockDataMode ? createMockGatewayClient(session) : createHttpGatewayClient();
}
