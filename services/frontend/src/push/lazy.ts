import { isPushConfigured } from '../config';

/** Firebase is only downloaded when this build can actually use push. */
export const loadPush = isPushConfigured ? () => import('./client') : undefined;
