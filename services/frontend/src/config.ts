const environment = import.meta.env;

/** A production build is closed by default. Demo must be opted into explicitly. */
export const isDemoMode =
  (environment.VITE_APP_MODE ?? (environment.DEV ? 'demo' : 'oidc')) === 'demo' &&
  (environment.DEV || environment.VITE_ENABLE_DEMO === 'true');

/** Real login can be exercised while the business Gateway is still unfinished. */
export const isMockDataMode = (environment.VITE_DATA_MODE ?? (isDemoMode ? 'mock' : 'gateway')) === 'mock';

/** Cookie authentication requires an API route on the same origin as the SPA. */
export const apiBaseUrl = (environment.VITE_API_BASE_URL || '/api/v1').replace(/\/+$/, '');
