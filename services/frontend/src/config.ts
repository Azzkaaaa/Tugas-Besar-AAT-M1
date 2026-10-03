const environment = import.meta.env;

/** A production build is closed by default. Demo must be opted into explicitly. */
export const isDemoMode =
  (environment.VITE_APP_MODE ?? (environment.DEV ? 'demo' : 'gateway')) === 'demo' &&
  (environment.DEV || environment.VITE_ENABLE_DEMO === 'true');

/** Include the API prefix; for example http://api.localhost:8080/api/v1. */
export const apiBaseUrl = (environment.VITE_API_BASE_URL || '/api/v1').replace(/\/+$/, '');
