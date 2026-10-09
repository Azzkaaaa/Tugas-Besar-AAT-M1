const environment = import.meta.env;

/** A production build is closed by default. Demo must be opted into explicitly. */
export const isDemoMode =
  (environment.VITE_APP_MODE ?? (environment.DEV ? 'demo' : 'oidc')) === 'demo' &&
  (environment.DEV || environment.VITE_ENABLE_DEMO === 'true');

/** Real login can be exercised while the business Gateway is still unfinished. */
export const isMockDataMode = (environment.VITE_DATA_MODE ?? (isDemoMode ? 'mock' : 'gateway')) === 'mock';

/** Cookie authentication requires an API route on the same origin as the SPA. */
export const apiBaseUrl = (environment.VITE_API_BASE_URL || '/api/v1').replace(/\/+$/, '');

/** Firebase web config values are public identifiers; the VAPID key is the public half. */
export const firebaseConfig = {
  apiKey: environment.VITE_FIREBASE_API_KEY ?? '',
  projectId: environment.VITE_FIREBASE_PROJECT_ID ?? '',
  messagingSenderId: environment.VITE_FIREBASE_MESSAGING_SENDER_ID ?? '',
  appId: environment.VITE_FIREBASE_APP_ID ?? '',
};
export const firebaseVapidKey = environment.VITE_FIREBASE_VAPID_KEY ?? '';

/** Push needs the real Gateway: simulated data has no danger-notification service behind it. */
export const isPushConfigured = !isMockDataMode && Object.values(firebaseConfig).every(Boolean) && Boolean(firebaseVapidKey);
