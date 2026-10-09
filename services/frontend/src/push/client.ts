import { initializeApp, type FirebaseApp } from 'firebase/app';
import { deleteToken, getMessaging, getToken, isSupported, onMessage, type Messaging } from 'firebase/messaging';
import { firebaseConfig, firebaseVapidKey, isPushConfigured } from '../config';
import type { GatewayClient } from '../types';
import { pushOpenMessage } from './swAuth';

export type PushState = 'unavailable' | 'default' | 'denied' | 'enabled' | 'disabled';

/** Must match the file emitted by vite.sw.config.ts. */
export const pushWorkerUrl = '/firebase-messaging-sw.js';
// A dedicated scope keeps the worker from controlling SPA pages; push delivery ignores scope.
const pushWorkerScope = '/firebase-cloud-messaging-push-scope';
const preferenceKey = 'bnpb.push.enabled';

let app: FirebaseApp | undefined;
let supported: Promise<boolean> | undefined;

function readPreference(): boolean {
  try { return localStorage.getItem(preferenceKey) === 'true'; } catch { return false; }
}

function writePreference(enabled: boolean) {
  try {
    if (enabled) localStorage.setItem(preferenceKey, 'true');
    else localStorage.removeItem(preferenceKey);
  } catch { /* Preference is a convenience; push still works for this page view. */ }
}

/** Resolves false outside a secure context, without Push API support, or without Firebase config. */
export function isPushAvailable(): Promise<boolean> {
  if (!isPushConfigured || typeof window === 'undefined' || !window.isSecureContext || !('Notification' in window) || !('serviceWorker' in navigator)) {
    return Promise.resolve(false);
  }
  supported ??= isSupported().catch(() => false);
  return supported;
}

function messaging(): Messaging {
  app ??= initializeApp(firebaseConfig);
  return getMessaging(app);
}

async function token(): Promise<string> {
  const registration = await navigator.serviceWorker.register(pushWorkerUrl, { scope: pushWorkerScope });
  // getToken is deprecated in favour of FID register(), but the danger-notification service
  // subscribes FCM registration tokens to its topic, so a token is what the Gateway accepts.
  return getToken(messaging(), { vapidKey: firebaseVapidKey, serviceWorkerRegistration: registration });
}

export async function getPushState(): Promise<PushState> {
  if (!await isPushAvailable()) return 'unavailable';
  if (Notification.permission === 'denied') return 'denied';
  if (Notification.permission === 'default') return 'default';
  return readPreference() ? 'enabled' : 'disabled';
}

/** Call from a user gesture only: browsers ignore or penalise unprompted permission requests. */
export async function enablePush(client: GatewayClient): Promise<PushState> {
  if (!await isPushAvailable()) return 'unavailable';
  const permission = await Notification.requestPermission();
  if (permission !== 'granted') return permission === 'denied' ? 'denied' : 'default';
  await client.registerPushToken(await token());
  writePreference(true);
  return 'enabled';
}

/**
 * Re-register after login. Logout unregisters the token, the service worker drops the
 * subscription once the session is gone, and FCM rotates tokens on its own schedule.
 */
export async function resumePush(client: GatewayClient): Promise<void> {
  if (!readPreference() || !await isPushAvailable() || Notification.permission !== 'granted') return;
  await client.registerPushToken(await token());
}

/** keepPreference: logout should stop delivery but re-enable push on the next login. */
export async function disablePush(client: GatewayClient, { keepPreference = false } = {}): Promise<void> {
  const wasEnabled = readPreference();
  if (!keepPreference) writePreference(false);
  // Never mint a token just to unregister it.
  if (!wasEnabled || !await isPushAvailable() || Notification.permission !== 'granted') return;
  const current = await token();
  try { await client.unregisterPushToken(current); }
  finally { await deleteToken(messaging()).catch(() => false); }
}

/** Foreground messages carry no display content; the page only uses them as a refresh hint. */
export function onForegroundPush(listener: () => void): () => void {
  let stop: (() => void) | undefined;
  let stopped = false;
  void isPushAvailable().then(available => {
    if (available && !stopped && Notification.permission === 'granted') stop = onMessage(messaging(), () => listener());
  });
  return () => { stopped = true; stop?.(); };
}

/** Follow notification clicks the worker forwards to an already open window. */
export function onPushOpen(listener: (route: string) => void): () => void {
  if (typeof navigator === 'undefined' || !('serviceWorker' in navigator)) return () => undefined;
  const handle = (event: MessageEvent) => {
    const data: unknown = event.data;
    if (!data || typeof data !== 'object' || (data as { type?: unknown }).type !== pushOpenMessage) return;
    const route = (data as { route?: unknown }).route;
    if (typeof route === 'string' && route.startsWith('/#/')) listener(route.slice(1));
  };
  navigator.serviceWorker.addEventListener('message', handle);
  return () => navigator.serviceWorker.removeEventListener('message', handle);
}
