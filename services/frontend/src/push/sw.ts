/// <reference lib="webworker" />
// Built separately by vite.sw.config.ts into a classic (IIFE) worker at /firebase-messaging-sw.js.
import { initializeApp } from 'firebase/app';
import { getMessaging, onBackgroundMessage } from 'firebase/messaging/sw';
import { apiBaseUrl, firebaseConfig } from '../config';
import { describe, lookupNotification, notificationRoute, pushOpenMessage } from './swAuth';

declare const self: ServiceWorkerGlobalScope;

// Take over from an older worker build immediately; it controls no pages, so nothing breaks.
self.addEventListener('install', () => { void self.skipWaiting(); });

const messaging = getMessaging(initializeApp(firebaseConfig));

// FCM messages are data-only ({ messageId, type, timestamp }); the content comes from the Gateway.
// The SDK keeps the push event alive until this handler's promise settles.
onBackgroundMessage(messaging, async payload => {
  const id = payload.data?.messageId;
  const lookup = id ? await lookupNotification(apiBaseUrl, id) : { kind: 'failed' as const };
  if (lookup.kind === 'signed-out') {
    // Topic membership outlives the session; stop delivery to a browser nobody is signed in on.
    // The page subscribes again on the next login (push/client.ts resumePush).
    await self.registration.pushManager.getSubscription().then(subscription => subscription?.unsubscribe()).catch(() => false);
  }
  const { title, body } = describe(lookup);
  // Browsers require a visible notification for every push, even when the lookup fails.
  await self.registration.showNotification(title, {
    body,
    tag: id ?? 'bnpb-hazard',
    data: { route: notificationRoute(lookup.kind === 'found' ? id : undefined) },
  });
});

self.addEventListener('notificationclick', event => {
  event.notification.close();
  const route = typeof event.notification.data?.route === 'string' ? event.notification.data.route : '/#/notifications';
  const target = new URL(route, self.location.origin).href;
  event.waitUntil((async () => {
    // includeUncontrolled: this worker's scope deliberately excludes the SPA pages.
    const windows = await self.clients.matchAll({ type: 'window', includeUncontrolled: true });
    const existing = windows.find(client => new URL(client.url).origin === self.location.origin);
    if (existing) {
      // Client.navigate() only works on controlled clients; the page applies the route itself.
      existing.postMessage({ type: pushOpenMessage, route });
      await existing.focus();
      return;
    }
    await self.clients.openWindow(target);
  })());
});
