import { NodeSDK } from '@opentelemetry/sdk-node';
import { UndiciInstrumentation } from '@opentelemetry/instrumentation-undici';

// OTLP export only when OTEL_EXPORTER_OTLP_ENDPOINT is set; exporters follow the standard OTEL_* variables.
// Incoming requests are traced by hand in server.mjs: HTTP auto-instrumentation records the
// query string, and /auth/callback carries the single-use authorization code there.
export function startTelemetry() {
  if (!process.env.OTEL_EXPORTER_OTLP_ENDPOINT?.trim()) return null;
  // Undici covers the fetch calls openid-client makes to Zitadel, carrying traceparent upstream.
  const sdk = new NodeSDK({ serviceName: process.env.OTEL_SERVICE_NAME || 'auth-bridge',
    instrumentations: [new UndiciInstrumentation()] });
  sdk.start();
  return sdk;
}
