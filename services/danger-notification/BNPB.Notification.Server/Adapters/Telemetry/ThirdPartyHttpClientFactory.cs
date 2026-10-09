using System.Diagnostics;
using System.Net;
using Google.Apis.Http;

namespace BNPB.Notification.Server.Adapters.Telemetry;

// HttpClients for Firebase and Google OAuth: third parties must not receive trace context.
// The HttpClient instrumentation injects traceparent/baggage itself, so the runtime's
// propagation is switched off (no runtime activity either) and ThirdPartyTracingHandler
// records the client span instead, without touching the request headers.
public sealed class ThirdPartyHttpClientFactory : HttpClientFactory
{
  protected override HttpMessageHandler CreateHandler(CreateHttpClientArgs args) =>
    new ThirdPartyTracingHandler(new SocketsHttpHandler
    {
      ActivityHeadersPropagator = null,
      AutomaticDecompression = args.GZipEnabled
        ? DecompressionMethods.GZip | DecompressionMethods.Deflate
        : DecompressionMethods.None,
    });
}

public sealed class ThirdPartyTracingHandler(HttpMessageHandler inner) : DelegatingHandler(inner)
{
  protected override async Task<HttpResponseMessage> SendAsync(
    HttpRequestMessage request, CancellationToken cancellationToken)
  {
    using var activity = NotificationTelemetry.Source.StartActivity(request.Method.Method, ActivityKind.Client);

    if (activity != null && request.RequestUri is { } uri)
    {
      activity.SetTag("http.request.method", request.Method.Method);
      activity.SetTag("server.address", uri.Host);
      activity.SetTag("server.port", uri.Port);
      // Without the query: Google OAuth and FCM URLs never need it in traces.
      activity.SetTag("url.full", uri.GetLeftPart(UriPartial.Path));
    }

    try
    {
      var response = await base.SendAsync(request, cancellationToken);

      activity?.SetTag("http.response.status_code", (int)response.StatusCode);
      if ((int)response.StatusCode >= 400)
      {
        activity?.SetTag("error.type", ((int)response.StatusCode).ToString());
        activity?.SetStatus(ActivityStatusCode.Error);
      }

      return response;
    }
    catch (Exception ex)
    {
      activity?.SetTag("error.type", ex.GetType().FullName);
      activity?.AddException(ex);
      activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
      throw;
    }
  }
}
