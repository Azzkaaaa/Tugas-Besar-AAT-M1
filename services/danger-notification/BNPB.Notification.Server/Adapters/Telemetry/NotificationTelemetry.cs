using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Text;
using OpenTelemetry.Context.Propagation;

namespace BNPB.Notification.Server.Adapters.Telemetry;

// Spans and instruments live here so the core stays free of telemetry code.
// Program.cs registers Source and Meter with the OpenTelemetry SDK; when no
// exporter is configured they record nothing.
public static class NotificationTelemetry
{
  public const string Name = "BNPB.Notification.Server";

  public static readonly ActivitySource Source = new(Name);
  public static readonly Meter Meter = new(Name);

  public static readonly Counter<long> EventsConsumed = Meter.CreateCounter<long>(
    "bnpb_notification_events_consumed_total",
    unit: "{event}",
    description: "Hazard events consumed from RabbitMQ, by outcome");

  public static readonly Counter<long> Broadcasts = Meter.CreateCounter<long>(
    "bnpb_notification_broadcasts_total",
    unit: "{broadcast}",
    description: "FCM topic broadcasts sent, by result");

  public static readonly Histogram<double> BroadcastDuration = Meter.CreateHistogram<double>(
    "bnpb_notification_broadcast_duration_seconds",
    unit: "s",
    description: "Duration of FCM topic broadcasts in seconds");

  // The aggregator injects W3C trace context into the AMQP headers. RabbitMQ.Client
  // delivers string header values as byte[].
  public static ActivityContext ExtractAmqpParent(IDictionary<string, object?>? headers)
  {
    if (headers == null)
      return default;

    var context = Propagators.DefaultTextMapPropagator.Extract(default, headers, ReadHeader);

    return context.ActivityContext;
  }

  private static IEnumerable<string> ReadHeader(IDictionary<string, object?> headers, string key)
  {
    if (!headers.TryGetValue(key, out var value))
      return [];

    return value switch
    {
      byte[] bytes => [Encoding.UTF8.GetString(bytes)],
      string text => [text],
      _ => [],
    };
  }
}
