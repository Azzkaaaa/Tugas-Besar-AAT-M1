using System.Diagnostics;
using BNPB.Notification.Server.Adapters.Telemetry;
using BNPB.Notification.Server.Core.Ports.Outbound;
using BNPB.Notification.Server.Core.Models;
using FirebaseAdmin.Messaging;

namespace BNPB.Notification.Server.Adapters.Outbound;

public class FcmClient : IPushProvider
{
  private const string HazardType = "HAZARD";

  private readonly ILogger<FcmClient> _logger;
  private readonly FirebaseMessaging _messaging;
  private readonly string _topic;

  public FcmClient(
    ILogger<FcmClient> logger,
    FirebaseMessaging messaging,
    IConfiguration configuration)
  {
    _logger = logger;
    _messaging = messaging;
    _topic = configuration["Fcm:Topic"]!;
  }

  public async Task SubscribeAsync(
    string token,
    CancellationToken cancellationToken)
  {
    var response = await _messaging.SubscribeToTopicAsync(
      new[] { token },
      _topic,
      cancellationToken);

    ThrowIfTopicOperationFailed(response, "subscribe");

    _logger.LogInformation("Device subscribed to FCM topic {Topic}", _topic);
  }

  public async Task UnsubscribeAsync(
    string token,
    CancellationToken cancellationToken)
  {
    var response = await _messaging.UnsubscribeFromTopicAsync(
      new[] { token },
      _topic,
      cancellationToken);

    ThrowIfTopicOperationFailed(response, "unsubscribe");

    _logger.LogInformation("Device unsubscribed from FCM topic {Topic}", _topic);
  }

  public async Task BroadcastAsync(
    HazardNotification message,
    CancellationToken cancellationToken)
  {
    var messageId = message.Id;

    var fcmMessage = new Message
    {
      Topic = _topic,
      Data = new Dictionary<string, string>
      {
        ["messageId"] = messageId,
        ["type"] = "HAZARD",
        ["timestamp"] = DateTimeOffset.UtcNow.ToString("O")
      },
      Android = new AndroidConfig
      {
        Priority = Priority.High
      },
      Apns = new ApnsConfig
      {
        Headers = new Dictionary<string, string>
        {
          ["apns-push-type"] = "background",
          ["apns-priority"] = "5"
        },
        Aps = new Aps
        {
          ContentAvailable = true
        }
      }
    };

    // Sent from the outbox worker, so continue the consume span's trace explicitly.
    var parent = message.ExtraData.TryGetValue(typeof(ActivityContext), out var context)
      ? (ActivityContext)context
      : default;

    using var activity = NotificationTelemetry.Source.StartActivity("fcm.send", ActivityKind.Client, parent);
    activity?.SetTag("messaging.system", "fcm");
    activity?.SetTag("messaging.destination", _topic);
    activity?.SetTag("notification.id", messageId);
    activity?.SetTag("hazard.severity", message.Severity.ToString());

    var severity = new KeyValuePair<string, object?>("hazard.severity", message.Severity.ToString());
    var started = Stopwatch.GetTimestamp();

    try
    {
      var fcmMessageId = await _messaging.SendAsync(fcmMessage, cancellationToken);

      activity?.SetTag("messaging.message_id", fcmMessageId);
      NotificationTelemetry.Broadcasts.Add(1, new KeyValuePair<string, object?>("result", "success"), severity);

      _logger.LogInformation(
        "Broadcast hazard {MessageId} to topic {Topic} (FCM id: {FcmMessageId})",
        messageId, _topic, fcmMessageId);
    }
    catch (Exception ex)
    {
      activity?.AddException(ex);
      activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
      NotificationTelemetry.Broadcasts.Add(1, new KeyValuePair<string, object?>("result", "error"), severity);

      if (ex is FirebaseMessagingException fcmEx)
      {
        activity?.SetTag("fcm.error_code", fcmEx.MessagingErrorCode?.ToString());
        _logger.LogError(ex,
          "FCM broadcast failed for hazard {MessageId} on topic {Topic}: {ErrorCode}",
          messageId, _topic, fcmEx.MessagingErrorCode);
      }

      throw;
    }
    finally
    {
      NotificationTelemetry.BroadcastDuration.Record(Stopwatch.GetElapsedTime(started).TotalSeconds, severity);
    }
  }

  private void ThrowIfTopicOperationFailed(TopicManagementResponse response, string operation)
  {
    if (response.FailureCount == 0)
    {
      return;
    }

    var reason = string.Join("; ", response.Errors.Select(e => e.Reason));

    _logger.LogError(
      "Failed to {Operation} topic {Topic}: {Reason}", operation, _topic, reason);

    throw new InvalidOperationException(
      $"FCM failed to {operation} device on topic '{_topic}': {reason}");
  }
}
