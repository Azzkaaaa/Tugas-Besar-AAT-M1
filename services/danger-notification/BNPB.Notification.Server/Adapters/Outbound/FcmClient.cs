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
    HazardEvent message,
    CancellationToken cancellationToken)
  {
    var messageId = message.Id;

    var fcmMessage = new Message
    {
      Topic = _topic,
      Data = new Dictionary<string, string>
      {
        ["messageId"] = messageId.ToString()!,
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

    try
    {
      var fcmMessageId = await _messaging.SendAsync(fcmMessage, cancellationToken);

      _logger.LogInformation(
        "Broadcast hazard {MessageId} to topic {Topic} (FCM id: {FcmMessageId})",
        messageId, _topic, fcmMessageId);
    }
    catch (FirebaseMessagingException ex)
    {
      _logger.LogError(ex,
        "FCM broadcast failed for hazard {MessageId} on topic {Topic}: {ErrorCode}",
        messageId, _topic, ex.MessagingErrorCode);
      throw;
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
