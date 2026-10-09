using BNPB.Notification.Server.Core.Models;
using BNPB.Notification.Server.Core.Ports.Inbound;
using BNPB.Notification.Server.Core.Ports.Outbound;

namespace BNPB.Notification.Server.Core.Services;

public class BroadcastService : IBroadcastProvider
{
  private readonly ILogger<BroadcastService> _logger;
  private readonly IMessageStore _messageStore;

  public BroadcastService(ILogger<BroadcastService> logger, IMessageStore messageStore)
  {
    _logger = logger;
    _messageStore = messageStore;
  }

  // Stores the notification as pending; PushDispatchService sends it.
  public async Task FilterAndBroadcastAsync(
    HazardNotification message,
    CancellationToken cancellationToken)
  {
    switch (message.Severity)
    {
      case HazardSeverity.Awas:
      case HazardSeverity.Siaga:
        break;
      default:
        return;
    }

    await _messageStore.UpsertMessageAsync(
      message with { PushStatus = PushStatus.Pending },
      cancellationToken);
  }
}
