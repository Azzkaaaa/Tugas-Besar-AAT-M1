using BNPB.Notification.Server.Core.Models;
using BNPB.Notification.Server.Core.Ports.Inbound;
using BNPB.Notification.Server.Core.Ports.Outbound;

namespace BNPB.Notification.Server.Core.Services;

public class BroadcastService : IBroadcastProvider
{
  private readonly ILogger<BroadcastService> _logger;
  private readonly IPushProvider _pushProvider;

  public BroadcastService(ILogger<BroadcastService> logger, IPushProvider pushProvider)
  {
    _logger = logger;
    _pushProvider = pushProvider;
  }

  public async Task FilterAndBroadcastAsync(
    HazardEvent message,
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

    await _pushProvider.BroadcastAsync(message, cancellationToken);
  }
}
