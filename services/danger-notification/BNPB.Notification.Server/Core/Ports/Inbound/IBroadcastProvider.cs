using BNPB.Notification.Server.Core.Models;

namespace BNPB.Notification.Server.Core.Ports.Inbound;

public interface IBroadcastProvider
{
  public Task FilterAndBroadcastAsync(HazardEvent message)
    => FilterAndBroadcastAsync(message, CancellationToken.None);

  public Task FilterAndBroadcastAsync(
    HazardEvent message,
    CancellationToken cancellationToken);
}
