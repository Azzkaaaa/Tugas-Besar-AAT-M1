using BNPB.Notification.Server.Core.Models;

namespace BNPB.Notification.Server.Core.Ports.Outbound;

public interface IPushProvider
{
  public Task SubscribeAsync(string token)
    => SubscribeAsync(token, CancellationToken.None);

  public Task SubscribeAsync(
    string token,
    CancellationToken cancellationToken);

  public Task UnsubscribeAsync(string token)
    => UnsubscribeAsync(token, CancellationToken.None);

  public Task UnsubscribeAsync(
    string token,
    CancellationToken cancellationToken);

  public Task BroadcastAsync(HazardEvent message)
    => BroadcastAsync(message, CancellationToken.None);

  public Task BroadcastAsync(
    HazardEvent message,
    CancellationToken cancellationToken);
}
