using BNPB.Notification.Server.Core.Models;

namespace BNPB.Notification.Server.Core.Ports.Inbound;

public interface ISubscriptionProvider
{
  public Task SubscribeAsync(
    string token,
    IEnumerable<UserPermission> permissions)
    => SubscribeAsync(token, permissions, CancellationToken.None);

  public Task SubscribeAsync(
    string token,
    IEnumerable<UserPermission> permissions,
    CancellationToken cancellationToken);

  public Task UnsubscribeAsync(string token)
    => UnsubscribeAsync(token, CancellationToken.None);

  public Task UnsubscribeAsync(
    string token,
    CancellationToken cancellationToken);
}
