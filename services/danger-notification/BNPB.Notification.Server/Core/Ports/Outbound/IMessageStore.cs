using BNPB.Notification.Server.Core.Models;

namespace BNPB.Notification.Server.Core.Ports.Outbound;

public interface IMessageStore
{
  public Task StoreMessageAsync(HazardEvent message)
    => StoreMessageAsync(message, CancellationToken.None);

  public Task StoreMessageAsync(
    HazardEvent message,
    CancellationToken cancellationToken);
}
