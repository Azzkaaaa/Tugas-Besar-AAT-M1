using BNPB.Notification.Server.Core.Models;

namespace BNPB.Notification.Server.Core.Ports.Outbound;

public interface IMessageProvider
{
  public Task<HazardNotification?> GetMessageAsync(string messageId)
    => GetMessageAsync(messageId, CancellationToken.None);

  public Task<HazardNotification?> GetMessageAsync(
    string messageId,
    CancellationToken cancellationToken);
}
