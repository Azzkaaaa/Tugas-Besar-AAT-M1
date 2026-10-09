namespace BNPB.Notification.Server.Core.Ports.Inbound;

public interface IPushDispatchProvider
{
  public Task DispatchPendingAsync()
    => DispatchPendingAsync(CancellationToken.None);

  public Task DispatchPendingAsync(CancellationToken cancellationToken);
}
