namespace BNPB.Notification.Server.Adapters.Outbound.Persistence;

// Wakes the outbox worker as soon as a push becomes pending, instead of at the next poll.
public sealed class PushOutboxSignal
{
  private readonly SemaphoreSlim _semaphore = new(0, 1);

  public void Notify()
  {
    try
    {
      _semaphore.Release();
    }
    catch (SemaphoreFullException)
    {
      // Already signalled.
    }
  }

  public Task<bool> WaitAsync(TimeSpan timeout, CancellationToken cancellationToken)
    => _semaphore.WaitAsync(timeout, cancellationToken);
}
