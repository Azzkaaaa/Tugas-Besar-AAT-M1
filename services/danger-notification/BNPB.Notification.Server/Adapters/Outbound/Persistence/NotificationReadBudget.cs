namespace BNPB.Notification.Server.Adapters.Outbound.Persistence;

// Shared by the FULL and SUMMARY store adapters in this process. A legacy
// summary repair owns this slot through query cancellation/disposal, parsing,
// allowlist copying and cache persistence. Waiting callers retain headers only.
public sealed class NotificationReadBudget
{
  internal SemaphoreSlim LegacySummary { get; } = new(1, 1);
  private ReadCredit? _activeCredit;
  internal ReadCredit? ActiveCredit => Volatile.Read(ref _activeCredit);

  internal ReadCredit AcquireFull()
  {
    if (!LegacySummary.Wait(0)) throw new BNPB.Notification.Server.Adapters.Exceptions.NotificationCapacityException(
      "Full notification capacity is busy; retry later");
    var credit = new ReadCredit(this);
    Volatile.Write(ref _activeCredit, credit);
    return credit;
  }
  internal void Return(ReadCredit credit)
  {
    Interlocked.CompareExchange(ref _activeCredit, null, credit);
    LegacySummary.Release();
  }

  internal async ValueTask<IDisposable> EnterStoreAsync(CancellationToken cancellationToken)
  {
    if (BNPB.Notification.Server.Adapters.Inbound.NotificationReadScope.Current?.Credit?.Budget == this)
      return Empty.Instance;
    await LegacySummary.WaitAsync(cancellationToken);
    return new ReleaseStore(LegacySummary);
  }

  private sealed class ReleaseStore(SemaphoreSlim slot) : IDisposable
  {
    public void Dispose() => slot.Release();
  }
  private sealed class Empty : IDisposable
  {
    public static readonly Empty Instance = new();
    public void Dispose() { }
  }
}

internal sealed class ReadCredit(NotificationReadBudget budget) : IDisposable
{
  private int _references = 1;
  private int _rootDisposed;
  public NotificationReadBudget Budget { get; } = budget;
  internal int References => Volatile.Read(ref _references);
  internal bool RootDisposed => Volatile.Read(ref _rootDisposed) != 0;
  public bool Retain()
  {
    int count;
    do { count = Volatile.Read(ref _references); if (count == 0) return false; }
    while (Interlocked.CompareExchange(ref _references, count + 1, count) != count);
    return true;
  }
  public void Release()
  {
    if (Interlocked.Decrement(ref _references) == 0) Budget.Return(this);
  }
  public void Dispose()
  {
    if (Interlocked.Exchange(ref _rootDisposed, 1) == 0) Release();
  }
}
