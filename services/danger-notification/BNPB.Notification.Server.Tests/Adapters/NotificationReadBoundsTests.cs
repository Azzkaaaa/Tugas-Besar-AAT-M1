using System.Buffers;
using BNPB.Notification.Server.Adapters.Inbound;
using BNPB.Notification.Server.Adapters.Outbound.Persistence;
using BNPB.Notification.Server.Adapters.Exceptions;
using Microsoft.AspNetCore.Connections;

namespace BNPB.Notification.Server.Tests.Adapters;

public class NotificationReadBoundsTests
{
  [Fact]
  public void QueuedPoolOwnersKeepAdmissionAfterRequestScopeEnds()
  {
    var budget = new NotificationReadBudget();
    using var pool = new NotificationMemoryPoolFactory(new TinyFactory()).Create();
    using var scope = new NotificationReadScope();
    NotificationReadScope.Admit(budget);
    var small = pool.Rent(32);
    var large = pool.Rent(128 * 1024);
    Assert.Equal(128 * 1024, large.Memory.Length);
    large.Memory.Span.Fill(42);
    scope.Dispose();
    Assert.Throws<NotificationCapacityException>(() => budget.AcquireFull());
    small.Dispose();
    Assert.Throws<NotificationCapacityException>(() => budget.AcquireFull());
    Assert.Equal(42, large.Memory.Span[100]);
    large.Dispose();
    large.Dispose();
    using var recovered = budget.AcquireFull();
  }

  private sealed class TinyFactory : IMemoryPoolFactory<byte>
  {
    public MemoryPool<byte> Create(MemoryPoolOptions? options = null) => new TinyPool();
  }
  private sealed class TinyPool : MemoryPool<byte>
  {
    public override int MaxBufferSize => 64;
    public override IMemoryOwner<byte> Rent(int minBufferSize = -1)
    {
      if (minBufferSize > 64) throw new InvalidOperationException("Large rental bypassed ownership wrapper");
      return MemoryPool<byte>.Shared.Rent(minBufferSize);
    }
    protected override void Dispose(bool disposing) { }
  }
}
