using System.Buffers;
using BNPB.Notification.Server.Adapters.Models;
using BNPB.Notification.Server.Adapters.Outbound.Persistence;
using Microsoft.AspNetCore.Connections;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace BNPB.Notification.Server.Adapters.Inbound;

public static class NotificationReadHosting
{
  public static void AddNotificationReadBounds(this WebApplicationBuilder builder)
  {
    builder.Services.TryAddSingleton<NotificationReadBudget>();
    builder.Services.Decorate<IMemoryPoolFactory<byte>, NotificationMemoryPoolFactory>();
    builder.Services.AddGrpc(options => options.MaxSendMessageSize = NotificationProjection.MaxResponseBytes);
    // Bound the small per-connection/stream buffers not rented in a FULL scope.
    builder.WebHost.ConfigureKestrel(options => {
      options.Limits.MaxConcurrentConnections = 32;
      options.Limits.Http2.MaxStreamsPerConnection = 16;
      options.Limits.MaxResponseBufferSize = 64 * 1024;
    });
  }

  public static void UseNotificationReadBounds(this IApplicationBuilder app) => app.UseMiddleware<NotificationReadLifetime>();
}

// Keep the framework's evicting pool for ordinary blocks. Ownership references
// wrap actual Kestrel pool rentals, so a queued HTTP/2 response keeps admission
// after the application has returned. Oversized rentals use an exact-size array.
internal sealed class NotificationMemoryPoolFactory(IMemoryPoolFactory<byte> inner) : IMemoryPoolFactory<byte>
{
  public MemoryPool<byte> Create(MemoryPoolOptions? options = null) => new Pool(inner.Create(options));
  private sealed class Pool(MemoryPool<byte> innerPool) : MemoryPool<byte>
  {
    // Ensure Pipe doesn't bypass this pool for a large size hint.
    public override int MaxBufferSize => int.MaxValue;
    public override IMemoryOwner<byte> Rent(int minBufferSize = -1)
    {
      // Large protobuf size hints must not fill reusable pool buckets with a
      // separate multi-megabyte block for each completed HTTP/2 stream.
      var owner = minBufferSize <= Math.Min(innerPool.MaxBufferSize, 64 * 1024)
        ? innerPool.Rent(minBufferSize) : new ArrayOwner(minBufferSize);
      var credit = NotificationReadScope.Current?.Credit;
      return credit != null && credit.Retain() ? new HeldOwner(owner, credit) : owner;
    }
    protected override void Dispose(bool disposing) { if (disposing) innerPool.Dispose(); }
  }
  private sealed class ArrayOwner(int length) : IMemoryOwner<byte>
  {
    private byte[]? _buffer = new byte[length];
    public Memory<byte> Memory => _buffer ?? throw new ObjectDisposedException(nameof(ArrayOwner));
    public void Dispose() => _buffer = null;
  }
  private sealed class HeldOwner(IMemoryOwner<byte> owner, ReadCredit credit) : IMemoryOwner<byte>
  {
    private IMemoryOwner<byte>? _owner = owner;
    public Memory<byte> Memory => _owner?.Memory ?? throw new ObjectDisposedException(nameof(HeldOwner));
    public void Dispose()
    {
      var item = Interlocked.Exchange(ref _owner, null);
      if (item == null) return;
      try { item.Dispose(); } finally { credit.Release(); }
    }
  }
}
