using BNPB.Notification.Server.Core.Models;
using BNPB.Notification.Server.Core.Ports.Inbound;
using BNPB.Notification.Server.Core.Ports.Outbound;

namespace BNPB.Notification.Server.Core.Services;

public class PushDispatchService : IPushDispatchProvider
{
  public const int DefaultBatchSize = 10;

  private readonly ILogger<PushDispatchService> _logger;
  private readonly IPushOutbox _outbox;
  private readonly IMessageStore _messageStore;
  private readonly IPushProvider _pushProvider;
  private readonly int _batchSize;

  public PushDispatchService(
    ILogger<PushDispatchService> logger,
    IPushOutbox outbox,
    IMessageStore messageStore,
    IPushProvider pushProvider,
    int batchSize = DefaultBatchSize)
  {
    _logger = logger;
    _outbox = outbox;
    _messageStore = messageStore;
    _pushProvider = pushProvider;
    _batchSize = batchSize;
  }

  public async Task DispatchPendingAsync(CancellationToken cancellationToken)
  {
    var batch = await _outbox.ClaimDuePushesAsync(_batchSize, cancellationToken);

    for (int i = 0; i < batch.Count; i++)
    {
      if (cancellationToken.IsCancellationRequested)
      {
        // The token is already cancelled; releasing must still go through.
        await _outbox.ReleasePushesAsync(batch.Skip(i), CancellationToken.None);
        return;
      }

      var message = batch[i];
      PushStatus status;

      try
      {
        await _pushProvider.BroadcastAsync(message, cancellationToken);
        status = PushStatus.Delivered;
      }
      catch (Exception ex)
      {
        _logger.LogError(ex, "Push failed for notification {NotificationId}", message.Id);
        status = PushStatus.Failed;
      }

      // Record the outcome even if shutdown started mid-send.
      await _messageStore.UpsertMessageAsync(
        message with { PushStatus = status },
        CancellationToken.None);
    }
  }
}
