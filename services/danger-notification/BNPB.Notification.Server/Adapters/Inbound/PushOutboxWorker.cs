using BNPB.Notification.Server.Core.Ports.Inbound;
using BNPB.Notification.Server.Adapters.Outbound.Persistence;

namespace BNPB.Notification.Server.Adapters.Inbound;

// Drives the push outbox: dispatches whenever a push becomes pending, and at
// least every PollInterval for retries and lapsed claims.
public class PushOutboxWorker : BackgroundService
{
  private readonly ILogger<PushOutboxWorker> _logger;
  private readonly IPushDispatchProvider _dispatchProvider;
  private readonly PushOutboxSignal _signal;
  private readonly TimeSpan _pollInterval;

  public PushOutboxWorker(
    ILogger<PushOutboxWorker> logger,
    IPushDispatchProvider dispatchProvider,
    PushOutboxSignal signal,
    OutboxOptions options)
  {
    _logger = logger;
    _dispatchProvider = dispatchProvider;
    _signal = signal;
    _pollInterval = options.PollInterval;
  }

  protected override async Task ExecuteAsync(CancellationToken stoppingToken)
  {
    while (!stoppingToken.IsCancellationRequested)
    {
      try
      {
        await _dispatchProvider.DispatchPendingAsync(stoppingToken);
        await _signal.WaitAsync(_pollInterval, stoppingToken);
      }
      catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
      {
        break;
      }
      catch (Exception ex)
      {
        _logger.LogError(ex, "Push outbox dispatch failed");
        await Task.Delay(_pollInterval, stoppingToken).ContinueWith(_ => { }, TaskScheduler.Default);
      }
    }
  }
}
