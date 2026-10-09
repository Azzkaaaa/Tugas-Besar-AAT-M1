using Microsoft.EntityFrameworkCore;

namespace BNPB.Notification.Server.Adapters.Outbound.Persistence;

// Registered before the consumer and outbox worker, so the schema exists before either starts.
public class NotificationStoreMigrator : IHostedService
{
  private readonly ILogger<NotificationStoreMigrator> _logger;
  private readonly IDbContextFactory<NotificationDbContext> _contextFactory;

  public NotificationStoreMigrator(
    ILogger<NotificationStoreMigrator> logger,
    IDbContextFactory<NotificationDbContext> contextFactory)
  {
    _logger = logger;
    _contextFactory = contextFactory;
  }

  public async Task StartAsync(CancellationToken cancellationToken)
  {
    await using var db = await _contextFactory.CreateDbContextAsync(cancellationToken);

    await db.Database.MigrateAsync(cancellationToken);

    // WAL lets list/get read while the consumer or outbox writes. It persists in the file.
    if (db.Database.IsSqlite())
      await db.Database.ExecuteSqlRawAsync("PRAGMA journal_mode=WAL;", cancellationToken);

    _logger.LogInformation("Notification store is up to date");
  }

  public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
