using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Design;

namespace BNPB.Notification.Server.Adapters.Outbound.Persistence;

// Exposes a provider's context factory as the base type the store depends on.
public sealed class NotificationDbContextFactory<TContext> : IDbContextFactory<NotificationDbContext>
  where TContext : NotificationDbContext
{
  private readonly IDbContextFactory<TContext> _inner;

  public NotificationDbContextFactory(IDbContextFactory<TContext> inner)
  {
    _inner = inner;
  }

  public NotificationDbContext CreateDbContext() => _inner.CreateDbContext();

  public async Task<NotificationDbContext> CreateDbContextAsync(CancellationToken cancellationToken = default)
    => await _inner.CreateDbContextAsync(cancellationToken);
}

// Used by `dotnet ef` only, so migrations don't boot the whole host. Pick one with --context.
public class PostgresDesignTimeFactory : IDesignTimeDbContextFactory<PostgresNotificationDbContext>
{
  public PostgresNotificationDbContext CreateDbContext(string[] args)
    => new(new DbContextOptionsBuilder<PostgresNotificationDbContext>()
      .UseNpgsql("Host=localhost;Database=notifications")
      .Options);
}

public class SqliteDesignTimeFactory : IDesignTimeDbContextFactory<SqliteNotificationDbContext>
{
  public SqliteNotificationDbContext CreateDbContext(string[] args)
    => new(new DbContextOptionsBuilder<SqliteNotificationDbContext>()
      .UseSqlite("Data Source=notifications.db")
      .Options);
}
