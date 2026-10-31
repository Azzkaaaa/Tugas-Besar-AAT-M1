using Microsoft.EntityFrameworkCore;

namespace BNPB.Notification.Server.Adapters.Outbound.Persistence;

// The shared model. EF migrations are provider-specific, so each provider has its own
// subclass (and migration set) below; the store only sees this type.
public abstract class NotificationDbContext : DbContext
{
  protected NotificationDbContext(DbContextOptions options)
    : base(options)
  {
  }

  public DbSet<NotificationRecord> Notifications => Set<NotificationRecord>();

  protected override void OnModelCreating(ModelBuilder modelBuilder)
  {
    var notification = modelBuilder.Entity<NotificationRecord>();

    notification.ToTable("notifications");
    notification.HasKey(r => r.Id);
    notification.Property(r => r.Hazard).IsRequired();

    // List: current rows, newest first.
    notification
      .HasIndex(r => new { r.IsCurrent, r.CreatedAt, r.Id })
      .IsDescending(false, true, false);

    // Superseding older revisions.
    notification.HasIndex(r => new { r.HazardId, r.IsCurrent });

    // At most one current revision per hazard, whichever replica stored it.
    notification
      .HasIndex(r => r.HazardId)
      .IsUnique()
      .HasFilter(CurrentFilter);

    // Outbox claims.
    notification.HasIndex(r => new { r.PushStatus, r.NextPushAttemptAt });
  }

  // Partial index predicate selecting current rows, in the provider's SQL.
  protected abstract string CurrentFilter { get; }
}

public class PostgresNotificationDbContext : NotificationDbContext
{
  public PostgresNotificationDbContext(DbContextOptions<PostgresNotificationDbContext> options)
    : base(options)
  {
  }

  protected override string CurrentFilter => "\"IsCurrent\"";

  protected override void OnModelCreating(ModelBuilder modelBuilder)
  {
    base.OnModelCreating(modelBuilder);

    // Maps to the xmin system column: a write over a row another replica changed since it was
    // read fails with DbUpdateConcurrencyException instead of silently winning.
    modelBuilder.Entity<NotificationRecord>().Property<uint>("RowVersion").IsRowVersion();
  }
}

public class SqliteNotificationDbContext : NotificationDbContext
{
  public SqliteNotificationDbContext(DbContextOptions<SqliteNotificationDbContext> options)
    : base(options)
  {
  }

  protected override string CurrentFilter => "\"IsCurrent\" = 1";
}
