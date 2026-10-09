using Microsoft.EntityFrameworkCore;

namespace BNPB.Notification.Server.Adapters.Outbound.Persistence;

public class NotificationDbContext : DbContext
{
  public NotificationDbContext(DbContextOptions<NotificationDbContext> options)
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

    // Outbox claims.
    notification.HasIndex(r => new { r.PushStatus, r.NextPushAttemptAt });
  }
}
