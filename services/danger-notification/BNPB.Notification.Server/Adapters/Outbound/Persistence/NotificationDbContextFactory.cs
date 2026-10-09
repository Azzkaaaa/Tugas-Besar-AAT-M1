using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Design;

namespace BNPB.Notification.Server.Adapters.Outbound.Persistence;

// Used by `dotnet ef` only, so migrations don't boot the whole host.
public class NotificationDbContextFactory : IDesignTimeDbContextFactory<NotificationDbContext>
{
  public NotificationDbContext CreateDbContext(string[] args)
    => new(new DbContextOptionsBuilder<NotificationDbContext>()
      .UseSqlite("Data Source=notifications.db")
      .Options);
}
