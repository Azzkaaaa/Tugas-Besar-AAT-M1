namespace BNPB.Notification.Server.Adapters.Outbound.Persistence;

// Bound from the "Outbox" configuration section.
public sealed class OutboxOptions
{
  public TimeSpan PollInterval { get; set; } = TimeSpan.FromSeconds(5);
  public int BatchSize { get; set; } = 10;
  public int MaxAttempts { get; set; } = 5;
  public TimeSpan BaseBackoff { get; set; } = TimeSpan.FromSeconds(5);
  // Must outlast BatchSize sends, or a slow batch is claimed twice.
  public TimeSpan LeaseDuration { get; set; } = TimeSpan.FromSeconds(60);
}
