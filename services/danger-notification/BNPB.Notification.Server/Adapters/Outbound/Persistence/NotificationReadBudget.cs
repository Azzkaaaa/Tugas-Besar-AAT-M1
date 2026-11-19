namespace BNPB.Notification.Server.Adapters.Outbound.Persistence;

// Shared by the FULL and SUMMARY store adapters in this process. A legacy
// summary repair owns this slot through query cancellation/disposal, parsing,
// allowlist copying and cache persistence. Waiting callers retain headers only.
public sealed class NotificationReadBudget
{
  internal SemaphoreSlim LegacySummary { get; } = new(1, 1);
}
