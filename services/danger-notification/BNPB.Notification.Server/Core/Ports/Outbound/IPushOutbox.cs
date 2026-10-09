using BNPB.Notification.Server.Core.Models;

namespace BNPB.Notification.Server.Core.Ports.Outbound;

public interface IPushOutbox
{
  public Task<IReadOnlyList<HazardNotification>> ClaimDuePushesAsync(int limit)
    => ClaimDuePushesAsync(limit, CancellationToken.None);

  // Claims up to limit notifications that need a push attempt. Claimed notifications
  // are hidden from other claimers until they are upserted, released, or the claim
  // lapses. Hand the returned notifications back unchanged apart from PushStatus.
  public Task<IReadOnlyList<HazardNotification>> ClaimDuePushesAsync(
    int limit,
    CancellationToken cancellationToken);

  public Task ReleasePushesAsync(IEnumerable<HazardNotification> messages)
    => ReleasePushesAsync(messages, CancellationToken.None);

  // Gives back claimed notifications that were not attempted.
  public Task ReleasePushesAsync(
    IEnumerable<HazardNotification> messages,
    CancellationToken cancellationToken);
}
