using BNPB.Notification.Server.Core.Models;

namespace BNPB.Notification.Server.Core.Ports.Outbound;

public interface IMessageStore
{
  public Task UpsertMessageAsync(HazardNotification message)
    => UpsertMessageAsync(message, CancellationToken.None);

  // Persists the whole notification, keyed by Id.
  // - A newer revision of the same hazard outdates the older ones in the same write;
  //   an older revision is stored already outdated. Outdated notifications are never
  //   returned or pushed, and their push status becomes Unspecified unless Delivered.
  // - Push status never regresses (Pending < Failed < Delivered), so upserting a
  //   redelivered message is idempotent.
  // - A notification claimed from IPushOutbox only changes push status while its
  //   claim is still held; the upsert also ends the claim.
  public Task UpsertMessageAsync(
    HazardNotification message,
    CancellationToken cancellationToken);
}
