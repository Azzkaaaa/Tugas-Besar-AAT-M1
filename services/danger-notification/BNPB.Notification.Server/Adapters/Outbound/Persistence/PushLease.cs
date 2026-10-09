namespace BNPB.Notification.Server.Adapters.Outbound.Persistence;

// ExtraData entry on claimed notifications; proves the outbox claim on upsert/release.
public sealed record PushLease(Guid Token);
