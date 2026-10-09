namespace BNPB.Notification.Server.Core.Models;

// Only the fields core logic reads are typed. Everything else (hazard projection,
// envelope, creation time, trace context, outbox lease) is adapter-owned ExtraData.
public readonly record struct HazardNotification
(
  string Id,
  HazardSource Source,
  HazardType Type,
  HazardSeverity Severity,
  string AreaName,
  DateTimeOffset OccurredAt,
  string Alert,
  PushStatus PushStatus,
  ImmutableDictionary<Type, object> ExtraData
);
