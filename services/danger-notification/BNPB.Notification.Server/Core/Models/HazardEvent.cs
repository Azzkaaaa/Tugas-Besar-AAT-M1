namespace BNPB.Notification.Server.Core.Models;

public readonly record struct HazardEvent
(
  string Id,
  HazardSource Source,
  HazardType Type,
  HazardSeverity Severity,
  string AreaName,
  DateTimeOffset OccurredAt,
  string Alert,
  ImmutableDictionary<Type, object> ExtraData
);
