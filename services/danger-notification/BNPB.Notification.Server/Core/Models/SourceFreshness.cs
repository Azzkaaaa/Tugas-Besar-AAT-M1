namespace BNPB.Notification.Server.Core.Models;

public readonly record struct SourceFreshness
(
  ImmutableDictionary<Type, object> ExtraData
);
