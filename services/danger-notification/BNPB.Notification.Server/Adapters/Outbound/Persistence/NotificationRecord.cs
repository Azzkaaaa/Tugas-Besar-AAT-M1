using BNPB.Common.V1;

using PushStatus = BNPB.Notification.V1.DangerNotification.Types.PushStatus;

namespace BNPB.Notification.Server.Adapters.Outbound.Persistence;

// Enums use the protobuf numbering, which is stable. Times are UTC unix
// milliseconds: SQLite cannot order or compare DateTimeOffset.
public class NotificationRecord
{
  // Publication event_id.
  public string Id { get; set; } = "";
  public string HazardId { get; set; } = "";
  public Source Source { get; set; }
  public HazardType HazardType { get; set; }
  public Severity Severity { get; set; }
  public string AreaName { get; set; } = "";
  public long OccurredAt { get; set; }
  // Envelope occurred_at; orders revisions of one hazard.
  public long PublishedAt { get; set; }
  public long CreatedAt { get; set; }
  // Full HazardEvent at notification creation, as protobuf.
  public byte[] Hazard { get; set; } = [];
  // False once a newer revision of the same hazard is stored.
  public bool IsCurrent { get; set; }
  public PushStatus PushStatus { get; set; }
  public int FailedPushAttempts { get; set; }
  public long? NextPushAttemptAt { get; set; }
  public Guid? LeaseToken { get; set; }
  public long? LeaseExpiresAt { get; set; }
  // W3C traceparent of the consume span, so the deferred send joins its trace.
  public string? TraceParent { get; set; }
}
