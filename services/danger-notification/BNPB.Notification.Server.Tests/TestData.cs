using Google.Protobuf.WellKnownTypes;
using BNPB.Common.V1;
using BNPB.Notification.Server.Core.Models;

using EHazardType = BNPB.Common.V1.HazardType;
using HazardType = BNPB.Notification.Server.Core.Models.HazardType;
using EHazardEvent = BNPB.Common.V1.HazardEvent;
using BNPB.Event.V1;
using BNPB.Notification.Server.Adapters.Models;
using ESourceFreshness = BNPB.Common.V1.SourceFreshness;
using SourceFreshness = BNPB.Notification.Server.Core.Models.SourceFreshness;

namespace BNPB.Notification.Server.Tests;

internal static class TestData
{
  public static readonly DateTimeOffset BaseTime = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

  public static HazardSummary Summary(
    string id,
    Severity severity = Severity.Awas,
    Source source = Source.Bmkg,
    int minutes = 0)
    => new()
    {
      HazardId = id,
      Source = source,
      HazardType = source == Source.Pvmbg ? EHazardType.Volcanic : EHazardType.Seismic,
      Severity = severity,
      AreaName = $"Area {id}",
      OccurredAt = Timestamp.FromDateTimeOffset(BaseTime.AddMinutes(minutes)),
      IngestedAt = Timestamp.FromDateTimeOffset(BaseTime.AddMinutes(minutes + 1)),
    };

  public static EHazardEvent Full(
    string id,
    Severity severity = Severity.Awas,
    Source source = Source.Bmkg,
    int minutes = 0)
    => new()
    {
      HazardId = id,
      Source = source,
      SourceRefId = $"ref-secret-{id}",
      HazardType = source == Source.Pvmbg ? EHazardType.Volcanic : EHazardType.Seismic,
      Severity = severity,
      AreaName = $"Area {id}",
      Latitude = -6.2,
      Longitude = 106.8,
      OccurredAt = Timestamp.FromDateTimeOffset(BaseTime.AddMinutes(minutes)),
      IngestedAt = Timestamp.FromDateTimeOffset(BaseTime.AddMinutes(minutes + 1)),
      Attributes = new Struct(),
    };

  public static HazardNotification Notification(
    string id,
    HazardSeverity severity = HazardSeverity.Awas,
    object? extra = null,
    PushStatus pushStatus = PushStatus.Unspecified)
  {
    var extraData = ImmutableDictionary<System.Type, object>.Empty
      .Add(typeof(NotificationCreatedAt), new NotificationCreatedAt(BaseTime.AddMinutes(5)));

    if (extra != null)
      extraData = extraData.Add(extra.GetType(), extra);

    return new HazardNotification(
      id,
      HazardSource.BMKG,
      HazardType.Seismic,
      severity,
      $"Area {id}",
      BaseTime,
      "",
      pushStatus,
      extraData);
  }

  // A notification as the RabbitMQ consumer builds it for one published revision.
  public static HazardNotification Published(
    string eventId,
    string hazardId,
    Severity severity = Severity.Awas,
    Source source = Source.Bmkg,
    int publishedMinutes = 0,
    int occurredMinutes = 0)
  {
    var hazard = Full(hazardId, severity, source, occurredMinutes);
    var envelope = new HazardEventPublished
    {
      EventId = eventId,
      SchemaVersion = 1,
      Source = source,
      Publisher = "aggregator",
      OccurredAt = Timestamp.FromDateTimeOffset(BaseTime.AddMinutes(publishedMinutes)),
      Payload = hazard,
    };

    return new HazardNotification(
      eventId,
      source == Source.Pvmbg ? HazardSource.PVMBG : HazardSource.BMKG,
      source == Source.Pvmbg ? HazardType.Volcanic : HazardType.Seismic,
      severity switch
      {
        Severity.Siaga => HazardSeverity.Siaga,
        Severity.Awas => HazardSeverity.Awas,
        _ => HazardSeverity.Other,
      },
      hazard.AreaName,
      hazard.OccurredAt.ToDateTimeOffset(),
      "",
      PushStatus.Unspecified,
      ImmutableDictionary<System.Type, object>.Empty
        .Add(typeof(HazardEventPublished), envelope)
        .Add(typeof(EHazardEvent), hazard));
  }

  public static ESourceFreshness ProtoFreshness(Source source, SourceStatus status)
    => new()
    {
      Source = source,
      Status = status,
      LastSuccessAt = Timestamp.FromDateTimeOffset(BaseTime),
    };

  public static SourceFreshness Freshness(Source source, SourceStatus status)
    => new(ImmutableDictionary<System.Type, object>.Empty
      .Add(typeof(ESourceFreshness), ProtoFreshness(source, status)));
}
