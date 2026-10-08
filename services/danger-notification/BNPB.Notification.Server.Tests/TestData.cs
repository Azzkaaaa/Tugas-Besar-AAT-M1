using Google.Protobuf.WellKnownTypes;
using BNPB.Common.V1;
using BNPB.Notification.Server.Core.Models;

using EHazardType = BNPB.Common.V1.HazardType;
using HazardType = BNPB.Notification.Server.Core.Models.HazardType;
using EHazardEvent = BNPB.Common.V1.HazardEvent;
using HazardEvent = BNPB.Notification.Server.Core.Models.HazardEvent;
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

  public static HazardEvent Event(
    string id,
    HazardSeverity severity = HazardSeverity.Awas,
    object? extra = null)
  {
    var extraData = extra == null
      ? ImmutableDictionary<System.Type, object>.Empty
      : ImmutableDictionary<System.Type, object>.Empty.Add(extra.GetType(), extra);

    return new HazardEvent(
      id,
      HazardSource.BMKG,
      HazardType.Seismic,
      severity,
      $"Area {id}",
      BaseTime,
      "",
      extraData);
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
