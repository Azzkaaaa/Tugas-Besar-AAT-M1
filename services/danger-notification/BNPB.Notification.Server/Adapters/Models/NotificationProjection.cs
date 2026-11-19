using BNPB.Common.V1;
using BNPB.Notification.V1;
using BNPB.Notification.Server.Core.Models;
using BNPB.Notification.Server.Core.Services;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using EPushStatus = BNPB.Notification.V1.DangerNotification.Types.PushStatus;
using PushStatus = BNPB.Notification.Server.Core.Models.PushStatus;

namespace BNPB.Notification.Server.Adapters.Models;

internal static class NotificationProjection
{
  public const int MaxResponseBytes = 16 * 1024 * 1024;
  // Leave space for freshness/cursor data. The complete RPC is checked exactly.
  public const int ContentBytes = MaxResponseBytes - 64 * 1024;

  private static DangerNotification Metadata(HazardNotification message) => new()
  {
    NotificationId = message.Id,
    Message = MessageService.BuildAlert(message),
    CreatedAt = Timestamp.FromDateTimeOffset(((NotificationCreatedAt)message.ExtraData[typeof(NotificationCreatedAt)]).Value),
    PushStatus = message.PushStatus switch
    {
      PushStatus.Pending => EPushStatus.Pending,
      PushStatus.Delivered => EPushStatus.Delivered,
      PushStatus.Failed => EPushStatus.Failed,
      _ => EPushStatus.Unspecified,
    },
  };

  public static DangerNotification ToProto(HazardNotification message)
  {
    var result = Metadata(message);
    result.Message = message.Alert;
    if (message.ExtraData.TryGetValue(typeof(HazardEvent), out var full))
      result.Hazard = new HazardProjection { Full = (HazardEvent)full };
    else if (message.ExtraData.TryGetValue(typeof(HazardSummary), out var summary))
      result.Hazard = new HazardProjection { Summary = (HazardSummary)summary };
    else
      throw new InvalidOperationException($"Message {message.Id} carries no hazard projection");
    return result;
  }

  // Complete field-1 framing is identical for Get.Item and List.Items.
  public static long ItemBytes(int itemBytes) => 1L + CodedOutputStream.ComputeLengthSize(itemBytes) + itemBytes;

  public static long FullItemUpperBound(HazardNotification header, int hazardBytes)
  {
    var projection = 1L + CodedOutputStream.ComputeLengthSize(hazardBytes) + hazardBytes;
    if (projection > MaxResponseBytes) return long.MaxValue;
    // A concurrent push transition can add its one-byte enum and tag.
    var item = Metadata(header).CalculateSize() + 2L + 1 + CodedOutputStream.ComputeLengthSize((int)projection) + projection;
    return item > MaxResponseBytes ? long.MaxValue : ItemBytes((int)item);
  }

  public static long ExactItemBytes(HazardNotification message)
    => ItemBytes(ToProto(message with { Alert = MessageService.BuildAlert(message) }).CalculateSize());
}
