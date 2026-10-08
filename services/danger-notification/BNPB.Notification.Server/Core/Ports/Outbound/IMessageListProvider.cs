using BNPB.Notification.Server.Core.Models;

namespace BNPB.Notification.Server.Core.Ports.Outbound;

public interface IMessageListProvider
{
  public Task<(IList<HazardEvent>, string)> GetMessageListAsync(
    HazardSource? source,
    HazardType? type,
    HazardSeverity? severity,
    DateTimeOffset? since,
    DateTimeOffset? until,
    int pageSize,
    string? pageToken)
    => GetMessageListAsync(
      source,
      type,
      severity,
      since,
      until,
      pageSize,
      pageToken,
      CancellationToken.None);

  public Task<(IList<HazardEvent>, string)> GetMessageListAsync(
    HazardSource? source,
    HazardType? type,
    HazardSeverity? severity,
    DateTimeOffset? since,
    DateTimeOffset? until,
    int pageSize,
    string? pageToken,
    CancellationToken cancellationToken);
}
