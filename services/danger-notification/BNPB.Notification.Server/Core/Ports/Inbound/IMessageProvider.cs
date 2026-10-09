using BNPB.Notification.Server.Core.Models;

namespace BNPB.Notification.Server.Core.Ports.Inbound;

public interface IMessageProvider
{
  public Task<HazardNotification?> GetMessageAsync(
    string messageId,
    HazardView view,
    IEnumerable<UserPermission> permissions)
    => GetMessageAsync(messageId, view, permissions, CancellationToken.None);

  public Task<HazardNotification?> GetMessageAsync(
    string messageId,
    HazardView view,
    IEnumerable<UserPermission> permissions,
    CancellationToken cancellationToken);

  public Task<(IList<HazardNotification>, string)> GetMessageListAsync(
    HazardView view,
    HazardSource? source,
    HazardType? type,
    HazardSeverity? severity,
    DateTimeOffset? since,
    DateTimeOffset? until,
    int pageSize,
    string? pageToken,
    IEnumerable<UserPermission> permissions)
    => GetMessageListAsync(
      view,
      source,
      type,
      severity,
      since,
      until,
      pageSize,
      pageToken,
      permissions,
      CancellationToken.None);

  public Task<(IList<HazardNotification>, string)> GetMessageListAsync(
    HazardView view,
    HazardSource? source,
    HazardType? type,
    HazardSeverity? severity,
    DateTimeOffset? since,
    DateTimeOffset? until,
    int pageSize,
    string? pageToken,
    IEnumerable<UserPermission> permissions,
    CancellationToken cancellationToken);
}
