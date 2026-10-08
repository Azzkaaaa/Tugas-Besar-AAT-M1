using BNPB.Notification.Server.Core.Models;
using BNPB.Notification.Server.Core.Exceptions;
using BNPB.Notification.Server.Core.Ports.Inbound;
using BNPB.Notification.Server.Core.Ports.Outbound;

namespace BNPB.Notification.Server.Core.Services;

public class SubscriptionService : ISubscriptionProvider
{
  private readonly ILogger<SubscriptionService> _logger;
  private readonly IPushProvider _pushProvider;

  public SubscriptionService(
    ILogger<SubscriptionService> logger,
    IPushProvider pushProvider)
  {
    _logger = logger;
    _pushProvider = pushProvider;
  }

  public async Task SubscribeAsync(
    string token,
    IEnumerable<UserPermission> permissions,
    CancellationToken cancellationToken)
  {
    await UnsubscribeAsync(token, cancellationToken);

    bool canReadNotifications = false;
    bool canReadHazardsFull = false;
    bool canReadHazardsSummary = false;

    foreach (UserPermission permission in permissions)
    {
      switch (permission)
      {
        case UserPermission.ReadNotifications:
          canReadNotifications = true;
          break;
        case UserPermission.ReadHazardsFull:
          canReadHazardsFull = true;
          break;
        case UserPermission.ReadHazardsSummary:
          canReadHazardsSummary = true;
          goto default;
        default:
          continue;
      }

      if (canReadNotifications && canReadHazardsFull) break;
    }

    if (!canReadNotifications || !(canReadHazardsFull || canReadHazardsSummary))
      throw new UnauthorizedException("User is not authorized to subscribe");

    await _pushProvider.SubscribeAsync(token, cancellationToken);
  }

  public async Task UnsubscribeAsync(
    string token,
    CancellationToken cancellationToken)
  {
    await _pushProvider.UnsubscribeAsync(
      token,
      cancellationToken);
  }
}
