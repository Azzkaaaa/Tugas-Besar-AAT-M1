using BNPB.Notification.Server.Core.Ports.Outbound;
using BNPB.Notification.Server.Core.Exceptions;
using BNPB.Notification.Server.Core.Models;

namespace BNPB.Notification.Server.Core.Services;

using IInboundMessageProvider = BNPB.Notification.Server.Core.Ports.Inbound.IMessageProvider;
using IOutboundMessageProvider = BNPB.Notification.Server.Core.Ports.Outbound.IMessageProvider;

public class MessageService : IInboundMessageProvider
{
  public const int DefaultPageSize = 20;
  public const int MaxPageSize = 100;

  private readonly ILogger<MessageService> _logger;
  private readonly IOutboundMessageProvider _messageProvider;
  private readonly IMessageListProvider _messageListProvider;
  private readonly IOutboundMessageProvider _summaryProvider;
  private readonly IMessageListProvider _summaryListProvider;

  public MessageService(
    ILogger<MessageService> logger,
    IOutboundMessageProvider messageProvider,
    IMessageListProvider messageListProvider,
    IOutboundMessageProvider summaryProvider,
    IMessageListProvider summaryListProvider)
  {
    _logger = logger;
    _messageProvider = messageProvider;
    _messageListProvider = messageListProvider;
    _summaryProvider = summaryProvider;
    _summaryListProvider = summaryListProvider;
  }

  public async Task<HazardEvent?> GetMessageAsync(
    string messageId,
    HazardView view,
    IEnumerable<UserPermission> permissions,
    CancellationToken cancellationToken)
  {
    if (string.IsNullOrWhiteSpace(messageId))
      throw new BadInputException("Notification id is required");

    var resolvedView = ResolveView(view, permissions);

    var message = resolvedView switch
    {
      HazardView.Full => await _messageProvider.GetMessageAsync(
        messageId,
        cancellationToken),

      _ => await _summaryProvider.GetMessageAsync(
        messageId,
        cancellationToken),
    };

    return message is { } m ? WithAlert(m) : null;
  }

  public async Task<(IList<HazardEvent>, string)> GetMessageListAsync(
    HazardView view,
    HazardSource? source,
    HazardType? type,
    HazardSeverity? severity,
    DateTimeOffset? since,
    DateTimeOffset? until,
    int pageSize,
    string? pageToken,
    IEnumerable<UserPermission> permissions,
    CancellationToken cancellationToken)
  {
    var resolvedView = ResolveView(view, permissions);

    if (pageSize == 0)
      pageSize = DefaultPageSize;

    if (pageSize < 1 || pageSize > MaxPageSize)
      throw new BadInputException($"Page size must be between 1 and {MaxPageSize}");

    if (since.HasValue && until.HasValue && since.Value >= until.Value)
      throw new BadInputException("since must precede until");

    var listProvider = resolvedView switch
    {
      HazardView.Full => _messageListProvider,
      _ => _summaryListProvider,
    };

    var (messages, nextPageToken) = await listProvider.GetMessageListAsync(
      source,
      type,
      severity,
      since,
      until,
      pageSize,
      pageToken,
      cancellationToken);

    return (messages.Select(WithAlert).ToList(), nextPageToken);
  }

  private static HazardView ResolveView(
    HazardView view,
    IEnumerable<UserPermission> permissions)
  {
    var (
      canReadNotifications,
      canReadHazardsFull,
      canReadHazardsSummary
    ) = Authorize(permissions);

    if (!canReadNotifications || !canReadHazardsSummary)
      throw new UnauthorizedException("User is not authorized to fetch notifications");

    var resolvedView = view == HazardView.Unknown ? HazardView.Summary : view;

    if (resolvedView == HazardView.Full && !canReadHazardsFull)
      throw new UnauthorizedException("User is not authorized to fetch full notifications");

    return resolvedView;
  }

  private static HazardEvent WithAlert(HazardEvent message)
    => message with { Alert = BuildAlert(message) };

  // Built exclusively from summary-safe fields.
  internal static string BuildAlert(HazardEvent message)
  {
    var severity = message.Severity switch
    {
      HazardSeverity.Awas => "AWAS",
      HazardSeverity.Siaga => "SIAGA",
      _ => "PERINGATAN",
    };

    var type = message.Type switch
    {
      HazardType.Seismic => "Gempa bumi",
      HazardType.Volcanic => "Aktivitas gunung api",
      _ => "Bahaya",
    };

    var source = message.Source switch
    {
      HazardSource.BMKG => "BMKG",
      HazardSource.PVMBG => "PVMBG",
      _ => "sumber tidak diketahui",
    };

    var area = string.IsNullOrWhiteSpace(message.AreaName)
      ? ""
      : $" di {message.AreaName}";

    var occurredAt = message.OccurredAt.ToUniversalTime().ToString("yyyy-MM-dd HH:mm 'UTC'");

    return $"[{severity}] {type}{area} ({source}), {occurredAt}";
  }

  private static (bool, bool, bool) Authorize(IEnumerable<UserPermission> permissions)
  {
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
          break;
      }
    }

    return
    (
      canReadNotifications,
      canReadHazardsFull,
      canReadHazardsSummary
    );
  }
}
