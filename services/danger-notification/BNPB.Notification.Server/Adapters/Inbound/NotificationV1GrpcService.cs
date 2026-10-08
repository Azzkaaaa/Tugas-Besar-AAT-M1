using Grpc.Core;
using BNPB.Common.V1;
using BNPB.Notification.V1;
using Microsoft.AspNetCore.Authorization;
using BNPB.Notification.Server.Core.Models;
using BNPB.Notification.Server.Core.Exceptions;
using BNPB.Notification.Server.Core.Ports.Inbound;

using EHazardType = BNPB.Common.V1.HazardType;
using HazardType = BNPB.Notification.Server.Core.Models.HazardType;
using EHazardEvent = BNPB.Common.V1.HazardEvent;
using HazardEvent = BNPB.Notification.Server.Core.Models.HazardEvent;
using ESourceFreshness = BNPB.Common.V1.SourceFreshness;
using SourceFreshness = BNPB.Notification.Server.Core.Models.SourceFreshness;
using AdapterBadInputException = BNPB.Notification.Server.Adapters.Exceptions.BadInputException;

namespace BNPB.Notification.Server.Adapters.Inbound;

[Authorize]
public class NotificationV1GrpcService : DangerNotificationService.DangerNotificationServiceBase
{
  public const string DefaultScopeClaim = "scope";

  private readonly ILogger<NotificationV1GrpcService> _logger;
  private readonly IMessageProvider _messageProvider;
  private readonly ISubscriptionProvider _subscriptionProvider;
  private readonly ISourceProvider _sourceProvider;
  private readonly string _scopeClaim;

  public NotificationV1GrpcService(
    ILogger<NotificationV1GrpcService> logger,
    IMessageProvider messageProvider,
    ISubscriptionProvider subscriptionProvider,
    ISourceProvider sourceProvider,
    IConfiguration configuration)
  {
    _logger = logger;
    _messageProvider = messageProvider;
    _subscriptionProvider = subscriptionProvider;
    _sourceProvider = sourceProvider;
    _scopeClaim = configuration["Auth:ScopeClaim"] ?? DefaultScopeClaim;
  }

  public override Task<ListNotificationsResponse>
  ListNotifications(
    ListNotificationsRequest request,
    ServerCallContext context)
    => HandleAsync(async () =>
    {
      var filter = request.Filter ?? new HazardFilter();
      var page = request.Page ?? new PageRequest();
      var permissions = GetPermissions(context);

      var view = MapView(request.View);
      var source = MapSource(filter.Source);
      var type = MapType(filter.HazardType);
      ValidateSourceType(source, type);

      var response = new ListNotificationsResponse();

      // NORMAL/WASPADA never create notifications, so the result is always empty.
      if (filter.Severity is Severity.Normal or Severity.Waspada)
      {
        EnsureCanList(view, permissions);
      }
      else
      {
        var (items, nextPageToken) = await _messageProvider.GetMessageListAsync(
          view,
          source,
          type,
          MapSeverity(filter.Severity),
          filter.Since?.ToDateTimeOffset(),
          filter.Until?.ToDateTimeOffset(),
          page.PageSize,
          page.PageToken,
          permissions,
          context.CancellationToken);

        response.Items.AddRange(items.Select(ToDangerNotification));
        response.NextPageToken = nextPageToken;
      }

      var (freshness, partial) = await GetFreshnessAsync(
        RelevantSources(filter.Source, filter.HazardType),
        context.CancellationToken);

      response.Freshness.AddRange(freshness);
      response.Partial = partial;

      return response;
    });

  public override Task<GetNotificationResponse>
  GetNotification(
    GetNotificationRequest request,
    ServerCallContext context)
    => HandleAsync(async () =>
    {
      var item = await _messageProvider.GetMessageAsync(
        request.NotificationId,
        MapView(request.View),
        GetPermissions(context),
        context.CancellationToken);

      if (item is not { } message)
        throw new RpcException(new Status(StatusCode.NotFound, "Notification not found"));

      var (freshness, partial) = await GetFreshnessAsync(
        [ToProtoSource(message.Source)],
        context.CancellationToken);

      var response = new GetNotificationResponse
      {
        Item = ToDangerNotification(message),
        Partial = partial,
      };
      response.Freshness.AddRange(freshness);

      return response;
    });

  public override Task<RegisterPushTokenResponse>
  RegisterPushToken(
    RegisterPushTokenRequest request,
    ServerCallContext context)
    => HandleAsync(async () =>
    {
      if (string.IsNullOrWhiteSpace(request.FcmToken))
        throw new AdapterBadInputException("fcm_token is required");

      await _subscriptionProvider.SubscribeAsync(
        request.FcmToken,
        GetPermissions(context),
        context.CancellationToken);

      return new RegisterPushTokenResponse();
    });

  public override Task<UnregisterPushTokenResponse>
  UnregisterPushToken(
    UnregisterPushTokenRequest request,
    ServerCallContext context)
    => HandleAsync(async () =>
    {
      if (string.IsNullOrWhiteSpace(request.FcmToken))
        throw new AdapterBadInputException("fcm_token is required");

      await _subscriptionProvider.UnsubscribeAsync(
        request.FcmToken,
        context.CancellationToken);

      return new UnregisterPushTokenResponse();
    });

  private async Task<T> HandleAsync<T>(Func<Task<T>> action)
  {
    try
    {
      return await action();
    }
    catch (RpcException)
    {
      throw;
    }
    catch (UnauthorizedException ex)
    {
      throw new RpcException(new Status(StatusCode.PermissionDenied, ex.Message));
    }
    catch (BadInputException ex)
    {
      throw new RpcException(new Status(StatusCode.InvalidArgument, ex.Message));
    }
    catch (AdapterBadInputException ex)
    {
      throw new RpcException(new Status(StatusCode.InvalidArgument, ex.Message));
    }
    catch (OperationCanceledException)
    {
      throw new RpcException(new Status(StatusCode.Cancelled, "Request was cancelled"));
    }
    catch (Exception ex)
    {
      _logger.LogError(ex, "Unhandled error while processing request");
      throw new RpcException(new Status(StatusCode.Internal, "Internal error"));
    }
  }

  private List<UserPermission> GetPermissions(ServerCallContext context)
  {
    var user = context.GetHttpContext().User;

    return user.FindAll(_scopeClaim)
      .SelectMany(claim => claim.Value.Split(' ', StringSplitOptions.RemoveEmptyEntries))
      .Select(scope => scope switch
      {
        "notifications:read" => UserPermission.ReadNotifications,
        "hazards:read:full" => UserPermission.ReadHazardsFull,
        "hazards:read:summary" => UserPermission.ReadHazardsSummary,
        _ => UserPermission.Other,
      })
      .Where(permission => permission != UserPermission.Other)
      .Distinct()
      .ToList();
  }

  // Mirrors the core read rules for requests that are answered without reaching core.
  private static void EnsureCanList(HazardView view, IReadOnlyCollection<UserPermission> permissions)
  {
    if (!permissions.Contains(UserPermission.ReadNotifications)
      || !permissions.Contains(UserPermission.ReadHazardsSummary))
      throw new UnauthorizedException("User is not authorized to fetch notifications");

    if (view == HazardView.Full && !permissions.Contains(UserPermission.ReadHazardsFull))
      throw new UnauthorizedException("User is not authorized to fetch full notifications");
  }

  private async Task<(IList<ESourceFreshness>, bool)> GetFreshnessAsync(
    IEnumerable<Source> relevantSources,
    CancellationToken cancellationToken)
  {
    var available = (await _sourceProvider.GetSourceFreshnessAsync(cancellationToken))
      .Select(freshness => freshness.ExtraData.TryGetValue(typeof(ESourceFreshness), out var value)
        ? (ESourceFreshness)value
        : null)
      .OfType<ESourceFreshness>()
      .ToList();

    var result = relevantSources
      .Select(source =>
        available.FirstOrDefault(freshness => freshness.Source == source)
        ?? new ESourceFreshness { Source = source, Status = SourceStatus.Unavailable })
      .ToList();

    var partial = result.Any(freshness =>
      freshness.Status is SourceStatus.Stale or SourceStatus.Unavailable);

    return (result, partial);
  }

  internal static Source[] RelevantSources(Source source, EHazardType type)
  {
    if (source != Source.Unspecified) return [source];

    return type switch
    {
      EHazardType.Seismic => [Source.Bmkg],
      EHazardType.Volcanic => [Source.Pvmbg],
      _ => [Source.Bmkg, Source.Pvmbg],
    };
  }

  private static void ValidateSourceType(HazardSource? source, HazardType? type)
  {
    if ((source == HazardSource.BMKG && type == HazardType.Volcanic)
      || (source == HazardSource.PVMBG && type == HazardType.Seismic))
      throw new AdapterBadInputException("Source and hazard type combination is invalid");
  }

  private static DangerNotification ToDangerNotification(HazardEvent message)
  {
    var notification = new DangerNotification
    {
      NotificationId = message.Id,
      Message = message.Alert,
      PushStatus = DangerNotification.Types.PushStatus.Unspecified,
    };

    if (message.ExtraData.TryGetValue(typeof(EHazardEvent), out var full))
    {
      var hazard = (EHazardEvent)full;
      notification.Hazard = new HazardProjection { Full = hazard };
      notification.CreatedAt = hazard.IngestedAt;
    }
    else if (message.ExtraData.TryGetValue(typeof(HazardSummary), out var summary))
    {
      var hazard = (HazardSummary)summary;
      notification.Hazard = new HazardProjection { Summary = hazard };
      notification.CreatedAt = hazard.IngestedAt;
    }
    else
    {
      throw new InvalidOperationException($"Message {message.Id} carries no hazard projection");
    }

    return notification;
  }

  private static HazardView MapView(View view) => view switch
  {
    View.Unspecified => HazardView.Unknown,
    View.Summary => HazardView.Summary,
    View.Full => HazardView.Full,
    _ => throw new AdapterBadInputException("View is invalid"),
  };

  private static HazardSource? MapSource(Source source) => source switch
  {
    Source.Unspecified => null,
    Source.Bmkg => HazardSource.BMKG,
    Source.Pvmbg => HazardSource.PVMBG,
    _ => throw new AdapterBadInputException("Source is invalid"),
  };

  private static Source ToProtoSource(HazardSource source) => source switch
  {
    HazardSource.BMKG => Source.Bmkg,
    HazardSource.PVMBG => Source.Pvmbg,
    _ => Source.Unspecified,
  };

  private static HazardType? MapType(EHazardType type) => type switch
  {
    EHazardType.Unspecified => null,
    EHazardType.Seismic => HazardType.Seismic,
    EHazardType.Volcanic => HazardType.Volcanic,
    _ => throw new AdapterBadInputException("Hazard type is invalid"),
  };

  private static HazardSeverity? MapSeverity(Severity severity) => severity switch
  {
    Severity.Unspecified => null,
    Severity.Siaga => HazardSeverity.Siaga,
    Severity.Awas => HazardSeverity.Awas,
    Severity.Normal or Severity.Waspada => HazardSeverity.Other,
    _ => throw new AdapterBadInputException("Severity is invalid"),
  };
}
