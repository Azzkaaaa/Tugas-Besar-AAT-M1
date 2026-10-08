using System.Security.Claims;
using System.Text.Encodings.Web;
using Grpc.Core;
using NSubstitute.ClearExtensions;
using NSubstitute.ExceptionExtensions;
using Grpc.Net.Client;
using Grpc.Health.V1;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using BNPB.Common.V1;
using BNPB.Notification.V1;
using BNPB.Notification.Server.Core.Models;
using BNPB.Notification.Server.Core.Exceptions;
using BNPB.Notification.Server.Core.Ports.Inbound;
using BNPB.Notification.Server.Adapters.Inbound;

using HazardEvent = BNPB.Notification.Server.Core.Models.HazardEvent;
using HazardType = BNPB.Notification.Server.Core.Models.HazardType;
using SourceFreshness = BNPB.Notification.Server.Core.Models.SourceFreshness;

namespace BNPB.Notification.Server.Tests.Adapters;

public class NotificationV1GrpcServiceTests : IClassFixture<NotificationV1GrpcServiceTests.Factory>
{
  private const string SummaryScopes = "notifications:read hazards:read:summary";
  private const string FullScopes = "notifications:read hazards:read:summary hazards:read:full";

  private readonly Factory _factory;
  private readonly IMessageProvider _messages;
  private readonly ISubscriptionProvider _subscriptions;
  private readonly ISourceProvider _sources;

  public NotificationV1GrpcServiceTests(Factory factory)
  {
    _factory = factory;
    _factory.ResetSubstitutes();
    _messages = factory.Messages;
    _subscriptions = factory.Subscriptions;
    _sources = factory.Sources;

    IReadOnlyList<SourceFreshness> freshness =
    [
      TestData.Freshness(Source.Bmkg, SourceStatus.Fresh),
      TestData.Freshness(Source.Pvmbg, SourceStatus.Stale),
    ];
    _sources.GetSourceFreshnessAsync(Arg.Any<CancellationToken>()).Returns(freshness);
  }

  private DangerNotificationService.DangerNotificationServiceClient Client()
    => new(_factory.Channel);

  private static Metadata As(string? scopes)
    => scopes == null ? [] : new Metadata { { TestAuthHandler.ScopeHeader, scopes } };

  private static HazardEvent SummaryEvent(string id)
    => TestData.Event(id, extra: TestData.Summary(id)) with { Alert = $"alert {id}" };

  private void SetupList(params HazardEvent[] items)
    => _messages
      .GetMessageListAsync(default, default, default, default, default, default, default, default, default!, default)
      .ReturnsForAnyArgs((items.ToList(), "next-token"));

  [Fact]
  public async Task ListNotifications_MapsItemsAndFiltersFreshnessBySource()
  {
    SetupList(SummaryEvent("h1"));

    var response = await Client().ListNotificationsAsync(
      new ListNotificationsRequest
      {
        Filter = new HazardFilter { Source = Source.Bmkg },
        Page = new PageRequest { PageSize = 5, PageToken = "tok" },
      },
      As(SummaryScopes));

    var item = Assert.Single(response.Items);
    Assert.Equal("h1", item.NotificationId);
    Assert.Equal("alert h1", item.Message);
    Assert.Equal(HazardProjection.DataOneofCase.Summary, item.Hazard.DataCase);
    Assert.Equal(item.Hazard.Summary.IngestedAt, item.CreatedAt);
    Assert.Equal(DangerNotification.Types.PushStatus.Unspecified, item.PushStatus);
    Assert.Equal("next-token", response.NextPageToken);

    var freshness = Assert.Single(response.Freshness);
    Assert.Equal(Source.Bmkg, freshness.Source);
    Assert.False(response.Partial);

    await _messages.Received(1).GetMessageListAsync(
      HazardView.Unknown,
      HazardSource.BMKG,
      null,
      null,
      null,
      null,
      5,
      "tok",
      Arg.Is<IEnumerable<UserPermission>>(p =>
        p.Contains(UserPermission.ReadNotifications) && p.Contains(UserPermission.ReadHazardsSummary)),
      Arg.Any<CancellationToken>());
  }

  [Fact]
  public async Task ListNotifications_NoSourceFilter_ReportsAllSourcesAndPartial()
  {
    SetupList();

    var response = await Client().ListNotificationsAsync(new ListNotificationsRequest(), As(SummaryScopes));

    Assert.Equal([Source.Bmkg, Source.Pvmbg], response.Freshness.Select(f => f.Source));
    Assert.True(response.Partial);
  }

  [Fact]
  public async Task ListNotifications_VolcanicFilter_OnlyPvmbgFreshness()
  {
    SetupList();

    var response = await Client().ListNotificationsAsync(
      new ListNotificationsRequest { Filter = new HazardFilter { HazardType = BNPB.Common.V1.HazardType.Volcanic } },
      As(SummaryScopes));

    Assert.Equal(Source.Pvmbg, Assert.Single(response.Freshness).Source);
    Assert.True(response.Partial);
  }

  [Theory]
  [InlineData(Severity.Normal)]
  [InlineData(Severity.Waspada)]
  public async Task ListNotifications_NonNotificationSeverity_IsEmptyWithoutCallingCore(Severity severity)
  {
    var response = await Client().ListNotificationsAsync(
      new ListNotificationsRequest { Filter = new HazardFilter { Severity = severity } },
      As(SummaryScopes));

    Assert.Empty(response.Items);
    Assert.Equal("", response.NextPageToken);
    Assert.Equal(2, response.Freshness.Count);
    await _messages.DidNotReceiveWithAnyArgs().GetMessageListAsync(
      default, default, default, default, default, default, default, default, default!, default);
  }

  [Fact]
  public async Task ListNotifications_NonNotificationSeverityWithoutScope_IsPermissionDenied()
  {
    var ex = await Assert.ThrowsAsync<RpcException>(() => Client().ListNotificationsAsync(
      new ListNotificationsRequest { Filter = new HazardFilter { Severity = Severity.Normal } },
      As("hazards:read:summary")).ResponseAsync);

    Assert.Equal(StatusCode.PermissionDenied, ex.StatusCode);
  }

  [Fact]
  public async Task ListNotifications_InvalidSourceTypeCombination_IsInvalidArgument()
  {
    var ex = await Assert.ThrowsAsync<RpcException>(() => Client().ListNotificationsAsync(
      new ListNotificationsRequest
      {
        Filter = new HazardFilter { Source = Source.Bmkg, HazardType = BNPB.Common.V1.HazardType.Volcanic },
      },
      As(SummaryScopes)).ResponseAsync);

    Assert.Equal(StatusCode.InvalidArgument, ex.StatusCode);
  }

  [Fact]
  public async Task ListNotifications_CoreBadInput_IsInvalidArgument()
  {
    _messages
      .GetMessageListAsync(default, default, default, default, default, default, default, default, default!, default)
      .ThrowsAsyncForAnyArgs(new BadInputException("bad page size"));

    var ex = await Assert.ThrowsAsync<RpcException>(
      () => Client().ListNotificationsAsync(new ListNotificationsRequest(), As(SummaryScopes)).ResponseAsync);

    Assert.Equal(StatusCode.InvalidArgument, ex.StatusCode);
  }

  [Fact]
  public async Task GetNotification_ReturnsItemWithItsSourceFreshness()
  {
    _messages
      .GetMessageAsync("h1", HazardView.Full, Arg.Any<IEnumerable<UserPermission>>(), Arg.Any<CancellationToken>())
      .Returns(TestData.Event("h1", extra: TestData.Full("h1")));

    var response = await Client().GetNotificationAsync(
      new GetNotificationRequest { NotificationId = "h1", View = View.Full },
      As(FullScopes));

    Assert.Equal(HazardProjection.DataOneofCase.Full, response.Item.Hazard.DataCase);
    Assert.Equal(Source.Bmkg, Assert.Single(response.Freshness).Source);
    Assert.False(response.Partial);
  }

  [Fact]
  public async Task GetNotification_SourceMissingFromFreshness_IsReportedUnavailable()
  {
    _sources.GetSourceFreshnessAsync(Arg.Any<CancellationToken>()).Returns(new List<SourceFreshness>());
    _messages
      .GetMessageAsync("h1", Arg.Any<HazardView>(), Arg.Any<IEnumerable<UserPermission>>(), Arg.Any<CancellationToken>())
      .Returns(SummaryEvent("h1"));

    var response = await Client().GetNotificationAsync(
      new GetNotificationRequest { NotificationId = "h1" },
      As(SummaryScopes));

    var freshness = Assert.Single(response.Freshness);
    Assert.Equal(SourceStatus.Unavailable, freshness.Status);
    Assert.True(response.Partial);
  }

  [Fact]
  public async Task GetNotification_Missing_IsNotFound()
  {
    _messages
      .GetMessageAsync(default!, default, default!, default)
      .ReturnsForAnyArgs((HazardEvent?)null);

    var ex = await Assert.ThrowsAsync<RpcException>(() => Client().GetNotificationAsync(
      new GetNotificationRequest { NotificationId = "missing" },
      As(SummaryScopes)).ResponseAsync);

    Assert.Equal(StatusCode.NotFound, ex.StatusCode);
  }

  [Fact]
  public async Task GetNotification_CoreUnauthorized_IsPermissionDenied()
  {
    _messages
      .GetMessageAsync(default!, default, default!, default)
      .ThrowsAsyncForAnyArgs(new UnauthorizedException("no"));

    var ex = await Assert.ThrowsAsync<RpcException>(() => Client().GetNotificationAsync(
      new GetNotificationRequest { NotificationId = "h1", View = View.Full },
      As(SummaryScopes)).ResponseAsync);

    Assert.Equal(StatusCode.PermissionDenied, ex.StatusCode);
  }

  [Fact]
  public async Task Rpc_WithoutCredentials_IsUnauthenticated()
  {
    var ex = await Assert.ThrowsAsync<RpcException>(() => Client().GetNotificationAsync(
      new GetNotificationRequest { NotificationId = "h1" },
      As(null)).ResponseAsync);

    Assert.Equal(StatusCode.Unauthenticated, ex.StatusCode);
  }

  [Fact]
  public async Task RegisterPushToken_SubscribesWithCallerPermissions()
  {
    await Client().RegisterPushTokenAsync(new RegisterPushTokenRequest { FcmToken = "fcm" }, As(SummaryScopes));

    await _subscriptions.Received(1).SubscribeAsync(
      "fcm",
      Arg.Is<IEnumerable<UserPermission>>(p => p.Count() == 2),
      Arg.Any<CancellationToken>());
  }

  [Fact]
  public async Task UnregisterPushToken_Unsubscribes()
  {
    await Client().UnregisterPushTokenAsync(new UnregisterPushTokenRequest { FcmToken = "fcm" }, As(SummaryScopes));

    await _subscriptions.Received(1).UnsubscribeAsync("fcm", Arg.Any<CancellationToken>());
  }

  [Fact]
  public async Task PushTokenRpcs_EmptyToken_IsInvalidArgument()
  {
    var register = await Assert.ThrowsAsync<RpcException>(() => Client().RegisterPushTokenAsync(
      new RegisterPushTokenRequest(), As(SummaryScopes)).ResponseAsync);
    var unregister = await Assert.ThrowsAsync<RpcException>(() => Client().UnregisterPushTokenAsync(
      new UnregisterPushTokenRequest(), As(SummaryScopes)).ResponseAsync);

    Assert.Equal(StatusCode.InvalidArgument, register.StatusCode);
    Assert.Equal(StatusCode.InvalidArgument, unregister.StatusCode);
  }

  [Fact]
  public async Task HealthCheck_IsServingWithoutCredentials()
  {
    var health = new Health.HealthClient(_factory.Channel);

    var response = await health.CheckAsync(new HealthCheckRequest());

    Assert.Equal(HealthCheckResponse.Types.ServingStatus.Serving, response.Status);
  }

  public class Factory : WebApplicationFactory<Program>
  {
    public IMessageProvider Messages { get; } = Substitute.For<IMessageProvider>();
    public ISubscriptionProvider Subscriptions { get; } = Substitute.For<ISubscriptionProvider>();
    public ISourceProvider Sources { get; } = Substitute.For<ISourceProvider>();

    private GrpcChannel? _channel;

    public GrpcChannel Channel => _channel ??= GrpcChannel.ForAddress(
      Server.BaseAddress,
      new GrpcChannelOptions { HttpHandler = Server.CreateHandler() });

    public void ResetSubstitutes()
    {
      Messages.ClearSubstitute();
      Subscriptions.ClearSubstitute();
      Sources.ClearSubstitute();
    }

    protected override void ConfigureWebHost(IWebHostBuilder builder)
    {
      builder.UseSetting("Aggregator:Address", "http://aggregator.invalid:9091");

      builder.ConfigureTestServices(services =>
      {
        services.RemoveAll<IMessageProvider>();
        services.RemoveAll<ISubscriptionProvider>();
        services.RemoveAll<ISourceProvider>();
        services.AddSingleton(Messages);
        services.AddSingleton(Subscriptions);
        services.AddSingleton(Sources);

        var consumer = services.Single(d => d.ImplementationType == typeof(RabbitMqConsumer));
        services.Remove(consumer);

        services
          .AddAuthentication(options =>
          {
            options.DefaultAuthenticateScheme = TestAuthHandler.SchemeName;
            options.DefaultChallengeScheme = TestAuthHandler.SchemeName;
          })
          .AddScheme<AuthenticationSchemeOptions, TestAuthHandler>(TestAuthHandler.SchemeName, _ => { });
      });
    }

    protected override void Dispose(bool disposing)
    {
      _channel?.Dispose();
      base.Dispose(disposing);
    }
  }

  // Authenticates any request carrying the scope header, with that header as the scope claim.
  public class TestAuthHandler(
    IOptionsMonitor<AuthenticationSchemeOptions> options,
    ILoggerFactory logger,
    UrlEncoder encoder)
    : AuthenticationHandler<AuthenticationSchemeOptions>(options, logger, encoder)
  {
    public const string SchemeName = "Test";
    public const string ScopeHeader = "x-test-scopes";

    protected override Task<AuthenticateResult> HandleAuthenticateAsync()
    {
      if (!Request.Headers.TryGetValue(ScopeHeader, out var scopes))
        return Task.FromResult(AuthenticateResult.NoResult());

      var identity = new ClaimsIdentity([new Claim("scope", scopes.ToString())], SchemeName);
      var ticket = new AuthenticationTicket(new ClaimsPrincipal(identity), SchemeName);

      return Task.FromResult(AuthenticateResult.Success(ticket));
    }
  }
}
