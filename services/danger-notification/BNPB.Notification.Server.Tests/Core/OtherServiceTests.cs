using Microsoft.Extensions.Logging.Abstractions;
using BNPB.Common.V1;
using BNPB.Notification.Server.Core.Models;
using BNPB.Notification.Server.Core.Services;
using BNPB.Notification.Server.Core.Exceptions;
using BNPB.Notification.Server.Core.Ports.Outbound;

using SourceFreshness = BNPB.Notification.Server.Core.Models.SourceFreshness;
using HazardEvent = BNPB.Notification.Server.Core.Models.HazardEvent;

namespace BNPB.Notification.Server.Tests.Core;

public class SubscriptionServiceTests
{
  private readonly IPushProvider _push = Substitute.For<IPushProvider>();
  private readonly SubscriptionService _service;

  public SubscriptionServiceTests()
  {
    _service = new SubscriptionService(NullLogger<SubscriptionService>.Instance, _push);
  }

  [Fact]
  public async Task Subscribe_WithScopes_SubscribesToken()
  {
    await _service.SubscribeAsync(
      "token",
      [UserPermission.ReadNotifications, UserPermission.ReadHazardsSummary],
      CancellationToken.None);

    await _push.Received(1).SubscribeAsync("token", Arg.Any<CancellationToken>());
  }

  [Fact]
  public async Task Subscribe_WithoutNotificationScope_IsUnauthorized()
  {
    await Assert.ThrowsAsync<UnauthorizedException>(() => _service.SubscribeAsync(
      "token",
      [UserPermission.ReadHazardsSummary],
      CancellationToken.None));

    await _push.DidNotReceiveWithAnyArgs().SubscribeAsync(default!, default);
  }

  [Fact]
  public async Task Unsubscribe_UnsubscribesToken()
  {
    await _service.UnsubscribeAsync("token", CancellationToken.None);

    await _push.Received(1).UnsubscribeAsync("token", Arg.Any<CancellationToken>());
  }
}

public class BroadcastServiceTests
{
  private readonly IPushProvider _push = Substitute.For<IPushProvider>();
  private readonly BroadcastService _service;

  public BroadcastServiceTests()
  {
    _service = new BroadcastService(NullLogger<BroadcastService>.Instance, _push);
  }

  [Theory]
  [InlineData(HazardSeverity.Siaga, 1)]
  [InlineData(HazardSeverity.Awas, 1)]
  [InlineData(HazardSeverity.Other, 0)]
  public async Task FilterAndBroadcast_OnlyPushesSiagaAndAwas(HazardSeverity severity, int expectedCalls)
  {
    await _service.FilterAndBroadcastAsync(TestData.Event("h1", severity), CancellationToken.None);

    await _push.Received(expectedCalls).BroadcastAsync(Arg.Any<HazardEvent>(), Arg.Any<CancellationToken>());
  }
}

public class SourceServiceTests
{
  [Fact]
  public async Task GetSourceFreshness_PassesThroughOutboundProvider()
  {
    var outbound = Substitute.For<ISourceProvider>();
    IReadOnlyList<SourceFreshness> freshness =
    [
      TestData.Freshness(Source.Bmkg, SourceStatus.Fresh),
      TestData.Freshness(Source.Pvmbg, SourceStatus.Stale),
    ];
    outbound.GetSourceFreshnessAsync(Arg.Any<CancellationToken>()).Returns(freshness);

    var service = new SourceService(NullLogger<SourceService>.Instance, outbound);

    var result = await service.GetSourceFreshnessAsync(CancellationToken.None);

    Assert.Same(freshness, result);
  }
}
