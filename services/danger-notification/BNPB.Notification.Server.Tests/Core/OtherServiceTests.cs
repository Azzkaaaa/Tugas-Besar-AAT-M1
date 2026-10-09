using Microsoft.Extensions.Logging.Abstractions;
using BNPB.Common.V1;
using BNPB.Notification.Server.Core.Models;
using BNPB.Notification.Server.Core.Services;
using BNPB.Notification.Server.Core.Exceptions;
using BNPB.Notification.Server.Core.Ports.Outbound;

using SourceFreshness = BNPB.Notification.Server.Core.Models.SourceFreshness;

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
  private readonly IMessageStore _store = Substitute.For<IMessageStore>();
  private readonly BroadcastService _service;

  public BroadcastServiceTests()
  {
    _service = new BroadcastService(NullLogger<BroadcastService>.Instance, _store);
  }

  [Theory]
  [InlineData(HazardSeverity.Siaga)]
  [InlineData(HazardSeverity.Awas)]
  public async Task FilterAndBroadcast_SiagaOrAwas_UpsertsAsPending(HazardSeverity severity)
  {
    await _service.FilterAndBroadcastAsync(TestData.Notification("e1", severity), CancellationToken.None);

    await _store.Received(1).UpsertMessageAsync(
      Arg.Is<HazardNotification>(n => n.Id == "e1" && n.PushStatus == PushStatus.Pending),
      Arg.Any<CancellationToken>());
  }

  [Fact]
  public async Task FilterAndBroadcast_OtherSeverity_IsDropped()
  {
    await _service.FilterAndBroadcastAsync(TestData.Notification("e1", HazardSeverity.Other), CancellationToken.None);

    await _store.DidNotReceiveWithAnyArgs().UpsertMessageAsync(default, default);
  }
}

public class PushDispatchServiceTests
{
  private readonly IPushOutbox _outbox = Substitute.For<IPushOutbox>();
  private readonly IMessageStore _store = Substitute.For<IMessageStore>();
  private readonly IPushProvider _push = Substitute.For<IPushProvider>();
  private readonly PushDispatchService _service;

  public PushDispatchServiceTests()
  {
    _service = new PushDispatchService(
      NullLogger<PushDispatchService>.Instance,
      _outbox,
      _store,
      _push,
      batchSize: 3);
  }

  private void SetupClaim(params HazardNotification[] batch)
    => _outbox.ClaimDuePushesAsync(3, Arg.Any<CancellationToken>()).Returns(batch);

  private Task ReceivedUpsert(string id, PushStatus status)
    => _store.Received(1).UpsertMessageAsync(
      Arg.Is<HazardNotification>(n => n.Id == id && n.PushStatus == status),
      Arg.Any<CancellationToken>());

  [Fact]
  public async Task DispatchPending_SentAndFailed_RecordsEachOutcome()
  {
    SetupClaim(
      TestData.Notification("e1", pushStatus: PushStatus.Pending),
      TestData.Notification("e2", pushStatus: PushStatus.Pending),
      TestData.Notification("e3", pushStatus: PushStatus.Failed));
    _push.BroadcastAsync(Arg.Is<HazardNotification>(n => n.Id == "e2"), Arg.Any<CancellationToken>())
      .Returns(Task.FromException(new InvalidOperationException("fcm down")));

    await _service.DispatchPendingAsync(CancellationToken.None);

    await ReceivedUpsert("e1", PushStatus.Delivered);
    await ReceivedUpsert("e2", PushStatus.Failed);
    await ReceivedUpsert("e3", PushStatus.Delivered);
    await _outbox.DidNotReceiveWithAnyArgs().ReleasePushesAsync(default!, default);
  }

  [Fact]
  public async Task DispatchPending_UpsertKeepsClaimedExtraData()
  {
    var claimed = TestData.Notification("e1", extra: "lease", pushStatus: PushStatus.Pending);
    SetupClaim(claimed);

    await _service.DispatchPendingAsync(CancellationToken.None);

    await _store.Received(1).UpsertMessageAsync(
      Arg.Is<HazardNotification>(n => n.ExtraData == claimed.ExtraData),
      Arg.Any<CancellationToken>());
  }

  [Fact]
  public async Task DispatchPending_CancelledMidBatch_ReleasesUnattempted()
  {
    using var cts = new CancellationTokenSource();
    SetupClaim(
      TestData.Notification("e1", pushStatus: PushStatus.Pending),
      TestData.Notification("e2", pushStatus: PushStatus.Pending),
      TestData.Notification("e3", pushStatus: PushStatus.Pending));
    _push.BroadcastAsync(Arg.Is<HazardNotification>(n => n.Id == "e1"), Arg.Any<CancellationToken>())
      .Returns(_ => { cts.Cancel(); return Task.CompletedTask; });

    await _service.DispatchPendingAsync(cts.Token);

    await ReceivedUpsert("e1", PushStatus.Delivered);
    await _push.DidNotReceive().BroadcastAsync(
      Arg.Is<HazardNotification>(n => n.Id != "e1"), Arg.Any<CancellationToken>());
    await _outbox.Received(1).ReleasePushesAsync(
      Arg.Is<IEnumerable<HazardNotification>>(items => items.Select(n => n.Id).SequenceEqual(new[] { "e2", "e3" })),
      Arg.Any<CancellationToken>());
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
