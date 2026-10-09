using System.Diagnostics;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging.Abstractions;
using BNPB.Common.V1;
using BNPB.Notification.Server.Adapters.Exceptions;
using BNPB.Notification.Server.Adapters.Models;
using BNPB.Notification.Server.Adapters.Outbound.Persistence;
using BNPB.Notification.Server.Core.Models;

using EHazardEvent = BNPB.Common.V1.HazardEvent;
using EPushStatus = BNPB.Notification.V1.DangerNotification.Types.PushStatus;
using PushStatus = BNPB.Notification.Server.Core.Models.PushStatus;

namespace BNPB.Notification.Server.Tests.Adapters;

public class EfNotificationStoreTests : IAsyncLifetime
{
  private static readonly OutboxOptions Options = new()
  {
    MaxAttempts = 3,
    BaseBackoff = TimeSpan.FromSeconds(10),
    LeaseDuration = TimeSpan.FromSeconds(60),
  };

  // A named shared-cache in-memory database lives as long as one connection stays open.
  private readonly string _connectionString = $"Data Source=store-{Guid.NewGuid():N};Mode=Memory;Cache=Shared";
  private readonly SqliteConnection _keepAlive;
  private readonly IDbContextFactory<NotificationDbContext> _contextFactory;
  private readonly ManualTimeProvider _time = new();
  private readonly PushOutboxSignal _signal = new();
  private readonly EfNotificationStore _full;
  private readonly EfNotificationStore _summary;

  public EfNotificationStoreTests()
  {
    _keepAlive = new SqliteConnection(_connectionString);
    _contextFactory = new ContextFactory(_connectionString);
    _full = Store(View.Full);
    _summary = Store(View.Summary);
  }

  public async Task InitializeAsync()
  {
    await _keepAlive.OpenAsync();
    await using var db = await _contextFactory.CreateDbContextAsync();
    await db.Database.MigrateAsync();
  }

  public async Task DisposeAsync() => await _keepAlive.DisposeAsync();

  private EfNotificationStore Store(View view)
    => new(NullLogger<EfNotificationStore>.Instance, _contextFactory, _signal, Options, _time, view);

  private async Task<NotificationRecord> Record(string id)
  {
    await using var db = await _contextFactory.CreateDbContextAsync();
    return await db.Notifications.AsNoTracking().SingleAsync(r => r.Id == id);
  }

  private async Task Ingest(HazardNotification notification)
  {
    await _full.UpsertMessageAsync(notification with { PushStatus = PushStatus.Pending }, CancellationToken.None);
    _time.Advance(TimeSpan.FromSeconds(1));
  }

  private Task<IReadOnlyList<HazardNotification>> Claim(int limit = 10)
    => _full.ClaimDuePushesAsync(limit, CancellationToken.None);

  private Task Report(HazardNotification claimed, PushStatus status)
    => _full.UpsertMessageAsync(claimed with { PushStatus = status }, CancellationToken.None);

  [Fact]
  public async Task Upsert_NewRevision_IsPendingAndClaimable()
  {
    await Ingest(TestData.Published("e1", "h1"));

    var claimed = Assert.Single(await Claim());

    Assert.Equal("e1", claimed.Id);
    Assert.Equal(PushStatus.Pending, claimed.PushStatus);
    Assert.True(claimed.ExtraData.ContainsKey(typeof(PushLease)));
    Assert.True(claimed.ExtraData.ContainsKey(typeof(EHazardEvent)));
  }

  [Fact]
  public async Task Upsert_RedeliveredAfterDelivery_DoesNotRegress()
  {
    var published = TestData.Published("e1", "h1");
    await Ingest(published);
    await Report(Assert.Single(await Claim()), PushStatus.Delivered);

    await Ingest(published);

    Assert.Equal(EPushStatus.Delivered, (await Record("e1")).PushStatus);
    Assert.Empty(await Claim());
  }

  [Fact]
  public async Task Upsert_MissingEnvelope_Throws()
  {
    await Assert.ThrowsAsync<InvalidOperationException>(
      () => _full.UpsertMessageAsync(TestData.Notification("e1"), CancellationToken.None));
  }

  [Theory]
  [InlineData(null, EPushStatus.Unspecified)]
  [InlineData(PushStatus.Failed, EPushStatus.Unspecified)]
  [InlineData(PushStatus.Delivered, EPushStatus.Delivered)]
  public async Task Upsert_NewerRevision_OutdatesOlder(PushStatus? olderOutcome, EPushStatus expected)
  {
    await Ingest(TestData.Published("e1", "h1", Severity.Siaga, publishedMinutes: 0));
    if (olderOutcome is { } outcome)
      await Report(Assert.Single(await Claim()), outcome);

    await Ingest(TestData.Published("e2", "h1", Severity.Awas, publishedMinutes: 1));

    var older = await Record("e1");
    Assert.False(older.IsCurrent);
    Assert.Equal(expected, older.PushStatus);
    Assert.Null(await _full.GetMessageAsync("e1", CancellationToken.None));

    var (items, _) = await _full.GetMessageListAsync(null, null, null, null, null, 10, null, CancellationToken.None);
    Assert.Equal("e2", Assert.Single(items).Id);

    // Only the newer revision is pushed; a Failed older one is not retried.
    _time.Advance(TimeSpan.FromMinutes(10));
    Assert.Equal("e2", Assert.Single(await Claim()).Id);
  }

  [Fact]
  public async Task Upsert_OlderRevisionArrivingLate_IsStoredOutdated()
  {
    await Ingest(TestData.Published("e2", "h1", publishedMinutes: 1));
    await Ingest(TestData.Published("e1", "h1", publishedMinutes: 0));

    var late = await Record("e1");
    Assert.False(late.IsCurrent);
    Assert.Equal(EPushStatus.Unspecified, late.PushStatus);

    var current = await Record("e2");
    Assert.True(current.IsCurrent);
    Assert.Equal(EPushStatus.Pending, current.PushStatus);
  }

  [Fact]
  public async Task Upsert_OutdatedWhileSending_StillRecordsDelivery()
  {
    await Ingest(TestData.Published("e1", "h1", publishedMinutes: 0));
    var claimed = Assert.Single(await Claim());
    await Ingest(TestData.Published("e2", "h1", publishedMinutes: 1));

    await Report(claimed, PushStatus.Delivered);

    Assert.Equal(EPushStatus.Delivered, (await Record("e1")).PushStatus);
  }

  [Fact]
  public async Task Upsert_RevisionsOfOtherHazards_AreIndependent()
  {
    await Ingest(TestData.Published("e1", "h1"));
    await Ingest(TestData.Published("e2", "h2", publishedMinutes: 1));

    Assert.True((await Record("e1")).IsCurrent);
    Assert.True((await Record("e2")).IsCurrent);
  }

  [Fact]
  public async Task Claim_ConcurrentClaimers_GetDisjointRows()
  {
    await Ingest(TestData.Published("e1", "h1"));
    await Ingest(TestData.Published("e2", "h2"));
    await Ingest(TestData.Published("e3", "h3"));

    var first = await Claim(limit: 2);
    var second = await Claim(limit: 2);

    Assert.Equal(["e1", "e2"], first.Select(n => n.Id));
    Assert.Equal("e3", Assert.Single(second).Id);
    Assert.Empty(await Claim());
  }

  [Fact]
  public async Task Claim_LapsedLease_IsReclaimedAndStaleUpsertIsSkipped()
  {
    await Ingest(TestData.Published("e1", "h1"));
    var stale = Assert.Single(await Claim());

    _time.Advance(Options.LeaseDuration + TimeSpan.FromSeconds(1));
    var reclaimed = Assert.Single(await Claim());

    await Report(stale, PushStatus.Delivered);
    var record = await Record("e1");
    Assert.Equal(EPushStatus.Pending, record.PushStatus);
    Assert.Equal(((PushLease)reclaimed.ExtraData[typeof(PushLease)]).Token, record.LeaseToken);

    await Report(reclaimed, PushStatus.Delivered);
    record = await Record("e1");
    Assert.Equal(EPushStatus.Delivered, record.PushStatus);
    Assert.Null(record.LeaseToken);
  }

  [Fact]
  public async Task Release_MakesRowsClaimableAtOnce()
  {
    await Ingest(TestData.Published("e1", "h1"));
    var claimed = await Claim();

    await _full.ReleasePushesAsync(claimed, CancellationToken.None);

    Assert.Equal("e1", Assert.Single(await Claim()).Id);
  }

  [Fact]
  public async Task Failed_IsRetriedAfterBackoffUntilMaxAttempts()
  {
    await Ingest(TestData.Published("e1", "h1"));

    for (int attempt = 1; attempt <= Options.MaxAttempts; attempt++)
    {
      await Report(Assert.Single(await Claim()), PushStatus.Failed);
      Assert.Empty(await Claim());

      _time.Advance(TimeSpan.FromSeconds(10 * Math.Pow(2, attempt - 1)));
    }

    _time.Advance(TimeSpan.FromHours(1));
    Assert.Empty(await Claim());

    var record = await Record("e1");
    Assert.Equal(EPushStatus.Failed, record.PushStatus);
    Assert.Equal(Options.MaxAttempts, record.FailedPushAttempts);
  }

  [Fact]
  public async Task Claim_RestoresIngestTraceContext()
  {
    using var activity = new Activity("consume").SetIdFormat(ActivityIdFormat.W3C).Start();
    await Ingest(TestData.Published("e1", "h1"));
    activity.Stop();

    var claimed = Assert.Single(await Claim());

    var parent = (ActivityContext)claimed.ExtraData[typeof(ActivityContext)];
    Assert.Equal(activity.TraceId, parent.TraceId);
    Assert.Equal(activity.SpanId, parent.SpanId);
  }

  [Fact]
  public async Task List_FiltersAndOrdersNewestFirst()
  {
    await Ingest(TestData.Published("e1", "h1", Severity.Awas, Source.Bmkg, occurredMinutes: 0));
    await Ingest(TestData.Published("e2", "h2", Severity.Siaga, Source.Bmkg, occurredMinutes: 10));
    await Ingest(TestData.Published("e3", "h3", Severity.Awas, Source.Pvmbg, occurredMinutes: 20));

    async Task<IEnumerable<string>> Ids(
      HazardSource? source = null,
      HazardSeverity? severity = null,
      DateTimeOffset? since = null,
      DateTimeOffset? until = null)
    {
      var (items, _) = await _full.GetMessageListAsync(
        source, null, severity, since, until, 10, null, CancellationToken.None);
      return items.Select(n => n.Id);
    }

    Assert.Equal(["e3", "e2", "e1"], await Ids());
    Assert.Equal(["e2", "e1"], await Ids(source: HazardSource.BMKG));
    Assert.Equal(["e3", "e1"], await Ids(severity: HazardSeverity.Awas));
    Assert.Empty(await Ids(severity: HazardSeverity.Other));
    Assert.Equal(["e2"], await Ids(
      since: TestData.BaseTime.AddMinutes(10),
      until: TestData.BaseTime.AddMinutes(20)));
  }

  [Fact]
  public async Task List_PagesWithKeysetCursorAndExcludesNewerRows()
  {
    for (int i = 1; i <= 5; i++)
      await Ingest(TestData.Published($"e{i}", $"h{i}"));

    var (page1, token1) = await _full.GetMessageListAsync(null, null, null, null, null, 2, null, CancellationToken.None);
    await Ingest(TestData.Published("e6", "h6"));
    var (page2, token2) = await _full.GetMessageListAsync(null, null, null, null, null, 2, token1, CancellationToken.None);
    var (page3, token3) = await _full.GetMessageListAsync(null, null, null, null, null, 2, token2, CancellationToken.None);

    Assert.Equal(["e5", "e4"], page1.Select(n => n.Id));
    Assert.Equal(["e3", "e2"], page2.Select(n => n.Id));
    Assert.Equal(["e1"], page3.Select(n => n.Id));
    Assert.Equal("", token3);
  }

  [Theory]
  [InlineData("not base64!")]
  [InlineData("bm9wZQ")]
  public async Task List_MalformedCursor_IsBadInput(string token)
  {
    await Assert.ThrowsAsync<BadInputException>(() =>
      _full.GetMessageListAsync(null, null, null, null, null, 2, token, CancellationToken.None));
  }

  [Fact]
  public async Task List_CursorFromDifferentRequest_IsBadInput()
  {
    for (int i = 1; i <= 3; i++)
      await Ingest(TestData.Published($"e{i}", $"h{i}"));

    var (_, token) = await _full.GetMessageListAsync(null, null, null, null, null, 2, null, CancellationToken.None);

    await Assert.ThrowsAsync<BadInputException>(() =>
      _full.GetMessageListAsync(null, null, null, null, null, 3, token, CancellationToken.None));
    await Assert.ThrowsAsync<BadInputException>(() =>
      _summary.GetMessageListAsync(null, null, null, null, null, 2, token, CancellationToken.None));
  }

  [Fact]
  public async Task Get_ProjectsByView()
  {
    await Ingest(TestData.Published("e1", "h1"));

    var full = (await _full.GetMessageAsync("e1", CancellationToken.None))!.Value;
    var summary = (await _summary.GetMessageAsync("e1", CancellationToken.None))!.Value;

    var hazard = (EHazardEvent)full.ExtraData[typeof(EHazardEvent)];
    Assert.Equal("ref-secret-h1", hazard.SourceRefId);
    Assert.False(full.ExtraData.ContainsKey(typeof(HazardSummary)));

    Assert.False(summary.ExtraData.ContainsKey(typeof(EHazardEvent)));
    var projection = (HazardSummary)summary.ExtraData[typeof(HazardSummary)];
    Assert.Equal("h1", projection.HazardId);
    Assert.DoesNotContain("ref-secret", projection.ToString());

    Assert.Equal(PushStatus.Pending, summary.PushStatus);
    Assert.Equal(HazardSeverity.Awas, summary.Severity);
    Assert.True(summary.ExtraData.ContainsKey(typeof(NotificationCreatedAt)));
  }

  private sealed class ContextFactory(string connectionString) : IDbContextFactory<NotificationDbContext>
  {
    public NotificationDbContext CreateDbContext()
      => new(new DbContextOptionsBuilder<NotificationDbContext>()
        .UseSqlite(connectionString)
        .Options);
  }

  private sealed class ManualTimeProvider : TimeProvider
  {
    private DateTimeOffset _now = TestData.BaseTime.AddDays(1);

    public override DateTimeOffset GetUtcNow() => _now;

    public void Advance(TimeSpan by) => _now += by;
  }
}
