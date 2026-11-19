// Disposable integration probe, never a deployment entry point. The exact DB
// name and empty-table check protect against using an existing notification DB.
using System.Collections.Immutable;
using System.Globalization;
using System.Text.Json;
using BNPB.Common.V1;
using BNPB.Event.V1;
using BNPB.Notification.Server.Adapters.Outbound.Persistence;
using BNPB.Notification.Server.Core.Models;
using BNPB.Notification.Server.Core.Ports.Outbound;
using BNPB.Notification.Server.Core.Services;
using Google.Protobuf.WellKnownTypes;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using ProtoHazardType = BNPB.Common.V1.HazardType;
using PushStatus = BNPB.Notification.Server.Core.Models.PushStatus;
using Type = System.Type;

const int keys = 200000;
const int count = 10;
var adminString = Environment.GetEnvironmentVariable("NOTIFICATION_TEST_POSTGRES")
  ?? throw new InvalidOperationException("An empty disposable PostgreSQL fixture is required");
var connectionInfo = new NpgsqlConnectionStringBuilder(adminString);
Require(connectionInfo.Database == "notification_m2_fixture", "Refusing an unexpected database name");
var adminFactory = new Factory(adminString);
await using (var db = adminFactory.CreateDbContext())
{
  await db.Database.MigrateAsync();
  Require(!await db.Notifications.AnyAsync(), "Refusing a nonempty notification table");
}
var options = new OutboxOptions();
var adminStore = Store(adminFactory, options);
await Seed(adminStore);
await using var admin = new NpgsqlConnection(adminString);
await admin.OpenAsync();
var before = await Hashes(admin);
Require(before.Count == count && before.Values.All(v => v.Length > 9000000), "Large snapshots were not stored");

if (Environment.GetEnvironmentVariable("NOTIFICATION_TEST_MODE") == "summary")
{
  Console.WriteLine(JsonSerializer.Serialize(await SummaryProbe.Run(adminString, options, admin, before, keys)));
  return;
}

// A column ACL makes an accidental full SELECT/RETURNING fail independently
// of parser behavior, process allocation counters or query string assertions.
await Sql(admin, """
  CREATE ROLE notification_metadata_fixture LOGIN PASSWORD 'm2-fixture-only';
  GRANT USAGE ON SCHEMA public TO notification_metadata_fixture;
  GRANT SELECT ("Id", "HazardId", "Source", "HazardType", "Severity", "AreaName",
    "OccurredAt", "PublishedAt", "CreatedAt", "IsCurrent", "PushStatus",
    "FailedPushAttempts", "NextPushAttemptAt", "LeaseToken", "LeaseExpiresAt",
    "TraceParent", xmin) ON notifications TO notification_metadata_fixture;
  GRANT UPDATE ("PushStatus", "FailedPushAttempts", "NextPushAttemptAt",
    "LeaseToken", "LeaseExpiresAt") ON notifications TO notification_metadata_fixture;
  """);
connectionInfo.Username = "notification_metadata_fixture";
connectionInfo.Password = "m2-fixture-only";
var restrictedFactory = new Factory(connectionInfo.ConnectionString);
await using (var db = restrictedFactory.CreateDbContext())
{
  try
  {
    _ = await db.Notifications.Select(r => r.Hazard).FirstAsync();
    throw new InvalidOperationException("Blob column ACL did not deny access");
  }
  catch (PostgresException error) when (error.SqlState == PostgresErrorCodes.InsufficientPrivilege) { }
}
var store = Store(restrictedFactory, options);
var batches = await Task.WhenAll(Enumerable.Range(0, 4).Select(_ => store.ClaimDuePushesAsync(3, CancellationToken.None)));
var claims = batches.SelectMany(batch => batch).ToList();
Require(claims.Count == count && claims.Select(c => c.Id).Distinct().Count() == count,
  "Concurrent SKIP LOCKED claims lost or duplicated rows");
Require(claims.All(c => !c.ExtraData.ContainsKey(typeof(HazardEvent)) && !c.ExtraData.ContainsKey(typeof(HazardSummary))),
  "Claims retained hazard projections");

var stale = claims[0];
await using (var command = new NpgsqlCommand("UPDATE notifications SET \"LeaseExpiresAt\" = 0 WHERE \"Id\" = $1", admin))
{
  command.Parameters.AddWithValue(stale.Id);
  await command.ExecuteNonQueryAsync();
}
var current = (await store.ClaimDuePushesAsync(1, CancellationToken.None)).Single();
Require(current.Id == stale.Id, "Expired claim was not reclaimed");
await store.UpsertMessageAsync(stale with { PushStatus = PushStatus.Delivered }, CancellationToken.None);
await using (var command = new NpgsqlCommand("SELECT \"PushStatus\", \"LeaseToken\" FROM notifications WHERE \"Id\" = $1", admin))
{
  command.Parameters.AddWithValue(stale.Id);
  await using var reader = await command.ExecuteReaderAsync();
  Require(await reader.ReadAsync() && reader.GetInt32(0) == 1
    && reader.GetGuid(1) == ((PushLease)current.ExtraData[typeof(PushLease)]).Token,
    "Stale outcome changed a reclaimed row");
}
await store.ReleasePushesAsync(claims, CancellationToken.None);
await store.ReleasePushesAsync([current], CancellationToken.None);

var push = new SlowPush();
var dispatcher = new PushDispatchService(NullLogger<PushDispatchService>.Instance, store, store, push, count);
using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
var dispatch = dispatcher.DispatchPendingAsync(timeout.Token);
await push.Entered.Task.WaitAsync(timeout.Token);
try
{
  Require((await store.ClaimDuePushesAsync(count, timeout.Token)).Count == 0, "Slow push lost its batch lease");
  // Broker redeliveries can overlap an in-progress push. They must neither
  // hydrate the immutable snapshot nor replace its live lease/status.
  foreach (var claim in claims)
    await store.UpsertMessageAsync(claim with { ExtraData = ImmutableDictionary<Type, object>.Empty }, timeout.Token);
}
finally { push.Release.TrySetResult(); }
await dispatch;
Require(push.Sent == count && (await store.ClaimDuePushesAsync(count, timeout.Token)).Count == 0, "Completed pushes were repeated");
foreach (var claim in claims)
  await store.UpsertMessageAsync(claim with { PushStatus = PushStatus.Pending, ExtraData = ImmutableDictionary<Type, object>.Empty }, timeout.Token);
Require((await store.ClaimDuePushesAsync(count, timeout.Token)).Count == 0, "Redelivery regressed delivered status");
var after = await Hashes(admin);
Require(before.OrderBy(k => k.Key).SequenceEqual(after.OrderBy(k => k.Key)), "Status writes changed an immutable snapshot");
await using (var command = new NpgsqlCommand("SELECT count(*) FROM notifications WHERE \"Summary\" IS NOT NULL", admin))
  Require(Convert.ToInt64(await command.ExecuteScalarAsync()) == count, "Metadata outcome writes cleared summary caches");
await CheckFull(adminStore, "event-9");

var rss = File.ReadAllLines("/proc/self/status").First(line => line.StartsWith("VmHWM:"));
Console.WriteLine(JsonSerializer.Serialize(new
{
  records = count,
  keys_per_record = keys,
  concurrent_claimers = 4,
  blocked_push_batch = count,
  snapshot_column_select_denied = true,
  stale_claim_rejected = true,
  snapshots_unchanged = true,
  completed_pushes = push.Sent,
  exact_full_keys_compared = keys,
  summary_caches_preserved = true,
  peak_rss = rss,
  cgroup_peak_bytes = File.ReadAllText("/sys/fs/cgroup/memory.peak").Trim(),
  memory_events = File.ReadAllText("/sys/fs/cgroup/memory.events")
}));

static EfNotificationStore Store(Factory factory, OutboxOptions options) => new(
  NullLogger<EfNotificationStore>.Instance, factory, new PushOutboxSignal(), options, TimeProvider.System, new NotificationReadBudget(), View.Full);

static async Task Seed(EfNotificationStore store)
{
  var hazard = new HazardEvent
  {
    Source = Source.Satelite,
    SourceRefId = "test-reference",
    HazardType = ProtoHazardType.Wildfire,
    Severity = Severity.Awas,
    AreaName = "memory fixture",
    Latitude = -6.1,
    Longitude = 106.1,
    OccurredAt = Timestamp.FromDateTimeOffset(DateTimeOffset.UtcNow),
    IngestedAt = Timestamp.FromDateTimeOffset(DateTimeOffset.UtcNow),
    Attributes = new Struct()
  };
  var detections = new ListValue();
  for (var i = 0; i < keys; i++) detections.Values.Add(Value.ForString(Key(i)));
  hazard.Attributes.Fields["detection_keys"] = Value.ForList(detections.Values.ToArray());
  for (var i = 0; i < count; i++)
  {
    hazard.HazardId = $"fire-{i}";
    var envelope = new HazardEventPublished
    {
      EventId = $"event-{i}",
      SchemaVersion = 1,
      Source = Source.Satelite,
      Publisher = "memory-probe",
      OccurredAt = hazard.IngestedAt,
      Payload = hazard
    };
    var message = new HazardNotification(envelope.EventId, HazardSource.Satellite,
      BNPB.Notification.Server.Core.Models.HazardType.Wildfire, HazardSeverity.Awas, hazard.AreaName,
      hazard.OccurredAt.ToDateTimeOffset(), "", PushStatus.Pending,
      ImmutableDictionary<Type, object>.Empty.Add(typeof(HazardEventPublished), envelope).Add(typeof(HazardEvent), hazard));
    await store.UpsertMessageAsync(message, CancellationToken.None);
  }
}

static string Key(int i) => string.Create(CultureInfo.InvariantCulture,
  $"N20|2026-11-19|0600|{-6.1 + i * 0.00000001:F8}|106.12345678");

static async Task CheckFull(EfNotificationStore store, string id)
{
  var notification = (await store.GetMessageAsync(id, CancellationToken.None))!.Value;
  var full = (HazardEvent)notification.ExtraData[typeof(HazardEvent)];
  var values = full.Attributes.Fields["detection_keys"].ListValue.Values;
  Require(values.Count == keys, "Full stored snapshot was truncated");
  for (var i = 0; i < keys; i++) Require(values[i].StringValue == Key(i), $"Detection key {i} changed");
}

static async Task<Dictionary<string, (int Length, string Hash)>> Hashes(NpgsqlConnection connection)
{
  await using var command = new NpgsqlCommand("SELECT \"Id\", octet_length(\"Hazard\"), md5(\"Hazard\") FROM notifications", connection);
  await using var reader = await command.ExecuteReaderAsync();
  var hashes = new Dictionary<string, (int, string)>();
  while (await reader.ReadAsync()) hashes.Add(reader.GetString(0), (reader.GetInt32(1), reader.GetString(2)));
  return hashes;
}

static async Task Sql(NpgsqlConnection connection, string sql)
{
  await using var command = new NpgsqlCommand(sql, connection);
  await command.ExecuteNonQueryAsync();
}

static void Require(bool condition, string failure)
{
  if (!condition) throw new InvalidOperationException(failure);
}

sealed class Factory(string connection, DbCommandInterceptor? interceptor = null) : IDbContextFactory<NotificationDbContext>
{
  public NotificationDbContext CreateDbContext()
  {
    var options = new DbContextOptionsBuilder<PostgresNotificationDbContext>().UseNpgsql(connection);
    if (interceptor != null) options.AddInterceptors(interceptor);
    return new PostgresNotificationDbContext(options.Options);
  }
}

sealed class SlowPush : IPushProvider
{
  public TaskCompletionSource Entered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
  public TaskCompletionSource Release { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
  public int Sent { get; private set; }
  public Task SubscribeAsync(string token, CancellationToken cancellationToken) => Task.CompletedTask;
  public Task UnsubscribeAsync(string token, CancellationToken cancellationToken) => Task.CompletedTask;
  public async Task BroadcastAsync(HazardNotification message, CancellationToken cancellationToken)
  {
    if (message.ExtraData.ContainsKey(typeof(HazardEvent))) throw new InvalidOperationException("Push retained a full hazard");
    Entered.TrySetResult();
    await Release.Task.WaitAsync(cancellationToken);
    Sent++;
  }
}
