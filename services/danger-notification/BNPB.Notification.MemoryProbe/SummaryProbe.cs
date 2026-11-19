using System.Collections.Immutable;
using System.Data.Common;
using BNPB.Common.V1;
using BNPB.Event.V1;
using BNPB.Notification.Server.Adapters.Outbound.Persistence;
using BNPB.Notification.Server.Core.Models;
using Google.Protobuf.WellKnownTypes;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using Type = System.Type;
using PushStatus = BNPB.Notification.Server.Core.Models.PushStatus;

internal static class SummaryProbe
{
  public static async Task<object> Run(string adminString, OutboxOptions options, NpgsqlConnection admin,
    Dictionary<string, (int Length, string Hash)> original, int keys)
  {
    using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(45));
    var token = deadline.Token;
    var budget = new NotificationReadBudget();
    EfNotificationStore Store(Factory factory, View view = View.Summary) => new(
      NullLogger<EfNotificationStore>.Instance, factory, new PushOutboxSignal(), options, TimeProvider.System, budget, view);
    await Sql(admin, """
      CREATE ROLE notification_summary_fixture LOGIN PASSWORD 'm2-fixture-only';
      GRANT USAGE ON SCHEMA public TO notification_summary_fixture;
      GRANT SELECT ("Id", "HazardId", "Source", "HazardType", "Severity", "AreaName",
        "OccurredAt", "PublishedAt", "CreatedAt", "IsCurrent", "PushStatus", "Summary")
        ON notifications TO notification_summary_fixture;
      """);
    var restrictedInfo = new NpgsqlConnectionStringBuilder(adminString) { Username = "notification_summary_fixture", Password = "m2-fixture-only" };
    var restrictedFactory = new Factory(restrictedInfo.ConnectionString);
    await using (var db = restrictedFactory.CreateDbContext())
    {
      try
      {
        _ = await db.Notifications.Select(r => r.Hazard).FirstAsync(token);
        throw new InvalidOperationException("Hazard column access was not denied");
      }
      catch (PostgresException error) when (error.SqlState == PostgresErrorCodes.InsufficientPrivilege) { }
    }
    var warm = Store(restrictedFactory);
    await Task.WhenAll(Enumerable.Range(0, 24).Select(async i =>
    {
      Check((await warm.GetMessageAsync($"event-{i % original.Count}", token))!.Value);
      var (page, _) = await warm.GetMessageListAsync(null, null, null, null, null, 100, null, token);
      Require(page.Count == original.Count, "Warm summary page was incomplete");
      foreach (var message in page) Check(message);
    }));

    await Sql(admin, "UPDATE notifications SET \"Summary\" = NULL WHERE \"Id\" = 'event-0'");
    var coldMeter = new Reads { BlockBody = true };
    var cold = Store(new Factory(adminString, coldMeter));
    var pending = Enumerable.Range(0, 24).Select(_ => cold.GetMessageAsync("event-0", token)).ToArray();
    await coldMeter.BodyEntered.Task.WaitAsync(token);
    try
    {
      Check((await warm.GetMessageAsync("event-9", token))!.Value);
      Require(coldMeter.Bodies == 1, "Cold readers loaded concurrent full snapshots");
    }
    finally { coldMeter.ContinueBody.TrySetResult(); }
    foreach (var message in await Task.WhenAll(pending)) Check(message!.Value);
    Require(coldMeter.Bodies == 1, "Shared cold repair loaded a full body more than once");

    await Sql(admin, "UPDATE notifications SET \"Summary\" = NULL WHERE \"Id\" = 'event-1'");
    var cancelledMeter = new Reads { BlockBody = true };
    var cancelledStore = Store(new Factory(adminString, cancelledMeter));
    using var cancel = new CancellationTokenSource();
    var first = cancelledStore.GetMessageAsync("event-1", cancel.Token);
    await cancelledMeter.BodyEntered.Task.WaitAsync(token);
    cancel.Cancel();
    var waiters = Enumerable.Range(0, 12).Select(_ => cancelledStore.GetMessageAsync("event-1", token)).ToArray();
    try
    {
      Check((await warm.GetMessageAsync("event-9", token))!.Value);
      Require(cancelledMeter.Bodies == 1 && waiters.All(t => !t.IsCompleted), "Cancellation released an outstanding repair");
    }
    finally { cancelledMeter.ContinueBody.TrySetResult(); }
    try { _ = await first; throw new InvalidOperationException("Cancelled query succeeded"); }
    catch (OperationCanceledException) { }
    foreach (var message in await Task.WhenAll(waiters)) Check(message!.Value);
    Require(cancelledMeter.Bodies == 2, "Cancelled repair did not recover with one body load");

    // A full snapshot accidentally placed in the cache must be rejected in SQL
    // before transferring/parsing that oversized cache value.
    await Sql(admin, "UPDATE notifications SET \"Summary\" = \"Hazard\" WHERE \"Id\" = 'event-2'");
    var badCacheMeter = new Reads();
    var repair = Store(new Factory(adminString, badCacheMeter));
    Check((await repair.GetMessageAsync("event-2", token))!.Value);
    Require(badCacheMeter.Bodies == 1, "Oversized cache did not use bounded fallback");
    Check((await warm.GetMessageAsync("event-2", token))!.Value);

    await Sql(admin, "UPDATE notifications SET \"Summary\" = NULL WHERE \"Id\" = 'event-3'");
    var supersededMeter = new Reads { BlockWrite = true };
    var supersededStore = Store(new Factory(adminString, supersededMeter));
    var oldRead = supersededStore.GetMessageAsync("event-3", token);
    await supersededMeter.WriteEntered.Task.WaitAsync(token);
    try
    {
      var hazard = new HazardEvent
      {
        HazardId = "fire-3",
        Source = Source.Satelite,
        SourceRefId = "replacement",
        Severity = Severity.Awas,
        HazardType = BNPB.Common.V1.HazardType.Wildfire,
        AreaName = "memory fixture",
        Latitude = -6.1,
        Longitude = 106.1,
        OccurredAt = Timestamp.FromDateTimeOffset(DateTimeOffset.UtcNow.AddMinutes(1)),
        IngestedAt = Timestamp.FromDateTimeOffset(DateTimeOffset.UtcNow.AddMinutes(1)),
        Attributes = new Struct()
      };
      var envelope = new HazardEventPublished
      {
        EventId = "event-replacement",
        SchemaVersion = 1,
        Source = Source.Satelite,
        Publisher = "summary-probe",
        OccurredAt = hazard.IngestedAt,
        Payload = hazard
      };
      var message = new HazardNotification(envelope.EventId, HazardSource.Satellite,
        BNPB.Notification.Server.Core.Models.HazardType.Wildfire, HazardSeverity.Awas, hazard.AreaName,
        hazard.OccurredAt.ToDateTimeOffset(), "", PushStatus.Pending,
        ImmutableDictionary<Type, object>.Empty.Add(typeof(HazardEventPublished), envelope).Add(typeof(HazardEvent), hazard));
      await Store(new Factory(adminString), View.Full).UpsertMessageAsync(message, token);
      Check((await warm.GetMessageAsync(envelope.EventId, token))!.Value);
    }
    finally { supersededMeter.ContinueWrite.TrySetResult(); }
    Require(await oldRead == null, "Late repair exposed a superseded row");
    await using (var command = new NpgsqlCommand("SELECT \"Summary\" FROM notifications WHERE \"Id\" = 'event-3'", admin))
      Require(await command.ExecuteScalarAsync(token) is DBNull, "Late repair repopulated an outdated cache");

    await using (var command = new NpgsqlCommand("SELECT \"Id\", octet_length(\"Hazard\"), md5(\"Hazard\") FROM notifications", admin))
    await using (var reader = await command.ExecuteReaderAsync(token))
      while (await reader.ReadAsync(token))
        if (original.TryGetValue(reader.GetString(0), out var expected))
          Require(expected == (reader.GetInt32(1), reader.GetString(2)), "Summary repairs changed a full snapshot");
    var full = (await Store(new Factory(adminString), View.Full).GetMessageAsync("event-9", token))!.Value;
    var values = ((HazardEvent)full.ExtraData[typeof(HazardEvent)]).Attributes.Fields["detection_keys"].ListValue.Values;
    Require(values.Count == keys, "Full snapshot was truncated");
    for (var i = 0; i < keys; i++)
      Require(values[i].StringValue == FormattableString.Invariant($"N20|2026-11-19|0600|{-6.1 + i * 0.00000001:F8}|106.12345678"), "Detection key changed");
    return new
    {
      mode = "summary",
      records = original.Count,
      keys_per_record = keys,
      warm_concurrent_get_and_list_readers = 24,
      full_column_denied_for_warm_reads = true,
      cold_readers = 24,
      cold_body_reads = coldMeter.Bodies,
      cancelled_query_then_waiters = 12,
      cancelled_and_recovery_body_attempts = cancelledMeter.Bodies,
      oversized_cache_repaired = true,
      superseded_cache_not_repopulated = true,
      all_snapshot_hashes_unchanged = true,
      exact_full_keys_compared = keys,
      peak_rss = File.ReadAllLines("/proc/self/status").First(l => l.StartsWith("VmHWM:")),
      cgroup_peak_bytes = File.ReadAllText("/sys/fs/cgroup/memory.peak").Trim(),
      memory_events = File.ReadAllText("/sys/fs/cgroup/memory.events")
    };
  }

  private static void Check(HazardNotification message)
  {
    Require(!message.ExtraData.ContainsKey(typeof(HazardEvent)), "SUMMARY retained a full hazard");
    var summary = (HazardSummary)message.ExtraData[typeof(HazardSummary)];
    Require(summary.Source == Source.Satelite && summary.HazardType == BNPB.Common.V1.HazardType.Wildfire
      && summary.Severity == Severity.Awas && summary.OccurredAt != null && summary.IngestedAt != null,
      "SUMMARY lost canonical metadata");
  }
  private static async Task Sql(NpgsqlConnection connection, string sql)
  {
    await using var command = new NpgsqlCommand(sql, connection);
    await command.ExecuteNonQueryAsync();
  }
  private static void Require(bool condition, string message)
  {
    if (!condition) throw new InvalidOperationException(message);
  }

  private sealed class Reads : DbCommandInterceptor
  {
    private int _bodies;
    public int Bodies => Volatile.Read(ref _bodies);
    public bool BlockBody { get; init; }
    public bool BlockWrite { get; init; }
    public TaskCompletionSource BodyEntered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    public TaskCompletionSource ContinueBody { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    public TaskCompletionSource WriteEntered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    public TaskCompletionSource ContinueWrite { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    public override async ValueTask<InterceptionResult<DbDataReader>> ReaderExecutingAsync(DbCommand command,
      CommandEventData eventData, InterceptionResult<DbDataReader> result, CancellationToken cancellationToken = default)
    {
      if (command.CommandText.StartsWith("SELECT") && command.CommandText.Split('\n')[0].Contains("\"Hazard\""))
      {
        Interlocked.Increment(ref _bodies);
        BodyEntered.TrySetResult();
        if (BlockBody) await ContinueBody.Task;
      }
      return result;
    }
    public override async ValueTask<InterceptionResult<int>> NonQueryExecutingAsync(DbCommand command,
      CommandEventData eventData, InterceptionResult<int> result, CancellationToken cancellationToken = default)
    {
      if (BlockWrite && command.CommandText.Contains("SET \"Summary\" ="))
      {
        WriteEntered.TrySetResult();
        await ContinueWrite.Task;
      }
      return result;
    }
  }
}
