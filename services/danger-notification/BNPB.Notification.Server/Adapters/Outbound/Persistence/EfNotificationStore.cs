using System.Diagnostics;
using System.Security.Cryptography;
using System.Text;
using BNPB.Common.V1;
using BNPB.Event.V1;
using Google.Protobuf;
using Microsoft.EntityFrameworkCore;
using BNPB.Notification.Server.Adapters.Exceptions;
using BNPB.Notification.Server.Adapters.Models;
using BNPB.Notification.Server.Core.Models;
using BNPB.Notification.Server.Core.Ports.Outbound;

using EHazardType = BNPB.Common.V1.HazardType;
using HazardType = BNPB.Notification.Server.Core.Models.HazardType;
using EHazardEvent = BNPB.Common.V1.HazardEvent;
using EPushStatus = BNPB.Notification.V1.DangerNotification.Types.PushStatus;
using PushStatus = BNPB.Notification.Server.Core.Models.PushStatus;

namespace BNPB.Notification.Server.Adapters.Outbound.Persistence;

public class EfNotificationStore : IMessageStore, IPushOutbox, IMessageProvider, IMessageListProvider
{
  private const char CursorDelimiter = '.';

  private readonly ILogger<EfNotificationStore> _logger;
  private readonly IDbContextFactory<NotificationDbContext> _contextFactory;
  private readonly PushOutboxSignal _signal;
  private readonly OutboxOptions _options;
  private readonly TimeProvider _timeProvider;
  private readonly View _view;

  public EfNotificationStore(
    ILogger<EfNotificationStore> logger,
    IDbContextFactory<NotificationDbContext> contextFactory,
    PushOutboxSignal signal,
    OutboxOptions options,
    TimeProvider timeProvider,
    View view = View.Summary)
  {
    _logger = logger;
    _contextFactory = contextFactory;
    _signal = signal;
    _options = options;
    _timeProvider = timeProvider;
    _view = view;
  }

  private long Now => _timeProvider.GetUtcNow().ToUnixTimeMilliseconds();

  public async Task UpsertMessageAsync(
    HazardNotification message,
    CancellationToken cancellationToken)
  {
    await using var db = await _contextFactory.CreateDbContextAsync(cancellationToken);
    // Microsoft.Data.Sqlite begins IMMEDIATE transactions, so writers are serialized.
    await using var transaction = await db.Database.BeginTransactionAsync(cancellationToken);

    var record = await db.Notifications.FindAsync([message.Id], cancellationToken);

    if (record == null)
      record = await InsertAsync(db, message, cancellationToken);
    else if (!Merge(record, message))
      return;

    await db.SaveChangesAsync(cancellationToken);
    await transaction.CommitAsync(cancellationToken);

    if (record.IsCurrent && record.PushStatus == EPushStatus.Pending)
      _signal.Notify();
  }

  public async Task<IReadOnlyList<HazardNotification>> ClaimDuePushesAsync(
    int limit,
    CancellationToken cancellationToken)
  {
    var now = Now;
    var token = Guid.NewGuid();
    var leaseExpiresAt = now + (long)_options.LeaseDuration.TotalMilliseconds;
    var maxAttempts = _options.MaxAttempts;

    await using var db = await _contextFactory.CreateDbContextAsync(cancellationToken);

    await using (var transaction = await db.Database.BeginTransactionAsync(cancellationToken))
    {
      var ids = await db.Notifications
        .Where(r => r.IsCurrent
          && (r.LeaseExpiresAt == null || r.LeaseExpiresAt <= now)
          && (r.PushStatus == EPushStatus.Pending
            || (r.PushStatus == EPushStatus.Failed
              && r.FailedPushAttempts < maxAttempts
              && r.NextPushAttemptAt <= now)))
        .OrderBy(r => r.CreatedAt)
        .ThenBy(r => r.Id)
        .Select(r => r.Id)
        .Take(limit)
        .ToListAsync(cancellationToken);

      if (ids.Count == 0) return [];

      await db.Notifications
        .Where(r => ids.Contains(r.Id))
        .ExecuteUpdateAsync(setters => setters
          .SetProperty(r => r.LeaseToken, token)
          .SetProperty(r => r.LeaseExpiresAt, leaseExpiresAt),
          cancellationToken);

      await transaction.CommitAsync(cancellationToken);

      // A full batch may leave more due; don't wait for the next poll.
      if (ids.Count == limit) _signal.Notify();
    }

    var records = await db.Notifications
      .AsNoTracking()
      .Where(r => r.LeaseToken == token)
      .OrderBy(r => r.CreatedAt)
      .ThenBy(r => r.Id)
      .ToListAsync(cancellationToken);

    return records.Select(record => ToClaimedNotification(record, token)).ToList();
  }

  public async Task ReleasePushesAsync(
    IEnumerable<HazardNotification> messages,
    CancellationToken cancellationToken)
  {
    var byLease = messages
      .Select(message => (message.Id, Lease: GetLease(message)))
      .Where(entry => entry.Lease != null)
      .GroupBy(entry => entry.Lease!.Token, entry => entry.Id)
      .ToList();

    if (byLease.Count == 0) return;

    await using var db = await _contextFactory.CreateDbContextAsync(cancellationToken);

    foreach (var group in byLease)
    {
      var token = group.Key;
      var ids = group.ToList();

      await db.Notifications
        .Where(r => ids.Contains(r.Id) && r.LeaseToken == token)
        .ExecuteUpdateAsync(setters => setters
          .SetProperty(r => r.LeaseToken, (Guid?)null)
          .SetProperty(r => r.LeaseExpiresAt, (long?)null),
          cancellationToken);
    }

    _signal.Notify();
  }

  public async Task<HazardNotification?> GetMessageAsync(
    string messageId,
    CancellationToken cancellationToken)
  {
    await using var db = await _contextFactory.CreateDbContextAsync(cancellationToken);

    var record = await db.Notifications
      .AsNoTracking()
      .FirstOrDefaultAsync(r => r.Id == messageId && r.IsCurrent, cancellationToken);

    return record == null ? null : ToNotification(record, _view);
  }

  public async Task<(IList<HazardNotification>, string)> GetMessageListAsync(
    HazardSource? source,
    HazardType? type,
    HazardSeverity? severity,
    DateTimeOffset? since,
    DateTimeOffset? until,
    int pageSize,
    string? pageToken,
    CancellationToken cancellationToken)
  {
    // Only SIAGA/AWAS are ever stored.
    if (severity == HazardSeverity.Other) return ([], "");

    var fingerprint = Fingerprint(source, type, severity, since, until, pageSize);
    (long, string)? cursor = string.IsNullOrEmpty(pageToken) ? null : ParseCursor(pageToken, fingerprint);

    await using var db = await _contextFactory.CreateDbContextAsync(cancellationToken);

    var query = db.Notifications.AsNoTracking().Where(r => r.IsCurrent);

    if (source.HasValue)
    {
      var value = ToProtoSource(source.Value);
      query = query.Where(r => r.Source == value);
    }

    if (type.HasValue)
    {
      var value = ToProtoType(type.Value);
      query = query.Where(r => r.HazardType == value);
    }

    if (severity.HasValue)
    {
      var value = ToProtoSeverity(severity.Value);
      query = query.Where(r => r.Severity == value);
    }

    if (since.HasValue)
    {
      var value = since.Value.ToUnixTimeMilliseconds();
      query = query.Where(r => r.OccurredAt >= value);
    }

    if (until.HasValue)
    {
      var value = until.Value.ToUnixTimeMilliseconds();
      query = query.Where(r => r.OccurredAt < value);
    }

    if (cursor is var (createdAt, id))
    {
      query = query.Where(r => r.CreatedAt < createdAt
        || (r.CreatedAt == createdAt && string.Compare(r.Id, id) > 0));
    }

    var records = await query
      .OrderByDescending(r => r.CreatedAt)
      .ThenBy(r => r.Id)
      .Take(pageSize + 1)
      .ToListAsync(cancellationToken);

    var nextPageToken = "";

    if (records.Count > pageSize)
    {
      records.RemoveAt(pageSize);
      var last = records[^1];
      nextPageToken = FormatCursor(last.CreatedAt, last.Id, fingerprint);
    }

    return (records.Select(record => ToNotification(record, _view)).ToList(), nextPageToken);
  }

  private async Task<NotificationRecord> InsertAsync(
    NotificationDbContext db,
    HazardNotification message,
    CancellationToken cancellationToken)
  {
    if (!message.ExtraData.TryGetValue(typeof(HazardEventPublished), out var value))
      throw new InvalidOperationException($"Notification {message.Id} carries no publication envelope");

    var envelope = (HazardEventPublished)value;
    var hazard = message.ExtraData.TryGetValue(typeof(EHazardEvent), out var full)
      ? (EHazardEvent)full
      : envelope.Payload;
    var hazardId = hazard.HazardId;
    var publishedAt = envelope.OccurredAt.ToDateTimeOffset().ToUnixTimeMilliseconds();
    var id = message.Id;

    var outdated = await db.Notifications.AnyAsync(r =>
      r.HazardId == hazardId
      && r.IsCurrent
      && (r.PublishedAt > publishedAt
        || (r.PublishedAt == publishedAt && string.Compare(r.Id, id) > 0)),
      cancellationToken);

    if (outdated)
    {
      _logger.LogInformation(
        "Notification {NotificationId} is older than the stored revision of hazard {HazardId}",
        id, hazardId);
    }
    else
    {
      await db.Notifications
        .Where(r => r.HazardId == hazardId && r.IsCurrent)
        .ExecuteUpdateAsync(setters => setters
          .SetProperty(r => r.IsCurrent, false)
          .SetProperty(r => r.PushStatus, r => r.PushStatus == EPushStatus.Delivered
            ? EPushStatus.Delivered
            : EPushStatus.Unspecified),
          cancellationToken);
    }

    var record = new NotificationRecord
    {
      Id = id,
      HazardId = hazardId,
      Source = hazard.Source,
      HazardType = hazard.HazardType,
      Severity = hazard.Severity,
      AreaName = hazard.AreaName,
      OccurredAt = hazard.OccurredAt.ToDateTimeOffset().ToUnixTimeMilliseconds(),
      PublishedAt = publishedAt,
      CreatedAt = Now,
      Hazard = hazard.ToByteArray(),
      IsCurrent = !outdated,
      PushStatus = outdated ? EPushStatus.Unspecified : ToProtoPushStatus(message.PushStatus),
      TraceParent = Activity.Current?.Id,
    };

    db.Notifications.Add(record);

    return record;
  }

  // Applies the push status of an existing notification. The hazard snapshot is
  // immutable. Returns false when there is nothing to write.
  private bool Merge(NotificationRecord record, HazardNotification message)
  {
    var lease = GetLease(message);

    if (lease != null)
    {
      if (record.LeaseToken != lease.Token)
      {
        _logger.LogWarning(
          "Claim on notification {NotificationId} lapsed; dropping push status {PushStatus}",
          message.Id, message.PushStatus);
        return false;
      }

      record.LeaseToken = null;
      record.LeaseExpiresAt = null;
    }

    var incoming = ToProtoPushStatus(message.PushStatus);

    if (!record.IsCurrent)
    {
      // Outdated notifications stay Unspecified, unless a send already went out.
      if (incoming == EPushStatus.Delivered)
        record.PushStatus = EPushStatus.Delivered;

      return true;
    }

    if (incoming == EPushStatus.Failed && record.PushStatus != EPushStatus.Delivered)
    {
      record.FailedPushAttempts++;
      record.NextPushAttemptAt = Now + Backoff(record.FailedPushAttempts);
    }

    if (Rank(incoming) > Rank(record.PushStatus))
      record.PushStatus = incoming;

    return true;
  }

  private long Backoff(int failedAttempts)
  {
    var factor = Math.Pow(2, Math.Min(failedAttempts - 1, 16));
    return (long)(_options.BaseBackoff.TotalMilliseconds * factor);
  }

  // Push status only moves forward: Pending < Failed < Delivered.
  private static int Rank(EPushStatus status) => status switch
  {
    EPushStatus.Pending => 1,
    EPushStatus.Failed => 2,
    EPushStatus.Delivered => 3,
    _ => 0,
  };

  private static PushLease? GetLease(HazardNotification message)
    => message.ExtraData.TryGetValue(typeof(PushLease), out var lease) ? (PushLease)lease : null;

  private static HazardNotification ToClaimedNotification(NotificationRecord record, Guid token)
  {
    var message = ToNotification(record, View.Full);
    var extraData = message.ExtraData.SetItem(typeof(PushLease), new PushLease(token));

    if (record.TraceParent != null
      && ActivityContext.TryParse(record.TraceParent, null, out var parent))
      extraData = extraData.SetItem(typeof(ActivityContext), parent);

    return message with { ExtraData = extraData };
  }

  private static HazardNotification ToNotification(NotificationRecord record, View view)
  {
    var hazard = EHazardEvent.Parser.ParseFrom(record.Hazard);

    var extraDataBuilder = ImmutableDictionary.CreateBuilder<Type, object>();
    extraDataBuilder.Add(
      typeof(NotificationCreatedAt),
      new NotificationCreatedAt(DateTimeOffset.FromUnixTimeMilliseconds(record.CreatedAt)));

    if (view == View.Full)
      extraDataBuilder.Add(typeof(EHazardEvent), hazard);
    else
      extraDataBuilder.Add(typeof(HazardSummary), ToSummary(hazard));

    return new HazardNotification(
      record.Id,
      hazard.Source switch
      {
        Source.Bmkg => HazardSource.BMKG,
        Source.Pvmbg => HazardSource.PVMBG,
        _ => HazardSource.Unknown,
      },
      hazard.HazardType switch
      {
        EHazardType.Seismic => HazardType.Seismic,
        EHazardType.Volcanic => HazardType.Volcanic,
        _ => HazardType.Unknown,
      },
      hazard.Severity switch
      {
        Severity.Siaga => HazardSeverity.Siaga,
        Severity.Awas => HazardSeverity.Awas,
        _ => HazardSeverity.Other,
      },
      hazard.AreaName,
      hazard.OccurredAt.ToDateTimeOffset(),
      "",
      record.PushStatus switch
      {
        EPushStatus.Pending => PushStatus.Pending,
        EPushStatus.Failed => PushStatus.Failed,
        EPushStatus.Delivered => PushStatus.Delivered,
        _ => PushStatus.Unspecified,
      },
      extraDataBuilder.ToImmutable());
  }

  // Allowlist copy: restricted fields are absent, not blanked.
  private static HazardSummary ToSummary(EHazardEvent hazard) => new()
  {
    HazardId = hazard.HazardId,
    Source = hazard.Source,
    HazardType = hazard.HazardType,
    Severity = hazard.Severity,
    AreaName = hazard.AreaName,
    OccurredAt = hazard.OccurredAt,
    IngestedAt = hazard.IngestedAt,
  };

  private static EPushStatus ToProtoPushStatus(PushStatus status) => status switch
  {
    PushStatus.Pending => EPushStatus.Pending,
    PushStatus.Failed => EPushStatus.Failed,
    PushStatus.Delivered => EPushStatus.Delivered,
    _ => EPushStatus.Unspecified,
  };

  private static Source ToProtoSource(HazardSource source) => source switch
  {
    HazardSource.BMKG => Source.Bmkg,
    HazardSource.PVMBG => Source.Pvmbg,
    _ => Source.Unspecified,
  };

  private static EHazardType ToProtoType(HazardType type) => type switch
  {
    HazardType.Seismic => EHazardType.Seismic,
    HazardType.Volcanic => EHazardType.Volcanic,
    _ => EHazardType.Unspecified,
  };

  private static Severity ToProtoSeverity(HazardSeverity severity) => severity switch
  {
    HazardSeverity.Siaga => Severity.Siaga,
    HazardSeverity.Awas => Severity.Awas,
    _ => Severity.Unspecified,
  };

  // Binds a cursor to the request it was issued for.
  private string Fingerprint(
    HazardSource? source,
    HazardType? type,
    HazardSeverity? severity,
    DateTimeOffset? since,
    DateTimeOffset? until,
    int pageSize)
  {
    var input = string.Join('|',
      _view,
      source,
      type,
      severity,
      since?.ToUnixTimeMilliseconds(),
      until?.ToUnixTimeMilliseconds(),
      pageSize);

    return Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(input)), 0, 8);
  }

  // Cursor: base64url("<createdAt>.<fingerprint>.<id>"); the id goes last because it may contain dots.
  internal static string FormatCursor(long createdAt, string id, string fingerprint)
    => Base64UrlEncode($"{createdAt}{CursorDelimiter}{fingerprint}{CursorDelimiter}{id}");

  private (long, string) ParseCursor(string pageToken, string fingerprint)
  {
    string[] parts;

    try
    {
      parts = Base64UrlDecode(pageToken).Split(CursorDelimiter, 3);
    }
    catch (FormatException ex)
    {
      _logger.LogWarning(ex, "Malformed page token found: {PageToken}", pageToken);
      throw new BadInputException("Page token is malformed");
    }

    if (parts.Length != 3 || !long.TryParse(parts[0], out var createdAt) || parts[2].Length == 0)
      throw new BadInputException("Page token is malformed");

    if (parts[1] != fingerprint)
      throw new BadInputException("Page token does not match the request");

    return (createdAt, parts[2]);
  }

  private static string Base64UrlEncode(string value)
    => Convert.ToBase64String(Encoding.UTF8.GetBytes(value))
      .TrimEnd('=')
      .Replace('+', '-')
      .Replace('/', '_');

  private static string Base64UrlDecode(string value)
  {
    var base64 = value.Replace('-', '+').Replace('_', '/');
    base64 += (base64.Length % 4) switch
    {
      2 => "==",
      3 => "=",
      0 => "",
      _ => throw new FormatException("Invalid base64url length"),
    };

    return Encoding.UTF8.GetString(Convert.FromBase64String(base64));
  }
}
