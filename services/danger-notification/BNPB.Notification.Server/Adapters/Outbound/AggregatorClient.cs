using System.Text;
using System.Runtime.Serialization;
using Grpc.Core;
using BNPB.Common.V1;
using BNPB.Aggregator.V1;
using BNPB.Notification.Server.Core.Models;
using BNPB.Notification.Server.Core.Ports.Outbound;
using BNPB.Notification.Server.Adapters.Exceptions;

using EHazardType = BNPB.Common.V1.HazardType;
using HazardType = BNPB.Notification.Server.Core.Models.HazardType;
using EHazardEvent = BNPB.Common.V1.HazardEvent;
using HazardEvent = BNPB.Notification.Server.Core.Models.HazardEvent;
using ESourceFreshness = BNPB.Common.V1.SourceFreshness;
using SourceFreshness = BNPB.Notification.Server.Core.Models.SourceFreshness;
using Timestamp = Google.Protobuf.WellKnownTypes.Timestamp;
using GrpcClient = BNPB.Aggregator.V1.AggregatorService.AggregatorServiceClient;

namespace BNPB.Notification.Server.Adapters.Outbound;

public class AggregatorClient : IMessageProvider, IMessageListProvider, ISourceProvider
{
  private const char TokenDelimiter = ':';
  private const char TokenEscape = '\\';
  private const int ExhaustedOffset = -1;

  private static ImmutableList<SourceFreshness> s_freshness = [];

  private readonly ILogger<AggregatorClient> _logger;
  private readonly GrpcClient _grpcClient;
  private readonly View _view;

  public AggregatorClient(
      ILogger<AggregatorClient> logger,
      GrpcClient grpcClient,
      View view = View.Summary)
  {
    _logger = logger;
    _grpcClient = grpcClient;
    _view = view;
  }

  public async Task<HazardEvent?> GetMessageAsync(string messageId, CancellationToken cancellationToken)
  {
    GetHazardResponse response;

    try
    {
      var request = new GetHazardRequest
      {
        HazardId = messageId,
        View = _view,
      };

      response = await _grpcClient.GetHazardAsync(
        request,
        cancellationToken: cancellationToken);
    }
    catch (RpcException ex) when (ex.StatusCode == StatusCode.NotFound)
    {
      _logger.LogWarning("Message {MessageId} not found on the gRPC server.", messageId);
      return null;
    }
    catch (RpcException ex)
    {
      _logger.LogError(ex, "gRPC error occurred while fetching message {MessageId}. Status Code: {StatusCode}", messageId, ex.StatusCode);
      throw;
    }
    catch (Exception ex)
    {
      _logger.LogError(ex, "Unexpected error fetching message {MessageId}.", messageId);
      throw;
    }

    RefreshFreshness(response.Freshness);

    var message = MapProjection(response.Item);

    return message.Severity switch
    {
      HazardSeverity.Siaga or HazardSeverity.Awas => message,
      _ => null,
    };
  }

  public async Task<(IList<HazardEvent>, string)> GetMessageListAsync(
    HazardSource? source,
    HazardType? type,
    HazardSeverity? severity,
    DateTimeOffset? since,
    DateTimeOffset? until,
    int pageSize,
    string? pageToken,
    CancellationToken cancellationToken)
  {
    if (severity is HazardSeverity.Siaga or HazardSeverity.Awas)
    {
      return await GetMessageListSingleAsync(
        source,
        type,
        severity.Value,
        since,
        until,
        pageSize,
        pageToken,
        cancellationToken);
    }

    var awasListState = new AutoListState();
    var siagaListState = new AutoListState();

    if (!string.IsNullOrEmpty(pageToken))
    {
      try
      {
        var tokens = SplitToken(pageToken);

        if (tokens.Count != 4) throw new FormatException("Unexpected token fragment count");

        awasListState.PageToken = tokens[0];
        awasListState.Offset = int.Parse(tokens[1]);

        siagaListState.PageToken = tokens[2];
        siagaListState.Offset = int.Parse(tokens[3]);

        if (awasListState.Offset < ExhaustedOffset || siagaListState.Offset < ExhaustedOffset)
          throw new FormatException("Negative offset");
      }
      catch (Exception ex)
      {
        _logger.LogError(ex, "Malformed page token found: {PageToken}", pageToken);
        throw new BadInputException("Page token is malformed");
      }
    }

    var awasList = AutoMessageListAsync(
      source,
      type,
      HazardSeverity.Awas,
      since,
      until,
      pageSize,
      awasListState,
      cancellationToken);

    var siagaList = AutoMessageListAsync(
      source,
      type,
      HazardSeverity.Siaga,
      since,
      until,
      pageSize,
      siagaListState,
      cancellationToken);

    var result = new List<HazardEvent>(pageSize);

    await using var awasEnum = awasList.GetAsyncEnumerator(cancellationToken);
    await using var siagaEnum = siagaList.GetAsyncEnumerator(cancellationToken);

    bool awasHasNext = await awasEnum.MoveNextAsync();
    bool siagaHasNext = await siagaEnum.MoveNextAsync();

    while (awasHasNext && siagaHasNext && result.Count < pageSize)
    {
      if (awasEnum.Current.OccurredAt >= siagaEnum.Current.OccurredAt)
      {
        result.Add(awasEnum.Current);
        awasHasNext = await awasEnum.MoveNextAsync();
      }
      else
      {
        result.Add(siagaEnum.Current);
        siagaHasNext = await siagaEnum.MoveNextAsync();
      }
    }

    while (awasHasNext && result.Count < pageSize)
    {
      result.Add(awasEnum.Current);
      awasHasNext = await awasEnum.MoveNextAsync();
    }

    while (siagaHasNext && result.Count < pageSize)
    {
      result.Add(siagaEnum.Current);
      siagaHasNext = await siagaEnum.MoveNextAsync();
    }

    if (awasListState.IsExhausted && siagaListState.IsExhausted)
      return (result, "");

    string[] tokenFragments =
    [
      awasListState.PageToken ?? "",
      awasListState.Offset.ToString(),
      siagaListState.PageToken ?? "",
      siagaListState.Offset.ToString()
    ];

    var token = JoinToken(tokenFragments);

    return (result, token);
  }

  public async Task<IReadOnlyList<SourceFreshness>> GetSourceFreshnessAsync(
    CancellationToken cancellationToken)
  {
    var freshness = s_freshness;

    if (!freshness.IsEmpty)
      return freshness;

    GetSourceStatusesResponse response;

    try
    {
      response = await _grpcClient.GetSourceStatusesAsync(
        new GetSourceStatusesRequest(),
        cancellationToken: cancellationToken);
    }
    catch (RpcException ex)
    {
      _logger.LogError(ex, "gRPC error occurred while fetching source statuses. Status Code: {StatusCode}", ex.StatusCode);
      throw;
    }

    RefreshFreshness(response.Freshness);

    return s_freshness;
  }

  // Test hook: the freshness list is static and would otherwise leak between tests.
  internal static void ResetFreshness() => s_freshness = [];

  private static void RefreshFreshness(IEnumerable<ESourceFreshness> freshness)
  {
    var incoming = freshness.ToList();

    if (incoming.Count == 0) return;

    ImmutableInterlocked.Update(ref s_freshness, current =>
    {
      var builder = current.ToBuilder();

      foreach (var entry in incoming)
      {
        builder.RemoveAll(existing => GetSource(existing) == entry.Source);
        builder.Add(MapSourceFreshness(entry));
      }

      return builder.ToImmutable();
    });
  }

  private static Source? GetSource(SourceFreshness freshness)
    => freshness.ExtraData.TryGetValue(typeof(ESourceFreshness), out var value)
      ? ((ESourceFreshness)value).Source
      : null;

  private static SourceFreshness MapSourceFreshness(ESourceFreshness freshness)
  {
    var extraDataBuilder = ImmutableDictionary.CreateBuilder<Type, object>();
    extraDataBuilder.Add(typeof(ESourceFreshness), freshness);

    return new SourceFreshness(extraDataBuilder.ToImmutable());
  }

  private class AutoListState
  {
    public string? PageToken { get; set; }
    public int Offset { get; set; }
    public bool IsExhausted => Offset == ExhaustedOffset;
  }

  private async IAsyncEnumerable<HazardEvent> AutoMessageListAsync(
    HazardSource? source,
    HazardType? type,
    HazardSeverity severity,
    DateTimeOffset? since,
    DateTimeOffset? until,
    int pageSize,
    AutoListState state,
    [System.Runtime.CompilerServices.EnumeratorCancellation]
    CancellationToken cancellationToken)
  {
    if (state.IsExhausted) yield break;

    var pageToken = state.PageToken;
    var offset = state.Offset;

    while (true)
    {
      var (page, nextPageToken) = await GetMessageListSingleAsync(
        source,
        type,
        severity,
        since,
        until,
        pageSize,
        pageToken,
        cancellationToken);

      for (int i = offset; i < page.Count; i++)
      {
        state.PageToken = pageToken;
        state.Offset = i;

        yield return page[i];
      }

      if (string.IsNullOrEmpty(nextPageToken))
      {
        state.PageToken = null;
        state.Offset = ExhaustedOffset;
        yield break;
      }

      pageToken = nextPageToken;
      offset = 0;
    }
  }

  private async Task<(IList<HazardEvent>, string)> GetMessageListSingleAsync(
    HazardSource? source,
    HazardType? type,
    HazardSeverity severity,
    DateTimeOffset? since,
    DateTimeOffset? until,
    int pageSize,
    string? pageToken,
    CancellationToken cancellationToken)
  {
    ListHazardsResponse response;

    try
    {
      var request = new ListHazardsRequest
      {
        Filter = new HazardFilter
        {
          Source = source switch
          {
            HazardSource.BMKG => Source.Bmkg,
            HazardSource.PVMBG => Source.Pvmbg,
            _ => Source.Unspecified,
          },
          HazardType = type switch
          {
            HazardType.Seismic => EHazardType.Seismic,
            HazardType.Volcanic => EHazardType.Volcanic,
            _ => EHazardType.Unspecified,
          },
          Severity = severity switch
          {
            HazardSeverity.Siaga => Severity.Siaga,
            HazardSeverity.Awas => Severity.Awas,
            _ => throw new ArgumentException("Severity has to be one of Siaga or Awas", nameof(severity)),
          },
          Since = since.HasValue ? Timestamp.FromDateTimeOffset(since.Value) : null,
          Until = until.HasValue ? Timestamp.FromDateTimeOffset(until.Value) : null,
        },
        View = _view,
        Page = new PageRequest
        {
          PageSize = pageSize,
          PageToken = pageToken ?? "",
        }
      };

      response = await _grpcClient.ListHazardsAsync(
        request,
        cancellationToken: cancellationToken);
    }
    catch (RpcException ex) when (ex.StatusCode == StatusCode.NotFound)
    {
      _logger.LogWarning("Messages not found on the gRPC server.");
      return ([], "");
    }
    catch (RpcException ex)
    {
      _logger.LogError(ex, "gRPC error occurred while fetching messages. Status Code: {StatusCode}", ex.StatusCode);
      throw;
    }
    catch (Exception ex)
    {
      _logger.LogError(ex, "Unexpected error fetching messages.");
      throw;
    }

    RefreshFreshness(response.Freshness);

    var messages = new List<HazardEvent>(response.Items.Count);

    foreach (var item in response.Items)
    {
      messages.Add(MapProjection(item));
    }

    return (messages, response.NextPageToken);
  }

  private HazardEvent MapProjection(HazardProjection projection)
  {
    try
    {
      return projection.DataCase switch
      {
        HazardProjection.DataOneofCase.Summary
          => MapHazardEvent(projection.Summary),

        HazardProjection.DataOneofCase.Full
          => MapHazardEvent(projection.Full),

        _ => throw new SerializationException("Hazard projection is empty")
      };
    }
    catch (Exception ex)
    {
      _logger.LogError(ex, "Error when deserializing message from upstream source.");
      throw new SerializationException("Message deserialization error", ex);
    }
  }

  private static HazardEvent MapHazardEvent(EHazardEvent hazardEvent)
  {
    var extraDataBuilder = ImmutableDictionary.CreateBuilder<Type, object>();
    extraDataBuilder.Add(typeof(EHazardEvent), hazardEvent);

    return new HazardEvent(
      hazardEvent.HazardId,
      MapSource(hazardEvent.Source),
      MapType(hazardEvent.HazardType),
      MapSeverity(hazardEvent.Severity),
      hazardEvent.AreaName,
      hazardEvent.OccurredAt.ToDateTimeOffset(),
      "",
      extraDataBuilder.ToImmutable()
    );
  }

  private static HazardEvent MapHazardEvent(HazardSummary hazardSummary)
  {
    var extraDataBuilder = ImmutableDictionary.CreateBuilder<Type, object>();
    extraDataBuilder.Add(typeof(HazardSummary), hazardSummary);

    return new HazardEvent(
      hazardSummary.HazardId,
      MapSource(hazardSummary.Source),
      MapType(hazardSummary.HazardType),
      MapSeverity(hazardSummary.Severity),
      hazardSummary.AreaName,
      hazardSummary.OccurredAt.ToDateTimeOffset(),
      "",
      extraDataBuilder.ToImmutable()
    );
  }

  private static HazardSource MapSource(Source source) => source switch
  {
    Source.Bmkg => HazardSource.BMKG,
    Source.Pvmbg => HazardSource.PVMBG,
    _ => HazardSource.Unknown,
  };

  private static HazardType MapType(EHazardType type) => type switch
  {
    EHazardType.Seismic => HazardType.Seismic,
    EHazardType.Volcanic => HazardType.Volcanic,
    _ => HazardType.Unknown,
  };

  private static HazardSeverity MapSeverity(Severity severity) => severity switch
  {
    Severity.Siaga => HazardSeverity.Siaga,
    Severity.Awas => HazardSeverity.Awas,
    _ => HazardSeverity.Other,
  };

  internal static List<string> SplitToken(string input)
  {
    if (string.IsNullOrEmpty(input)) return [];

    var result = new List<string>();
    var currentToken = new StringBuilder();
    bool isEscaped = false;

    foreach (char c in input)
    {
      if (isEscaped)
      {
        currentToken.Append(c);
        isEscaped = false;
      }
      else if (c == TokenEscape)
      {
        isEscaped = true;
      }
      else if (c == TokenDelimiter)
      {
        result.Add(currentToken.ToString());
        currentToken.Clear();
      }
      else
      {
        currentToken.Append(c);
      }
    }

    if (isEscaped) throw new FormatException("Dangling escape character in token");

    result.Add(currentToken.ToString());
    return result;
  }

  internal static string JoinToken(IEnumerable<string> values)
  {
    if (values == null) return string.Empty;

    var sb = new StringBuilder();
    using var enumerator = values.GetEnumerator();

    if (enumerator.MoveNext())
    {
      AppendEscaped(sb, enumerator.Current);

      while (enumerator.MoveNext())
      {
        sb.Append(TokenDelimiter);
        AppendEscaped(sb, enumerator.Current);
      }
    }

    return sb.ToString();
  }

  private static void AppendEscaped(StringBuilder sb, string value)
  {
    if (string.IsNullOrEmpty(value)) return;

    foreach (char c in value)
    {
      if (c == TokenEscape || c == TokenDelimiter)
      {
        sb.Append(TokenEscape);
      }
      sb.Append(c);
    }
  }
}
