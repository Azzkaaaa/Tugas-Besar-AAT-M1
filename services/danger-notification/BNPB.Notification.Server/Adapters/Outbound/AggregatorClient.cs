using System.Diagnostics;
using Grpc.Core;
using BNPB.Aggregator.V1;
using BNPB.Notification.Server.Core.Models;
using BNPB.Notification.Server.Core.Ports.Outbound;

using Source = BNPB.Common.V1.Source;
using ESourceFreshness = BNPB.Common.V1.SourceFreshness;
using SourceFreshness = BNPB.Notification.Server.Core.Models.SourceFreshness;
using GrpcClient = BNPB.Aggregator.V1.AggregatorService.AggregatorServiceClient;

namespace BNPB.Notification.Server.Adapters.Outbound;

// Notifications are served from the local store; the Aggregator only reports source freshness.
public class AggregatorClient : ISourceProvider
{
  public static readonly TimeSpan DefaultFreshnessTtl = TimeSpan.FromSeconds(5);

  private static ImmutableList<SourceFreshness> s_freshness = [];
  private static long s_freshnessFetchedAt;

  private readonly ILogger<AggregatorClient> _logger;
  private readonly GrpcClient _grpcClient;
  private readonly TimeSpan _freshnessTtl;

  public AggregatorClient(
      ILogger<AggregatorClient> logger,
      GrpcClient grpcClient,
      TimeSpan? freshnessTtl = null)
  {
    _logger = logger;
    _grpcClient = grpcClient;
    _freshnessTtl = freshnessTtl ?? DefaultFreshnessTtl;
  }

  public async Task<IReadOnlyList<SourceFreshness>> GetSourceFreshnessAsync(
    CancellationToken cancellationToken)
  {
    var freshness = s_freshness;
    var age = Stopwatch.GetElapsedTime(Interlocked.Read(ref s_freshnessFetchedAt));

    // Freshness is reported per response, so the cache only absorbs bursts.
    if (!freshness.IsEmpty && age < _freshnessTtl)
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
    Interlocked.Exchange(ref s_freshnessFetchedAt, Stopwatch.GetTimestamp());

    return s_freshness;
  }

  // Test hook: the freshness list is static and would otherwise leak between tests.
  internal static void ResetFreshness()
  {
    s_freshness = [];
    s_freshnessFetchedAt = 0;
  }

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
}
