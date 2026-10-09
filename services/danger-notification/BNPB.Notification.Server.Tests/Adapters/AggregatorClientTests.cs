using Grpc.Core;
using Microsoft.Extensions.Logging.Abstractions;
using BNPB.Common.V1;
using BNPB.Aggregator.V1;
using BNPB.Notification.Server.Adapters.Outbound;

using ESourceFreshness = BNPB.Common.V1.SourceFreshness;
using SourceFreshness = BNPB.Notification.Server.Core.Models.SourceFreshness;
using GrpcClient = BNPB.Aggregator.V1.AggregatorService.AggregatorServiceClient;

namespace BNPB.Notification.Server.Tests.Adapters;

// The freshness list is static, so every test resets it and the tests in this
// class must not run in parallel with any other user of AggregatorClient.
[Collection(nameof(AggregatorClientTests))]
public class AggregatorClientTests : IDisposable
{
  private readonly GrpcClient _grpcClient = Substitute.For<GrpcClient>();
  private readonly AggregatorClient _client;

  public AggregatorClientTests()
  {
    AggregatorClient.ResetFreshness();
    _client = new AggregatorClient(NullLogger<AggregatorClient>.Instance, _grpcClient);
  }

  public void Dispose() => AggregatorClient.ResetFreshness();

  private static AsyncUnaryCall<T> Call<T>(T response)
    => new(
      Task.FromResult(response),
      Task.FromResult(new Metadata()),
      () => Status.DefaultSuccess,
      () => [],
      () => { });

  private static AsyncUnaryCall<T> Fail<T>(StatusCode code)
    => new(
      Task.FromException<T>(new RpcException(new Status(code, code.ToString()))),
      Task.FromResult(new Metadata()),
      () => new Status(code, code.ToString()),
      () => [],
      () => { });

  private void SetupSourceStatuses(params ESourceFreshness[] freshness)
  {
    var response = new GetSourceStatusesResponse();
    response.Freshness.AddRange(freshness);

    _grpcClient
      .GetSourceStatusesAsync(Arg.Any<GetSourceStatusesRequest>(), Arg.Any<Metadata>(), Arg.Any<DateTime?>(), Arg.Any<CancellationToken>())
      .Returns(Call(response));
  }

  private static ESourceFreshness ProtoOf(SourceFreshness freshness)
    => (ESourceFreshness)freshness.ExtraData[typeof(ESourceFreshness)];

  private void ReceivedStatusCalls(int count)
    => _ = _grpcClient.Received(count)
      .GetSourceStatusesAsync(Arg.Any<GetSourceStatusesRequest>(), Arg.Any<Metadata>(), Arg.Any<DateTime?>(), Arg.Any<CancellationToken>());

  [Fact]
  public async Task GetSourceFreshness_WithinTtl_FetchesStatusesOnce()
  {
    SetupSourceStatuses(
      TestData.ProtoFreshness(Source.Bmkg, SourceStatus.Fresh),
      TestData.ProtoFreshness(Source.Pvmbg, SourceStatus.Unavailable));

    var first = await _client.GetSourceFreshnessAsync(CancellationToken.None);
    var second = await _client.GetSourceFreshnessAsync(CancellationToken.None);

    Assert.Equal(2, first.Count);
    Assert.Equal(2, second.Count);
    ReceivedStatusCalls(1);
  }

  [Fact]
  public async Task GetSourceFreshness_AfterTtl_RefetchesAndReplacesPerSource()
  {
    var client = new AggregatorClient(NullLogger<AggregatorClient>.Instance, _grpcClient, TimeSpan.Zero);

    SetupSourceStatuses(
      TestData.ProtoFreshness(Source.Bmkg, SourceStatus.Fresh),
      TestData.ProtoFreshness(Source.Pvmbg, SourceStatus.Fresh));
    await client.GetSourceFreshnessAsync(CancellationToken.None);

    SetupSourceStatuses(TestData.ProtoFreshness(Source.Bmkg, SourceStatus.Stale));
    var freshness = (await client.GetSourceFreshnessAsync(CancellationToken.None))
      .Select(ProtoOf)
      .ToDictionary(entry => entry.Source, entry => entry.Status);

    Assert.Equal(SourceStatus.Stale, freshness[Source.Bmkg]);
    Assert.Equal(SourceStatus.Fresh, freshness[Source.Pvmbg]);
    ReceivedStatusCalls(2);
  }

  [Fact]
  public async Task GetSourceFreshness_IsSharedAcrossInstances()
  {
    SetupSourceStatuses(TestData.ProtoFreshness(Source.Bmkg, SourceStatus.Fresh));
    await _client.GetSourceFreshnessAsync(CancellationToken.None);

    var otherClient = new AggregatorClient(NullLogger<AggregatorClient>.Instance, _grpcClient);
    var freshness = await otherClient.GetSourceFreshnessAsync(CancellationToken.None);

    Assert.Equal(Source.Bmkg, ProtoOf(Assert.Single(freshness)).Source);
    ReceivedStatusCalls(1);
  }

  [Fact]
  public async Task GetSourceFreshness_UpstreamUnavailable_Rethrows()
  {
    _grpcClient
      .GetSourceStatusesAsync(Arg.Any<GetSourceStatusesRequest>(), Arg.Any<Metadata>(), Arg.Any<DateTime?>(), Arg.Any<CancellationToken>())
      .Returns(Fail<GetSourceStatusesResponse>(StatusCode.Unavailable));

    var ex = await Assert.ThrowsAsync<RpcException>(
      () => _client.GetSourceFreshnessAsync(CancellationToken.None));

    Assert.Equal(StatusCode.Unavailable, ex.StatusCode);
  }
}
