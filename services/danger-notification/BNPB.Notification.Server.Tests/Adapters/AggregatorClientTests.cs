using Grpc.Core;
using Microsoft.Extensions.Logging.Abstractions;
using BNPB.Common.V1;
using BNPB.Aggregator.V1;
using BNPB.Notification.Server.Core.Models;
using BNPB.Notification.Server.Adapters.Outbound;
using BNPB.Notification.Server.Adapters.Exceptions;

using ESourceFreshness = BNPB.Common.V1.SourceFreshness;
using HazardEvent = BNPB.Notification.Server.Core.Models.HazardEvent;
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
    _client = new AggregatorClient(NullLogger<AggregatorClient>.Instance, _grpcClient, View.Summary);
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

  private void SetupGetHazard(GetHazardResponse response)
    => _grpcClient
      .GetHazardAsync(Arg.Any<GetHazardRequest>(), Arg.Any<Metadata>(), Arg.Any<DateTime?>(), Arg.Any<CancellationToken>())
      .Returns(Call(response));

  private void SetupSourceStatuses(params ESourceFreshness[] freshness)
  {
    var response = new GetSourceStatusesResponse();
    response.Freshness.AddRange(freshness);

    _grpcClient
      .GetSourceStatusesAsync(Arg.Any<GetSourceStatusesRequest>(), Arg.Any<Metadata>(), Arg.Any<DateTime?>(), Arg.Any<CancellationToken>())
      .Returns(Call(response));
  }

  private static GetHazardResponse HazardResponse(Severity severity, params ESourceFreshness[] freshness)
  {
    var response = new GetHazardResponse
    {
      Item = new HazardProjection { Summary = TestData.Summary("h1", severity) },
    };
    response.Freshness.AddRange(freshness);
    return response;
  }

  private static ESourceFreshness ProtoOf(SourceFreshness freshness)
    => (ESourceFreshness)freshness.ExtraData[typeof(ESourceFreshness)];

  // Serves each severity from an in-memory list ordered by occurred_at desc;
  // the upstream page token is the index of the page's first item.
  private void SetupPagedAggregator(
    IReadOnlyList<HazardSummary> awas,
    IReadOnlyList<HazardSummary> siaga)
  {
    _grpcClient
      .ListHazardsAsync(Arg.Any<ListHazardsRequest>(), Arg.Any<Metadata>(), Arg.Any<DateTime?>(), Arg.Any<CancellationToken>())
      .Returns(call =>
      {
        var request = call.Arg<ListHazardsRequest>();
        var items = request.Filter.Severity == Severity.Awas ? awas : siaga;
        var start = string.IsNullOrEmpty(request.Page.PageToken) ? 0 : int.Parse(request.Page.PageToken);
        var end = Math.Min(start + request.Page.PageSize, items.Count);

        var response = new ListHazardsResponse
        {
          NextPageToken = end < items.Count ? end.ToString() : "",
        };
        response.Items.AddRange(items.Skip(start).Take(end - start)
          .Select(item => new HazardProjection { Summary = item }));
        response.Freshness.Add(TestData.ProtoFreshness(Source.Bmkg, SourceStatus.Fresh));

        return Call(response);
      });
  }

  [Fact]
  public async Task GetMessage_RefreshesFreshnessWithoutStatusCall()
  {
    SetupGetHazard(HazardResponse(Severity.Awas, TestData.ProtoFreshness(Source.Bmkg, SourceStatus.Fresh)));

    await _client.GetMessageAsync("h1", CancellationToken.None);
    var freshness = await _client.GetSourceFreshnessAsync(CancellationToken.None);

    var entry = ProtoOf(Assert.Single(freshness));
    Assert.Equal(Source.Bmkg, entry.Source);
    Assert.Equal(SourceStatus.Fresh, entry.Status);
    _ = _grpcClient.DidNotReceiveWithAnyArgs()
      .GetSourceStatusesAsync(default!, default(Metadata), default, default);
  }

  [Fact]
  public async Task GetMessage_DoesNotPutFreshnessIntoHazardEvent()
  {
    SetupGetHazard(HazardResponse(Severity.Awas, TestData.ProtoFreshness(Source.Bmkg, SourceStatus.Fresh)));

    var message = await _client.GetMessageAsync("h1", CancellationToken.None);

    var key = Assert.Single(message!.Value.ExtraData.Keys);
    Assert.Equal(typeof(HazardSummary), key);
  }

  [Fact]
  public async Task Freshness_IsSharedAcrossInstancesAndReplacedPerSource()
  {
    SetupGetHazard(HazardResponse(Severity.Awas, TestData.ProtoFreshness(Source.Bmkg, SourceStatus.Fresh)));
    await _client.GetMessageAsync("h1", CancellationToken.None);

    var otherClient = new AggregatorClient(NullLogger<AggregatorClient>.Instance, _grpcClient, View.Full);
    SetupGetHazard(HazardResponse(
      Severity.Awas,
      TestData.ProtoFreshness(Source.Bmkg, SourceStatus.Stale),
      TestData.ProtoFreshness(Source.Pvmbg, SourceStatus.Fresh)));
    await otherClient.GetMessageAsync("h1", CancellationToken.None);

    var freshness = (await _client.GetSourceFreshnessAsync(CancellationToken.None))
      .Select(ProtoOf)
      .ToDictionary(entry => entry.Source, entry => entry.Status);

    Assert.Equal(2, freshness.Count);
    Assert.Equal(SourceStatus.Stale, freshness[Source.Bmkg]);
    Assert.Equal(SourceStatus.Fresh, freshness[Source.Pvmbg]);
  }

  [Fact]
  public async Task GetSourceFreshness_EmptyList_FetchesStatuses()
  {
    SetupSourceStatuses(
      TestData.ProtoFreshness(Source.Bmkg, SourceStatus.Fresh),
      TestData.ProtoFreshness(Source.Pvmbg, SourceStatus.Unavailable));

    var first = await _client.GetSourceFreshnessAsync(CancellationToken.None);
    var second = await _client.GetSourceFreshnessAsync(CancellationToken.None);

    Assert.Equal(2, first.Count);
    Assert.Equal(2, second.Count);
    _ = _grpcClient.Received(1)
      .GetSourceStatusesAsync(Arg.Any<GetSourceStatusesRequest>(), Arg.Any<Metadata>(), Arg.Any<DateTime?>(), Arg.Any<CancellationToken>());
  }

  [Theory]
  [InlineData(Severity.Normal)]
  [InlineData(Severity.Waspada)]
  public async Task GetMessage_NonNotificationSeverity_ReturnsNull(Severity severity)
  {
    SetupGetHazard(HazardResponse(severity));

    Assert.Null(await _client.GetMessageAsync("h1", CancellationToken.None));
  }

  [Fact]
  public async Task GetMessage_UpstreamNotFound_ReturnsNull()
  {
    _grpcClient
      .GetHazardAsync(Arg.Any<GetHazardRequest>(), Arg.Any<Metadata>(), Arg.Any<DateTime?>(), Arg.Any<CancellationToken>())
      .Returns(Fail<GetHazardResponse>(StatusCode.NotFound));

    Assert.Null(await _client.GetMessageAsync("h1", CancellationToken.None));
  }

  [Fact]
  public async Task GetMessage_UpstreamUnavailable_Rethrows()
  {
    _grpcClient
      .GetHazardAsync(Arg.Any<GetHazardRequest>(), Arg.Any<Metadata>(), Arg.Any<DateTime?>(), Arg.Any<CancellationToken>())
      .Returns(Fail<GetHazardResponse>(StatusCode.Unavailable));

    var ex = await Assert.ThrowsAsync<RpcException>(() => _client.GetMessageAsync("h1", CancellationToken.None));
    Assert.Equal(StatusCode.Unavailable, ex.StatusCode);
  }

  [Fact]
  public async Task GetMessageList_SingleSeverity_PassesThroughUpstreamPage()
  {
    SetupPagedAggregator(
      [TestData.Summary("a1", minutes: 3), TestData.Summary("a2", minutes: 2), TestData.Summary("a3", minutes: 1)],
      []);

    var (items, token) = await _client.GetMessageListAsync(
      null, null, HazardSeverity.Awas, null, null, 2, null, CancellationToken.None);

    Assert.Equal(["a1", "a2"], items.Select(item => item.Id));
    Assert.Equal("2", token);
  }

  [Theory]
  [InlineData(null)]
  [InlineData(HazardSeverity.Other)]
  public async Task GetMessageList_NoSeverityFilter_MergesAwasAndSiagaAcrossPages(HazardSeverity? severity)
  {
    SetupPagedAggregator(
      [TestData.Summary("a10", minutes: 10), TestData.Summary("a7", minutes: 7), TestData.Summary("a4", minutes: 4)],
      [
        TestData.Summary("s9", Severity.Siaga, minutes: 9),
        TestData.Summary("s8", Severity.Siaga, minutes: 8),
        TestData.Summary("s1", Severity.Siaga, minutes: 1),
      ]);

    var collected = new List<HazardEvent>();
    var pages = 0;
    string? token = null;

    do
    {
      var (items, next) = await _client.GetMessageListAsync(
        null, null, severity, null, null, 2, token, CancellationToken.None);

      collected.AddRange(items);
      token = next;
      pages++;
    }
    while (!string.IsNullOrEmpty(token) && pages < 10);

    Assert.Equal(["a10", "s9", "s8", "a7", "a4", "s1"], collected.Select(item => item.Id));
    Assert.Equal(3, pages);
  }

  [Fact]
  public async Task GetMessageList_NoMatches_ReturnsEmptyTokenAndItems()
  {
    SetupPagedAggregator([], []);

    var (items, token) = await _client.GetMessageListAsync(
      null, null, null, null, null, 5, null, CancellationToken.None);

    Assert.Empty(items);
    Assert.Equal("", token);
  }

  [Theory]
  [InlineData("garbage")]
  [InlineData("a:1:b")]
  [InlineData("a:x:b:0")]
  [InlineData("a:-5:b:0")]
  [InlineData("a:1:b:0\\")]
  public async Task GetMessageList_MalformedToken_IsBadInput(string token)
  {
    SetupPagedAggregator([], []);

    await Assert.ThrowsAsync<BadInputException>(() => _client.GetMessageListAsync(
      null, null, null, null, null, 5, token, CancellationToken.None));
  }

  [Theory]
  [InlineData("plain", "1", "with:colon", "-1")]
  [InlineData("back\\slash", "0", "", "3")]
  public void Token_JoinThenSplit_RoundTrips(params string[] fragments)
  {
    var joined = AggregatorClient.JoinToken(fragments);

    Assert.Equal(fragments, AggregatorClient.SplitToken(joined));
  }
}
