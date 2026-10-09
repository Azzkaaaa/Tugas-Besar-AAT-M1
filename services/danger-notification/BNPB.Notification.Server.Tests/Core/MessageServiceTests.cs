using Microsoft.Extensions.Logging.Abstractions;
using BNPB.Notification.Server.Core.Models;
using BNPB.Notification.Server.Core.Services;
using BNPB.Notification.Server.Core.Exceptions;
using BNPB.Notification.Server.Core.Ports.Outbound;

namespace BNPB.Notification.Server.Tests.Core;

public class MessageServiceTests
{
  private static readonly UserPermission[] SummaryScopes =
    [UserPermission.ReadNotifications, UserPermission.ReadHazardsSummary];

  private static readonly UserPermission[] FullScopes =
    [UserPermission.ReadNotifications, UserPermission.ReadHazardsSummary, UserPermission.ReadHazardsFull];

  private readonly IMessageProvider _full = Substitute.For<IMessageProvider>();
  private readonly IMessageListProvider _fullList = Substitute.For<IMessageListProvider>();
  private readonly IMessageProvider _summary = Substitute.For<IMessageProvider>();
  private readonly IMessageListProvider _summaryList = Substitute.For<IMessageListProvider>();
  private readonly MessageService _service;

  public MessageServiceTests()
  {
    _service = new MessageService(
      NullLogger<MessageService>.Instance,
      _full,
      _fullList,
      _summary,
      _summaryList);

    _full.GetMessageAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
      .Returns(call => TestData.Notification(call.Arg<string>()));
    _summary.GetMessageAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
      .Returns(call => TestData.Notification(call.Arg<string>()));

    _fullList.GetMessageListAsync(default, default, default, default, default, default, default, default)
      .ReturnsForAnyArgs((new List<HazardNotification> { TestData.Notification("f") }, "next"));
    _summaryList.GetMessageListAsync(default, default, default, default, default, default, default, default)
      .ReturnsForAnyArgs((new List<HazardNotification> { TestData.Notification("s") }, "next"));
  }

  [Theory]
  [InlineData(HazardView.Summary)]
  [InlineData(HazardView.Unknown)]
  public async Task GetMessage_SummaryOrUnspecifiedView_UsesSummaryProvider(HazardView view)
  {
    var result = await _service.GetMessageAsync("h1", view, SummaryScopes, CancellationToken.None);

    Assert.NotNull(result);
    await _summary.Received(1).GetMessageAsync("h1", Arg.Any<CancellationToken>());
    await _full.DidNotReceiveWithAnyArgs().GetMessageAsync(default!, default);
  }

  [Fact]
  public async Task GetMessage_FullViewWithFullScope_UsesFullProvider()
  {
    await _service.GetMessageAsync("h1", HazardView.Full, FullScopes, CancellationToken.None);

    await _full.Received(1).GetMessageAsync("h1", Arg.Any<CancellationToken>());
    await _summary.DidNotReceiveWithAnyArgs().GetMessageAsync(default!, default);
  }

  [Fact]
  public async Task GetMessage_FullViewWithoutFullScope_IsUnauthorized()
  {
    await Assert.ThrowsAsync<UnauthorizedException>(
      () => _service.GetMessageAsync("h1", HazardView.Full, SummaryScopes, CancellationToken.None));
  }

  [Theory]
  [InlineData(UserPermission.ReadHazardsSummary, UserPermission.ReadHazardsFull)]
  [InlineData(UserPermission.ReadNotifications, UserPermission.ReadHazardsFull)]
  public async Task GetMessage_MissingRequiredScope_IsUnauthorized(params UserPermission[] scopes)
  {
    await Assert.ThrowsAsync<UnauthorizedException>(
      () => _service.GetMessageAsync("h1", HazardView.Summary, scopes, CancellationToken.None));
  }

  [Fact]
  public async Task GetMessage_EmptyId_IsBadInput()
  {
    await Assert.ThrowsAsync<BadInputException>(
      () => _service.GetMessageAsync("", HazardView.Summary, SummaryScopes, CancellationToken.None));
  }

  [Fact]
  public async Task GetMessage_NotFound_ReturnsNull()
  {
    _summary.GetMessageAsync("missing", Arg.Any<CancellationToken>()).Returns((HazardNotification?)null);

    var result = await _service.GetMessageAsync("missing", HazardView.Summary, SummaryScopes, CancellationToken.None);

    Assert.Null(result);
  }

  [Fact]
  public async Task GetMessage_AlertBuiltFromSummaryFieldsOnly()
  {
    var full = TestData.Full("h1");
    _full.GetMessageAsync("h1", Arg.Any<CancellationToken>()).Returns(TestData.Notification("h1", extra: full));

    var result = await _service.GetMessageAsync("h1", HazardView.Full, FullScopes, CancellationToken.None);

    var alert = result!.Value.Alert;
    Assert.Contains("AWAS", alert);
    Assert.Contains("Area h1", alert);
    Assert.Contains("BMKG", alert);
    Assert.DoesNotContain(full.SourceRefId, alert);
    Assert.DoesNotContain("-6.2", alert);
  }

  [Fact]
  public async Task GetMessageList_RoutesByView()
  {
    var (summaryItems, _) = await _service.GetMessageListAsync(
      HazardView.Summary, null, null, null, null, null, 10, null, FullScopes, CancellationToken.None);
    var (fullItems, _) = await _service.GetMessageListAsync(
      HazardView.Full, null, null, null, null, null, 10, null, FullScopes, CancellationToken.None);

    Assert.Equal("s", Assert.Single(summaryItems).Id);
    Assert.Equal("f", Assert.Single(fullItems).Id);
    Assert.All(summaryItems.Concat(fullItems), item => Assert.NotEmpty(item.Alert));
  }

  [Fact]
  public async Task GetMessageList_ZeroPageSize_DefaultsTo20()
  {
    await _service.GetMessageListAsync(
      HazardView.Summary, null, null, null, null, null, 0, null, SummaryScopes, CancellationToken.None);

    await _summaryList.Received(1).GetMessageListAsync(
      null, null, null, null, null, MessageService.DefaultPageSize, null, Arg.Any<CancellationToken>());
  }

  [Theory]
  [InlineData(-1)]
  [InlineData(101)]
  public async Task GetMessageList_OutOfRangePageSize_IsBadInput(int pageSize)
  {
    await Assert.ThrowsAsync<BadInputException>(() => _service.GetMessageListAsync(
      HazardView.Summary, null, null, null, null, null, pageSize, null, SummaryScopes, CancellationToken.None));
  }

  [Fact]
  public async Task GetMessageList_SinceNotBeforeUntil_IsBadInput()
  {
    var time = TestData.BaseTime;

    await Assert.ThrowsAsync<BadInputException>(() => _service.GetMessageListAsync(
      HazardView.Summary, null, null, null, time, time, 10, null, SummaryScopes, CancellationToken.None));
  }

  [Fact]
  public async Task GetMessageList_WithoutNotificationScope_IsUnauthorized()
  {
    await Assert.ThrowsAsync<UnauthorizedException>(() => _service.GetMessageListAsync(
      HazardView.Summary, null, null, null, null, null, 10, null,
      [UserPermission.ReadHazardsSummary], CancellationToken.None));
  }
}
