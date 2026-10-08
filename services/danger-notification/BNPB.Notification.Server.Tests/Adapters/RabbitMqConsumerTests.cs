using Google.Protobuf;
using BNPB.Event.V1;
using BNPB.Notification.Server.Adapters.Inbound;

namespace BNPB.Notification.Server.Tests.Adapters;

public class RabbitMqConsumerTests
{
  // A truncated length-delimited field, as a broker would deliver a corrupt body.
  private static InvalidProtocolBufferException ParseError()
    => Assert.Throws<InvalidProtocolBufferException>(
      () => HazardEventPublished.Parser.ParseFrom([0x0A, 0x05, 0x01]));

  [Theory]
  [InlineData(false)]
  [InlineData(true)]
  public void ShouldRequeue_ParseError_DeadLetters(bool redelivered)
    => Assert.False(RabbitMqConsumer.ShouldRequeue(
      ParseError(),
      redelivered));

  [Fact]
  public void ShouldRequeue_FirstFailure_Requeues()
    => Assert.True(RabbitMqConsumer.ShouldRequeue(new InvalidOperationException(), redelivered: false));

  [Fact]
  public void ShouldRequeue_RedeliveredFailure_DeadLetters()
    => Assert.False(RabbitMqConsumer.ShouldRequeue(new InvalidOperationException(), redelivered: true));
}
