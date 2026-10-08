using System.Diagnostics;
using System.Text;
using BNPB.Event.V1;
using Google.Protobuf;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using BNPB.Notification.Server.Core.Models;
using BNPB.Notification.Server.Core.Ports.Inbound;
using BNPB.Notification.Server.Adapters.Telemetry;

namespace BNPB.Notification.Server.Adapters.Inbound;

using ExternalSource = BNPB.Common.V1.Source;
using ExternalType = BNPB.Common.V1.HazardType;
using ExternalSeverity = BNPB.Common.V1.Severity;
using ExternalHazardEvent = BNPB.Common.V1.HazardEvent;

public class RabbitMqConsumer : BackgroundService
{
  private readonly ILogger<RabbitMqConsumer> _logger;
  private readonly IBroadcastProvider _broadcastProvider;
  private readonly IConfiguration _configuration;
  private readonly string _queueName;
  private readonly string? _deadLetterQueue;

  // Make sure these are initialized in InitializeRabbitMqListener()
  private IConnection _connection = default!;
  private IChannel _channel = default!;

  public RabbitMqConsumer(
    ILogger<RabbitMqConsumer> logger,
    IBroadcastProvider broadcastProvider,
    IConfiguration configuration)
  {
    _logger = logger;
    _broadcastProvider = broadcastProvider;
    _configuration = configuration;
    _queueName = _configuration["RabbitMq:QueueName"]!;
    _deadLetterQueue = _configuration["RabbitMq:DeadLetterQueue"];
  }

  // Parse failures never succeed on retry. Anything else gets one redelivery
  // before it is dead-lettered, so a persistent failure cannot loop forever.
  internal static bool ShouldRequeue(Exception ex, bool redelivered)
    => ex is not InvalidProtocolBufferException && !redelivered;

  private async Task InitializeRabbitMqListener()
  {
    var factory = new ConnectionFactory
    {
      HostName = _configuration["RabbitMq:HostName"]!,
      UserName = _configuration["RabbitMq:UserName"]!,
      Password = _configuration["RabbitMq:Password"]!,
      Port = _configuration.GetValue("RabbitMq:Port", AmqpTcpEndpoint.UseDefaultPort),
      VirtualHost = _configuration.GetValue("RabbitMq:VirtualHost", ConnectionFactory.DefaultVHost)!,
    };

    _connection = await factory.CreateConnectionAsync();
    _channel = await _connection.CreateChannelAsync();

    // Must match the arguments the aggregator declares this queue with,
    // otherwise whichever side declares second fails with PRECONDITION_FAILED.
    Dictionary<string, object?>? arguments = null;

    if (!string.IsNullOrEmpty(_deadLetterQueue))
    {
      arguments = new()
      {
        ["x-dead-letter-exchange"] = "",
        ["x-dead-letter-routing-key"] = _deadLetterQueue,
      };
    }
    else
    {
      _logger.LogWarning("RabbitMq:DeadLetterQueue is not set; failed messages will be dropped");
    }

    await _channel.QueueDeclareAsync(
        queue: _queueName,
        durable: true,
        exclusive: false,
        autoDelete: false,
        arguments: arguments);

    await _channel.BasicQosAsync(prefetchSize: 0, prefetchCount: 1, global: false);
  }

  public override async Task StartAsync(CancellationToken stoppingToken)
  {
    await InitializeRabbitMqListener();
    await base.StartAsync(stoppingToken);
  }

  protected override async Task ExecuteAsync(CancellationToken stoppingToken)
  {
    stoppingToken.ThrowIfCancellationRequested();

    var consumer = new AsyncEventingBasicConsumer(_channel);

    consumer.ReceivedAsync += async (model, ea) =>
    {
      var body = ea.Body.ToArray();
      string? eventId = null;

      using var activity = NotificationTelemetry.Source.StartActivity(
        "rabbitmq.consume",
        ActivityKind.Consumer,
        NotificationTelemetry.ExtractAmqpParent(ea.BasicProperties.Headers));

      activity?.SetTag("messaging.system", "rabbitmq");
      activity?.SetTag("messaging.destination", _queueName);
      activity?.SetTag("messaging.message_id", ea.BasicProperties.MessageId);

      try
      {
        _logger.LogInformation("Processing message");

        var externalEvent = HazardEventPublished.Parser.ParseFrom(body);
        eventId = externalEvent.EventId;

        activity?.SetTag("event.id", eventId);
        activity?.SetTag("hazard.id", externalEvent.Payload.HazardId);
        activity?.SetTag("hazard.source", externalEvent.Payload.Source.ToString());
        activity?.SetTag("hazard.severity", externalEvent.Payload.Severity.ToString());

        HazardSource source = externalEvent.Payload.Source switch
        {
          ExternalSource.Bmkg => HazardSource.BMKG,
          ExternalSource.Pvmbg => HazardSource.PVMBG,
          _ => HazardSource.Unknown,
        };

        HazardType type = externalEvent.Payload.HazardType switch
        {
          ExternalType.Seismic => HazardType.Seismic,
          ExternalType.Volcanic => HazardType.Volcanic,
          _ => HazardType.Unknown,
        };

        HazardSeverity severity = externalEvent.Payload.Severity switch
        {
          ExternalSeverity.Siaga => HazardSeverity.Siaga,
          ExternalSeverity.Awas => HazardSeverity.Awas,
          _ => HazardSeverity.Other,
        };

        var extraDataBuilder = ImmutableDictionary.CreateBuilder<Type, object>();
        extraDataBuilder.Add(typeof(HazardEventPublished), externalEvent);
        extraDataBuilder.Add(typeof(ExternalHazardEvent), externalEvent.Payload);

        var internalEvent = new HazardEvent(
          externalEvent.Payload.HazardId,
          source,
          type,
          severity,
          externalEvent.Payload.AreaName,
          externalEvent.Payload.OccurredAt.ToDateTimeOffset(),
          "",
          extraDataBuilder.ToImmutable()
        );

        await _broadcastProvider.FilterAndBroadcastAsync(internalEvent);

        await _channel.BasicAckAsync(deliveryTag: ea.DeliveryTag, multiple: false);

        NotificationTelemetry.EventsConsumed.Add(1, new KeyValuePair<string, object?>("outcome", "acked"));
      }
      catch (Exception ex)
      {
        var requeue = ShouldRequeue(ex, ea.Redelivered);

        activity?.AddException(ex);
        activity?.SetStatus(ActivityStatusCode.Error, ex.Message);

        _logger.LogError(ex,
          "Error processing message {EventId}; {Outcome}",
          eventId,
          requeue ? "requeued" : "dead-lettered");

        await _channel.BasicNackAsync(deliveryTag: ea.DeliveryTag, multiple: false, requeue: requeue);

        NotificationTelemetry.EventsConsumed.Add(1,
          new KeyValuePair<string, object?>("outcome", requeue ? "requeued" : "dead_lettered"));
      }
    };

    await _channel.BasicConsumeAsync(
      queue: _queueName,
      autoAck: false,
      consumer: consumer,
      cancellationToken: stoppingToken);
  }

  public override void Dispose()
  {
    _logger.LogInformation("Closing RabbitMQ connection");
    _channel?.CloseAsync();
    _channel?.Dispose();
    _connection?.CloseAsync();
    _connection?.Dispose();
    GC.SuppressFinalize(this);
  }
}
