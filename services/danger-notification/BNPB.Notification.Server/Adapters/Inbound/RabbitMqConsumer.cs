using System.Text;
using BNPB.Event.V1;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using BNPB.Notification.Server.Core.Models;
using BNPB.Notification.Server.Core.Ports.Inbound;

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
  }

  private async Task InitializeRabbitMqListener()
  {
    var factory = new ConnectionFactory
    {
      HostName = _configuration["RabbitMq:HostName"]!,
      UserName = _configuration["RabbitMq:UserName"]!,
      Password = _configuration["RabbitMq:Password"]!,
    };

    _connection = await factory.CreateConnectionAsync();
    _channel = await _connection.CreateChannelAsync();

    await _channel.QueueDeclareAsync(
        queue: _queueName,
        durable: true,
        exclusive: false,
        autoDelete: false,
        arguments: null);

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

      try
      {
        _logger.LogInformation("Processing message");

        var externalEvent = HazardEventPublished.Parser.ParseFrom(body);

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
      }
      catch (Exception ex)
      {
        _logger.LogError(ex, "Error processing message");

        await _channel.BasicNackAsync(deliveryTag: ea.DeliveryTag, multiple: false, requeue: true);
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
