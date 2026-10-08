using FirebaseAdmin;
using FirebaseAdmin.Messaging;
using Google.Apis.Auth.OAuth2;
using BNPB.Aggregator.V1;
using BNPB.Common.V1;
using BNPB.Notification.Server.Adapters.Inbound;
using BNPB.Notification.Server.Adapters.Outbound;
using BNPB.Notification.Server.Core.Services;
using BNPB.Notification.Server.Core.Ports.Inbound;
using BNPB.Notification.Server.Core.Ports.Outbound;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.Extensions.Diagnostics.HealthChecks;

using IInboundMessageProvider = BNPB.Notification.Server.Core.Ports.Inbound.IMessageProvider;
using IInboundSourceProvider = BNPB.Notification.Server.Core.Ports.Inbound.ISourceProvider;
using IOutboundSourceProvider = BNPB.Notification.Server.Core.Ports.Outbound.ISourceProvider;

var builder = WebApplication.CreateBuilder(args);

// Push
builder.Services.AddSingleton(_ =>
{
  if (FirebaseApp.DefaultInstance == null)
    FirebaseApp.Create(new AppOptions { Credential = GoogleCredential.GetApplicationDefault() });

  return FirebaseMessaging.DefaultInstance;
});
builder.Services.AddSingleton<IPushProvider, FcmClient>();

// Aggregator
builder.Services.AddHttpContextAccessor();
builder.Services
  .AddGrpcClient<AggregatorService.AggregatorServiceClient>(options =>
  {
    options.Address = new Uri(builder.Configuration["Aggregator:Address"]!);
  })
  .AddCallCredentials((_, metadata, serviceProvider) =>
  {
    var httpContext = serviceProvider.GetRequiredService<IHttpContextAccessor>().HttpContext;
    var authorization = httpContext?.Request.Headers.Authorization.ToString();

    if (!string.IsNullOrEmpty(authorization))
      metadata.Add("authorization", authorization);

    return Task.CompletedTask;
  })
  .EnableCallContextPropagation(options => options.SuppressContextNotFoundErrors = true)
  .ConfigureChannel(options => options.UnsafeUseInsecureChannelCallCredentials = true);

builder.Services.AddScoped<IOutboundSourceProvider>(sp =>
  ActivatorUtilities.CreateInstance<AggregatorClient>(sp, View.Summary));

// Core
builder.Services.AddScoped<IInboundMessageProvider>(sp =>
{
  var full = ActivatorUtilities.CreateInstance<AggregatorClient>(sp, View.Full);
  var summary = ActivatorUtilities.CreateInstance<AggregatorClient>(sp, View.Summary);

  return new MessageService(
    sp.GetRequiredService<ILogger<MessageService>>(),
    full,
    full,
    summary,
    summary);
});
builder.Services.AddScoped<IInboundSourceProvider, SourceService>();
builder.Services.AddScoped<ISubscriptionProvider, SubscriptionService>();
builder.Services.AddSingleton<IBroadcastProvider, BroadcastService>();

// Broker
builder.Services.AddHostedService<RabbitMqConsumer>();

// gRPC
builder.Services.AddGrpc();
builder.Services
  .AddGrpcHealthChecks()
  .AddCheck("self", () => HealthCheckResult.Healthy());

// Auth
builder.Services.AddAuthorizationBuilder()
  .SetDefaultPolicy(new AuthorizationPolicyBuilder()
    .RequireAuthenticatedUser()
    .Build());

builder.Services
  .AddAuthentication(JwtBearerDefaults.AuthenticationScheme)
  .AddJwtBearer(options =>
  {
    var authority = builder.Configuration["Auth:Authority"];

    if (!string.IsNullOrEmpty(authority))
      options.Authority = authority;

    options.RequireHttpsMetadata = builder.Configuration.GetValue("Auth:RequireHttpsMetadata", true);
    options.MapInboundClaims = false;
    options.TokenValidationParameters.ClockSkew = TimeSpan.FromSeconds(1);
    options.TokenValidationParameters.ValidateAudience = false;
  });

var app = builder.Build();

app.UseAuthentication();
app.UseAuthorization();

app.MapGrpcService<NotificationV1GrpcService>();
app.MapGrpcHealthChecksService().AllowAnonymous();
app.MapGet("/", () => "Communication with gRPC endpoints must be made through a gRPC client. To learn how to create a client, visit: https://go.microsoft.com/fwlink/?linkid=2086909");

app.Run();

public partial class Program;
