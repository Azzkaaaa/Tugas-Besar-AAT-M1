using System.Collections.Immutable;
using System.Data.Common;
using System.Security.Claims;
using System.Text.Encodings.Web;
using System.Text.Json;
using BNPB.Common.V1;
using BNPB.Notification.Server.Adapters.Inbound;
using BNPB.Notification.Server.Adapters.Models;
using BNPB.Notification.Server.Adapters.Outbound.Persistence;
using BNPB.Notification.Server.Core.Services;
using Google.Protobuf.WellKnownTypes;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using InboundMessages = BNPB.Notification.Server.Core.Ports.Inbound.IMessageProvider;
using InboundSources = BNPB.Notification.Server.Core.Ports.Inbound.ISourceProvider;
using Subscriptions = BNPB.Notification.Server.Core.Ports.Inbound.ISubscriptionProvider;
using CoreFreshness = BNPB.Notification.Server.Core.Models.SourceFreshness;
using Type = System.Type;

internal static class RpcProbe
{
  public static async Task Run(string connection, OutboxOptions options)
  {
    var builder = WebApplication.CreateBuilder();
    builder.Logging.ClearProviders();
    builder.Configuration["Auth:ScopeClaim"] = "scope";
    var budget = new NotificationReadBudget();
    var reads = new BodyReads();
    var factory = new Factory(connection, reads);
    builder.Services.AddSingleton(budget);
    builder.AddNotificationReadBounds();
    builder.WebHost.ConfigureKestrel(kestrel => {
      kestrel.ListenAnyIP(19094, listen => listen.Protocols = HttpProtocols.Http2);
      kestrel.ListenAnyIP(19095, listen => listen.Protocols = HttpProtocols.Http1);
    });
    var full = new EfNotificationStore(Microsoft.Extensions.Logging.Abstractions.NullLogger<EfNotificationStore>.Instance,
      factory, new PushOutboxSignal(), options, TimeProvider.System, budget, View.Full);
    var summary = new EfNotificationStore(Microsoft.Extensions.Logging.Abstractions.NullLogger<EfNotificationStore>.Instance,
      factory, new PushOutboxSignal(), options, TimeProvider.System, budget, View.Summary);
    builder.Services.AddSingleton<InboundMessages>(new MessageService(
      Microsoft.Extensions.Logging.Abstractions.NullLogger<MessageService>.Instance, full, full, summary, summary));
    builder.Services.AddSingleton<InboundSources>(new Freshness());
    builder.Services.AddSingleton<Subscriptions>(new NoSubscriptions());
    builder.Services.AddAuthentication("fixture").AddScheme<AuthenticationSchemeOptions, FixtureAuth>("fixture", _ => { });
    builder.Services.AddAuthorization();
    await using var app = builder.Build();
    app.UseAuthentication();
    app.UseAuthorization();
    app.UseNotificationReadBounds();
    app.MapGrpcService<NotificationV1GrpcService>();
    app.MapGet("/fixture/stats", () => {
      var credit = budget.ActiveCredit;
      return new { references = credit?.References ?? 0, root_disposed = credit?.RootDisposed ?? true,
        body_reads = reads.Count, capacity = budget.LegacySummary.CurrentCount };
    });
    using var stopped = new CancellationTokenSource();
    var watcher = Task.Run(async () => {
      try
      {
        var until = DateTimeOffset.UtcNow.AddSeconds(120);
        while (!File.Exists("/tmp/m2-notification-rpc-stop") && DateTimeOffset.UtcNow < until)
          await Task.Delay(100, stopped.Token);
        app.Lifetime.StopApplication();
      }
      catch (OperationCanceledException) { }
    });
    app.Lifetime.ApplicationStarted.Register(() => Console.WriteLine("NOTIFICATION_RPC_READY grpc=19094 stats=19095"));
    Console.WriteLine(JsonSerializer.Serialize(new { phase = "before_rpc_host", heap_bytes = GC.GetTotalMemory(false),
      gc_committed = GC.GetGCMemoryInfo().TotalCommittedBytes,
      current_bytes = File.ReadAllText("/sys/fs/cgroup/memory.current").Trim(),
      memory_stat = File.ReadAllText("/sys/fs/cgroup/memory.stat") }));
    await app.RunAsync();
    stopped.Cancel();
    await watcher;
    Console.WriteLine(JsonSerializer.Serialize(new { mode = "rpc", body_reads = reads.Count,
      gc_available_bytes = GC.GetGCMemoryInfo().TotalAvailableMemoryBytes,
      server_gc = System.Runtime.GCSettings.IsServerGC,
      heap_bytes = GC.GetTotalMemory(false), gc_committed = GC.GetGCMemoryInfo().TotalCommittedBytes,
      memory_stat = File.ReadAllText("/sys/fs/cgroup/memory.stat"),
      peak_rss = File.ReadAllLines("/proc/self/status").First(l => l.StartsWith("VmHWM:")),
      cgroup_peak_bytes = File.ReadAllText("/sys/fs/cgroup/memory.peak").Trim(),
      memory_events = File.ReadAllText("/sys/fs/cgroup/memory.events") }));
  }

  private sealed class BodyReads : DbCommandInterceptor
  {
    private int _count;
    public int Count => Volatile.Read(ref _count);
    public override ValueTask<InterceptionResult<DbDataReader>> ReaderExecutingAsync(DbCommand command,
      CommandEventData eventData, InterceptionResult<DbDataReader> result, CancellationToken cancellationToken = default)
    {
      if (command.CommandText.StartsWith("SELECT") && command.CommandText.Split('\n')[0].TrimEnd().EndsWith(".\"Hazard\""))
        Interlocked.Increment(ref _count);
      return ValueTask.FromResult(result);
    }
  }
  private sealed class FixtureAuth(IOptionsMonitor<AuthenticationSchemeOptions> options, ILoggerFactory logger, UrlEncoder encoder)
    : AuthenticationHandler<AuthenticationSchemeOptions>(options, logger, encoder)
  {
    protected override Task<AuthenticateResult> HandleAuthenticateAsync()
    {
      var token = Request.Headers.Authorization.ToString();
      if (token is not ("Bearer field" or "Bearer media")) return Task.FromResult(AuthenticateResult.NoResult());
      var scopes = "notifications:read hazards:read:summary" + (token == "Bearer field" ? " hazards:read:full" : "");
      return Task.FromResult(AuthenticateResult.Success(new AuthenticationTicket(
        new ClaimsPrincipal(new ClaimsIdentity([new Claim("scope", scopes)], "fixture")), "fixture")));
    }
  }
  private sealed class Freshness : InboundSources
  {
    public Task<IReadOnlyList<CoreFreshness>> GetSourceFreshnessAsync(CancellationToken cancellationToken)
      => Task.FromResult<IReadOnlyList<CoreFreshness>>(new[] { Source.Bmkg, Source.Pvmbg, Source.Satelite, Source.CoastSensor }.Select(source =>
        new CoreFreshness(ImmutableDictionary<Type, object>.Empty.Add(typeof(SourceFreshness), new SourceFreshness {
          Source = source, Status = SourceStatus.Fresh, LastSuccessAt = Timestamp.FromDateTimeOffset(DateTimeOffset.UtcNow)
        }))).ToArray());
  }
  private sealed class NoSubscriptions : Subscriptions
  {
    public Task SubscribeAsync(string token, IEnumerable<BNPB.Notification.Server.Core.Models.UserPermission> permissions, CancellationToken cancellationToken)
      => Task.CompletedTask;
    public Task UnsubscribeAsync(string token, CancellationToken cancellationToken) => Task.CompletedTask;
  }
}
