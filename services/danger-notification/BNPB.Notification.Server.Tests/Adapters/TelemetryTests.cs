using System.Diagnostics;
using System.Net;
using System.Text;
using Google.Apis.Http;
using OpenTelemetry;
using OpenTelemetry.Trace;
using BNPB.Notification.Server.Adapters.Telemetry;

namespace BNPB.Notification.Server.Tests.Adapters;

public class TelemetryTests
{
  private const string TraceParent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";

  [Fact]
  public void ExtractAmqpContext_ReadsTraceContextAndBaggage()
  {
    // RabbitMQ.Client delivers string headers as byte[].
    var headers = new Dictionary<string, object?>
    {
      ["traceparent"] = Encoding.UTF8.GetBytes(TraceParent),
      ["baggage"] = Encoding.UTF8.GetBytes("hazard.source=BMKG"),
    };

    var context = NotificationTelemetry.ExtractAmqpContext(headers);

    Assert.Equal("0af7651916cd43dd8448eb211c80319c", context.ActivityContext.TraceId.ToHexString());
    Assert.True(context.ActivityContext.IsRemote);
    Assert.Equal("BMKG", context.Baggage.GetBaggage("hazard.source"));
  }

  [Fact]
  public void ExtractAmqpContext_MalformedHeadersGiveEmptyContext()
  {
    var headers = new Dictionary<string, object?> { ["traceparent"] = "00-not-a-trace-01" };

    Assert.Equal(default, NotificationTelemetry.ExtractAmqpContext(headers).ActivityContext);
    Assert.Equal(default, NotificationTelemetry.ExtractAmqpContext(null).ActivityContext);
  }

  [Fact]
  public async Task ThirdPartyHttpClient_RecordsClientSpanWithoutTraceContext()
  {
    var port = FreePort();
    using var listener = new HttpListener();
    listener.Prefixes.Add($"http://127.0.0.1:{port}/");
    listener.Start();
    var received = Task.Run(async () =>
    {
      var context = await listener.GetContextAsync();
      var headers = context.Request.Headers;
      context.Response.StatusCode = 200;
      context.Response.Close();
      return headers;
    });

    var spans = new List<Activity>();
    using var source = new ActivitySource("test");
    using (Sdk.CreateTracerProviderBuilder()
      .AddSource("test", NotificationTelemetry.Name)
      .AddHttpClientInstrumentation()
      .AddProcessor(new CollectingProcessor(spans))
      .Build())
    {
      using var parent = source.StartActivity("fcm.send");
      Baggage.SetBaggage("hazard.source", "BMKG");
      using var client = new ThirdPartyHttpClientFactory().CreateHttpClient(new CreateHttpClientArgs());

      using var response = await client.GetAsync($"http://127.0.0.1:{port}/v1/send?key=secret");
      Assert.Equal(HttpStatusCode.OK, response.StatusCode);
    }

    var headers = await received;
    Assert.Null(headers["traceparent"]);
    Assert.Null(headers["tracestate"]);
    Assert.Null(headers["baggage"]);

    var span = Assert.Single(spans, activity => activity.Kind == ActivityKind.Client);
    Assert.Equal("127.0.0.1", span.GetTagItem("server.address"));
    Assert.Equal($"http://127.0.0.1:{port}/v1/send", span.GetTagItem("url.full"));
    Assert.Equal(200, span.GetTagItem("http.response.status_code"));
  }

  private static int FreePort()
  {
    using var socket = new System.Net.Sockets.TcpListener(IPAddress.Loopback, 0);
    socket.Start();
    return ((IPEndPoint)socket.LocalEndpoint).Port;
  }

  private sealed class CollectingProcessor(List<Activity> sink) : BaseProcessor<Activity>
  {
    public override void OnEnd(Activity activity)
    {
      lock (sink)
        sink.Add(activity);
    }
  }
}
