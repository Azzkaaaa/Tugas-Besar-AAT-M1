using BNPB.Notification.Server.Adapters.Outbound.Persistence;

namespace BNPB.Notification.Server.Adapters.Inbound;

// Established outside the gRPC call handler. The mutable holder flows back to
// serialization; setting AsyncLocal only inside a handler would not do that.
internal sealed class NotificationReadScope : IDisposable
{
  private static readonly AsyncLocal<NotificationReadScope?> Ambient = new();
  private readonly NotificationReadScope? _prior;
  public static NotificationReadScope? Current => Ambient.Value;
  public ReadCredit? Credit { get; private set; }
  public NotificationReadScope() { _prior = Ambient.Value; Ambient.Value = this; }
  public static void Admit(NotificationReadBudget budget)
  {
    var scope = Current ?? throw new InvalidOperationException("Notification read lifetime middleware is required");
    scope.Credit ??= budget.AcquireFull();
  }
  public void Dispose()
  {
    Ambient.Value = _prior;
    Credit?.Dispose();
  }
}

internal sealed class NotificationReadLifetime(RequestDelegate next)
{
  public async Task InvokeAsync(HttpContext context)
  {
    using var scope = new NotificationReadScope();
    await next(context);
  }
}
