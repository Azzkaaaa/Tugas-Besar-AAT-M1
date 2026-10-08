using BNPB.Notification.Server.Core.Models;

namespace BNPB.Notification.Server.Core.Services;

using IInboundSourceProvider = BNPB.Notification.Server.Core.Ports.Inbound.ISourceProvider;
using IOutboundSourceProvider = BNPB.Notification.Server.Core.Ports.Outbound.ISourceProvider;

public class SourceService : IInboundSourceProvider
{
  private readonly ILogger<SourceService> _logger;
  private readonly IOutboundSourceProvider _sourceProvider;

  public SourceService(
    ILogger<SourceService> logger,
    IOutboundSourceProvider sourceProvider)
  {
    _logger = logger;
    _sourceProvider = sourceProvider;
  }

  public async Task<IReadOnlyList<SourceFreshness>> GetSourceFreshnessAsync(
    CancellationToken cancellationToken)
  {
    return await _sourceProvider.GetSourceFreshnessAsync(cancellationToken);
  }
}
