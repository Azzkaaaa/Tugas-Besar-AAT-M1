using BNPB.Notification.Server.Core.Models;

namespace BNPB.Notification.Server.Core.Ports.Outbound;

public interface ISourceProvider
{
  public Task<IReadOnlyList<SourceFreshness>> GetSourceFreshnessAsync()
    => GetSourceFreshnessAsync(CancellationToken.None);

  public Task<IReadOnlyList<SourceFreshness>> GetSourceFreshnessAsync(
    CancellationToken cancellationToken);
}
