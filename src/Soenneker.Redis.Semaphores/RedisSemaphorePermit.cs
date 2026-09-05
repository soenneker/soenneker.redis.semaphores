using StackExchange.Redis;
using System.Text.Json.Serialization;

namespace Soenneker.Redis.Semaphores;

/// <summary>A persisted permit whose owner renews or releases it explicitly. It does not run a background timer.</summary>
/// <param name="Key">Fully qualified Redis permit key; keep it in the same cluster slot as dependent state.</param>
/// <param name="Token">Unique owner token used for conditional renewal and release.</param>
public sealed record RedisSemaphorePermit(string Key, string Token)
{
    /// <summary>Condition to include when committing state that depends on this permit still being owned.</summary>
    [JsonIgnore]
    public Condition OwnershipCondition => Condition.StringEqual(Key, Token);
}
