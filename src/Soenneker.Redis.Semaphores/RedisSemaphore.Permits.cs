using System;
using System.Threading;
using System.Threading.Tasks;
using Soenneker.Redis.Util.Atomics;
using StackExchange.Redis;

namespace Soenneker.Redis.Semaphores;

public sealed partial class RedisSemaphore
{
    /// <summary>Tries to acquire an explicitly managed permit on the selected database, without automatic renewal.
    /// The fully qualified prefix may include a cluster hash tag. All callers must agree on maxCount;
    /// coordinate limit changes with dependent state. Transport failures propagate and may leave a permit until expiry.</summary>
    public static async Task<RedisSemaphorePermit?> TryAcquirePermit(IDatabase database, string keyPrefix, int maxCount,
        TimeSpan leaseDuration, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(keyPrefix);
        if (maxCount < 1 || leaseDuration < TimeSpan.FromMilliseconds(1)) throw new ArgumentOutOfRangeException(nameof(maxCount));
        string token = Guid.NewGuid().ToString("N");
        int start = System.Random.Shared.Next(maxCount);
        for (int offset = 0; offset < maxCount; offset++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            int slot = (int)(((long)start + offset) % maxCount);
            string key = $"{keyPrefix}:semaphore:{slot}";
            if (await database.StringSetAsync(key, token, leaseDuration, When.NotExists).ConfigureAwait(false))
                return new RedisSemaphorePermit(key, token);
        }
        return null;
    }

    /// <summary>Renews an explicitly managed permit if it is still owned. Returns false after ownership is lost.</summary>
    public static Task<bool> RenewPermit(IDatabase database, RedisSemaphorePermit permit, TimeSpan duration,
        CancellationToken cancellationToken = default) => RedisAtomics.CompareExpire(database, permit.Key, permit.Token, duration, cancellationToken);

    /// <summary>Releases an explicitly managed permit without deleting a successor's permit.</summary>
    public static Task<bool> ReleasePermit(IDatabase database, RedisSemaphorePermit permit,
        CancellationToken cancellationToken = default) => RedisAtomics.CompareDelete(database, permit.Key, permit.Token, cancellationToken);
}
