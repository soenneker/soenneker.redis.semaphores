using System;
using System.Linq;
using System.Threading.Tasks;
using StackExchange.Redis;
using System.Threading;

namespace Soenneker.Redis.Semaphores.Tests;

public sealed class RedisSemaphorePermitTests
{
    [Test]
    public async ValueTask ExplicitPermitsEnforceCapacityAndFenceSuccessors(CancellationToken cancellationToken)
    {
        using var connection = await ConnectionMultiplexer.ConnectAsync(Environment.GetEnvironmentVariable("REDIS_TEST_CONNECTION") ?? Environment.GetEnvironmentVariable("FLYWHEEL_TEST_REDIS") ?? "localhost:6379");
        var db = connection.GetDatabase();
        string prefix = "permits:{" + Guid.NewGuid().ToString("N") + "}";
        try
        {
            var permits = await Task.WhenAll(Enumerable.Range(0, 12).Select(_ => RedisSemaphore.TryAcquirePermit(db, prefix, 2, TimeSpan.FromSeconds(10), cancellationToken: cancellationToken)));
            if (permits.Count(p => p is not null) != 2) throw new Exception("Capacity exceeded");
            foreach (var permit in permits.Where(p => p is not null))
            {
                if (!await RedisSemaphore.RenewPermit(db, permit!, TimeSpan.FromSeconds(20), cancellationToken: cancellationToken)) throw new Exception("Renewal failed");
                if (!await RedisSemaphore.ReleasePermit(db, permit!, cancellationToken: cancellationToken)) throw new Exception("Release failed");
            }
            var old = (await RedisSemaphore.TryAcquirePermit(db, prefix, 1, TimeSpan.FromMilliseconds(40), cancellationToken: cancellationToken))!;
            await Task.Delay(80, cancellationToken: cancellationToken);
            var current = (await RedisSemaphore.TryAcquirePermit(db, prefix, 1, TimeSpan.FromSeconds(10), cancellationToken: cancellationToken))!;
            if (current is null || await RedisSemaphore.RenewPermit(db, old, TimeSpan.FromSeconds(10), cancellationToken: cancellationToken) || await RedisSemaphore.ReleasePermit(db, old, cancellationToken: cancellationToken))
                throw new Exception("Stale owner changed successor permit");
            if (await db.StringGetAsync(current.Key) != current.Token) throw new Exception("Successor was lost");
        }
        finally
        {
            await db.KeyDeleteAsync(new RedisKey[] { prefix + ":semaphore:0", prefix + ":semaphore:1" });
        }
    }
}
