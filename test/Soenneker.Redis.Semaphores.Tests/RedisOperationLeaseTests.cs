using Soenneker.Redis.Semaphores;
using System;
using System.Threading;
using System.Threading.Tasks;
using AwesomeAssertions;
using Soenneker.Redis.Semaphores.Abstract;
using Soenneker.Redis.Util.Abstract;

namespace Soenneker.Redis.Semaphores.Tests;

public sealed class RedisOperationLeaseTests
{
    [Test]
    public async Task Contender_waits_until_owner_releases_and_then_owns_the_lock()
    {
        var locks = new RedisSemaphore(new Microsoft.Extensions.Configuration.ConfigurationBuilder().Build(),
            System.Reflection.DispatchProxy.Create<IRedisUtil, RedisUtilProxy>(),
            Microsoft.Extensions.Logging.Abstractions.NullLogger<RedisSemaphore>.Instance);
        string key = $"lock-wait-test:{Guid.NewGuid()}";
        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        Task<RedisOperationLease> pending;
        await using (var owner = await RedisOperationLease.TryAcquire(locks, key, TimeSpan.FromMinutes(1), CancellationToken.None))
        {
            owner.Should().NotBeNull();
            pending = RedisOperationLease.Acquire(locks, key, TimeSpan.FromMinutes(1), cancellation.Token).AsTask();
            await Task.Delay(300);
            pending.IsCompleted.Should().BeFalse();
        }
        await using var acquired = await pending;
        await using var competitor = await RedisOperationLease.TryAcquire(locks, key, TimeSpan.FromMinutes(1), CancellationToken.None);
        competitor.Should().BeNull();
    }

    [Test]
    public async Task Cancellation_stops_waiting_without_releasing_another_owners_lock()
    {
        var locks = new RedisSemaphore(new Microsoft.Extensions.Configuration.ConfigurationBuilder().Build(),
            System.Reflection.DispatchProxy.Create<IRedisUtil, RedisUtilProxy>(),
            Microsoft.Extensions.Logging.Abstractions.NullLogger<RedisSemaphore>.Instance);
        string key = $"lock-wait-test:{Guid.NewGuid()}";
        await using var owner = await RedisOperationLease.TryAcquire(locks, key, TimeSpan.FromMinutes(1), CancellationToken.None);
        owner.Should().NotBeNull();
        using var cancellation = new CancellationTokenSource();
        Task<RedisOperationLease> pending = RedisOperationLease.Acquire(locks, key, TimeSpan.FromMinutes(1), cancellation.Token).AsTask();
        cancellation.Cancel();
        Func<Task> wait = async () => { await using var acquired = await pending; };
        await wait.Should().ThrowAsync<OperationCanceledException>();
        await using var competitor = await RedisOperationLease.TryAcquire(locks, key, TimeSpan.FromMinutes(1), CancellationToken.None);
        competitor.Should().BeNull();
    }

    [Test]
    public async Task Deadline_ends_a_stuck_wait_without_releasing_another_owners_lock()
    {
        var locks = new RedisSemaphore(new Microsoft.Extensions.Configuration.ConfigurationBuilder().Build(),
            System.Reflection.DispatchProxy.Create<IRedisUtil, RedisUtilProxy>(),
            Microsoft.Extensions.Logging.Abstractions.NullLogger<RedisSemaphore>.Instance);
        string key = $"lock-wait-test:{Guid.NewGuid()}";
        await using var owner = await RedisOperationLease.TryAcquire(locks, key, TimeSpan.FromMinutes(1), CancellationToken.None);
        owner.Should().NotBeNull();
        Func<Task> wait = async () =>
        {
            await using var acquired = await RedisOperationLease.Acquire(locks, key, TimeSpan.FromMinutes(1),
                CancellationToken.None, TimeSpan.FromMilliseconds(300));
        };
        await wait.Should().ThrowAsync<TimeoutException>();
        await using var competitor = await RedisOperationLease.TryAcquire(locks, key, TimeSpan.FromMinutes(1), CancellationToken.None);
        competitor.Should().BeNull();
    }

    [Test]
    public async Task Renewing_lease_work_token_survives_acquisition_deadline_scope()
    {
        var locks = new RedisSemaphore(new Microsoft.Extensions.Configuration.ConfigurationBuilder().Build(),
            System.Reflection.DispatchProxy.Create<IRedisUtil, RedisUtilProxy>(),
            Microsoft.Extensions.Logging.Abstractions.NullLogger<RedisSemaphore>.Instance);
        string key = $"lock-wait-test:{Guid.NewGuid()}";
        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        Task<RedisOperationLease> pending;
        await using (var owner = await RedisOperationLease.TryAcquire(locks, key, TimeSpan.FromMinutes(1), CancellationToken.None))
        {
            owner.Should().NotBeNull();
            pending = RedisOperationLease.Acquire(locks, key, TimeSpan.FromSeconds(2), cancellation.Token, TimeSpan.FromSeconds(1)).AsTask();
            await Task.Delay(300);
            pending.IsCompleted.Should().BeFalse();
        }
        await using var lease = await pending;
        await Task.Delay(TimeSpan.FromSeconds(3), cancellation.Token);
        lease.Token.IsCancellationRequested.Should().BeFalse();
        await using var competitor = await RedisOperationLease.TryAcquire(locks, key, TimeSpan.FromMinutes(1), CancellationToken.None);
        competitor.Should().BeNull("the acquired lease must continue renewing ownership");
        cancellation.Cancel();
        lease.Token.IsCancellationRequested.Should().BeTrue();
    }
}
