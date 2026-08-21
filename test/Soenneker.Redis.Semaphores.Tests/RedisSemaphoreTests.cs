using System;
using System.Collections.Generic;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using AwesomeAssertions;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging.Abstractions;
using Soenneker.Asyncs.Locks;
using Soenneker.Extensions.ValueTask;
using Soenneker.Redis.Semaphores.Abstract;
using Soenneker.Redis.Util.Abstract;

namespace Soenneker.Redis.Semaphores.Tests;

public sealed class RedisSemaphoreTests
{
    private static readonly IConfiguration _config = new ConfigurationBuilder().Build();
    private readonly IRedisSemaphore _semaphore;

    public RedisSemaphoreTests()
    {
        _semaphore = new RedisSemaphore(_config, CreateRedisUtil(), NullLogger<RedisSemaphore>.Instance);
    }

    [Test]
    public async Task TryAcquire_should_enforce_max_count(CancellationToken cancellationToken)
    {
        string name = CreateName();

        RedisSemaphoreHandle? first = await _semaphore.TryAcquire(name, 2, cancellationToken: cancellationToken);
        RedisSemaphoreHandle? second = await _semaphore.TryAcquire(name, 2, cancellationToken: cancellationToken);
        RedisSemaphoreHandle? third = await _semaphore.TryAcquire(name, 2, cancellationToken: cancellationToken);

        first.Should().NotBeNull();
        second.Should().NotBeNull();
        third.Should().BeNull();
        (await _semaphore.GetAcquiredCount(name, 2, cancellationToken)).Should().Be(2);

        await first!.DisposeAsync();
        await second!.DisposeAsync();
    }

    [Test]
    public async Task Disposing_handle_should_release_permit(CancellationToken cancellationToken)
    {
        string name = CreateName();

        RedisSemaphoreHandle? first = await _semaphore.TryAcquire(name, 1, cancellationToken: cancellationToken);
        first.Should().NotBeNull();

        await first!.DisposeAsync();

        RedisSemaphoreHandle? replacement = await _semaphore.TryAcquire(name, 1, cancellationToken: cancellationToken);
        replacement.Should().NotBeNull();

        await replacement!.DisposeAsync();
    }

    [Test]
    public async Task Expired_handle_should_not_release_new_owner(CancellationToken cancellationToken)
    {
        string name = CreateName();

        var fixedLease = new RedisSemaphoreOptions {LeaseDuration = TimeSpan.FromMilliseconds(100), AutoRenew = false};
        RedisSemaphoreHandle? expired = await _semaphore.TryAcquire(name, 1, fixedLease, cancellationToken);
        expired.Should().NotBeNull();

        await Task.Delay(TimeSpan.FromMilliseconds(250), cancellationToken);

        RedisSemaphoreHandle? current = await _semaphore.TryAcquire(name, 1, cancellationToken: cancellationToken);
        current.Should().NotBeNull();

        await expired!.DisposeAsync();
        (await _semaphore.GetAcquiredCount(name, 1, cancellationToken)).Should().Be(1);

        await current!.DisposeAsync();
    }

    [Test]
    public async Task Acquire_should_wait_until_permit_is_released(CancellationToken cancellationToken)
    {
        string name = CreateName();

        RedisSemaphoreHandle? first = await _semaphore.TryAcquire(name, 1, cancellationToken: cancellationToken);
        first.Should().NotBeNull();

        var options = new RedisSemaphoreOptions {RetryInterval = TimeSpan.FromMilliseconds(10)};
        ValueTask<RedisSemaphoreHandle> pending = _semaphore.Acquire(name, 1, options, cancellationToken);

        await Task.Delay(TimeSpan.FromMilliseconds(50), cancellationToken);
        pending.IsCompleted.Should().BeFalse();

        await first!.DisposeAsync();

        RedisSemaphoreHandle replacement = await pending;
        replacement.Should().NotBeNull();
        await replacement.DisposeAsync();
    }

    [Test]
    public async Task ForceReleaseAll_should_clear_every_permit(CancellationToken cancellationToken)
    {
        string name = CreateName();

        RedisSemaphoreHandle? first = await _semaphore.TryAcquire(name, 2, cancellationToken: cancellationToken);
        RedisSemaphoreHandle? second = await _semaphore.TryAcquire(name, 2, cancellationToken: cancellationToken);

        await _semaphore.ForceReleaseAll(name, 2, cancellationToken);

        (await _semaphore.GetAcquiredCount(name, 2, cancellationToken)).Should().Be(0);

        await first!.DisposeAsync();
        await second!.DisposeAsync();
    }

    [Test]
    public async Task GetStatus_should_report_distributed_capacity(CancellationToken cancellationToken)
    {
        string name = CreateName();

        RedisSemaphoreHandle? first = await _semaphore.TryAcquire(name, 4, cancellationToken: cancellationToken);
        RedisSemaphoreHandle? second = await _semaphore.TryAcquire(name, 4, cancellationToken: cancellationToken);

        RedisSemaphoreStatus status = await _semaphore.GetStatus(name, 4, cancellationToken);

        status.SemaphoreName.Should().Be(name);
        status.MaxCount.Should().Be(4);
        status.AcquiredCount.Should().Be(2);
        status.AvailableCount.Should().Be(2);
        status.Utilization.Should().Be(0.5);
        status.IsFull.Should().BeFalse();
        status.IsAvailable.Should().BeTrue();

        await first!.DisposeAsync();
        await second!.DisposeAsync();
    }

    [Test]
    public async Task Semaphore_name_with_hash_tag_braces_should_be_rejected()
    {
        Func<Task> action = async () => await _semaphore.TryAcquire("jobs:{invalid}", 1);

        await action.Should().ThrowAsync<ArgumentException>();
    }

    [Test]
    public async Task Automatic_renewal_should_keep_long_running_permit(CancellationToken cancellationToken)
    {
        string name = CreateName();
        var options = new RedisSemaphoreOptions
        {
            LeaseDuration = TimeSpan.FromMilliseconds(120),
            RenewalInterval = TimeSpan.FromMilliseconds(30),
            RenewalSafetyMargin = TimeSpan.FromMilliseconds(25)
        };

        RedisSemaphoreHandle? handle = await _semaphore.TryAcquire(name, 1, options, cancellationToken);
        handle.Should().NotBeNull();

        await Task.Delay(TimeSpan.FromMilliseconds(350), cancellationToken);

        RedisSemaphoreHandle? competing = await _semaphore.TryAcquire(name, 1, options, cancellationToken);
        competing.Should().BeNull();
        handle!.IsPermitLost.Should().BeFalse();

        await handle.DisposeAsync();
    }

    [Test]
    public async Task Failed_renewal_should_signal_permit_loss(CancellationToken cancellationToken)
    {
        var semaphore = new RedisSemaphore(_config, CreateRedisUtil(renewalsSucceed: false), NullLogger<RedisSemaphore>.Instance);
        var options = new RedisSemaphoreOptions
        {
            LeaseDuration = TimeSpan.FromMilliseconds(150),
            RenewalInterval = TimeSpan.FromMilliseconds(30),
            RenewalSafetyMargin = TimeSpan.FromMilliseconds(30)
        };

        RedisSemaphoreHandle? handle = await semaphore.TryAcquire(CreateName(), 1, options, cancellationToken);
        handle.Should().NotBeNull();

        await Task.Delay(TimeSpan.FromMilliseconds(250), cancellationToken);

        handle!.IsPermitLost.Should().BeTrue();
        handle.PermitLostToken.IsCancellationRequested.Should().BeTrue();

        await handle.DisposeAsync();
    }

    private static string CreateName() => $"test:semaphore:{Guid.NewGuid():N}";

    private static IRedisUtil CreateRedisUtil(bool renewalsSucceed = true)
    {
        IRedisUtil redisUtil = DispatchProxy.Create<IRedisUtil, RedisUtilProxy>();
        ((RedisUtilProxy) redisUtil).RenewalsSucceed = renewalsSucceed;
        return redisUtil;
    }
}

public class RedisUtilProxy : DispatchProxy
{
    private readonly Dictionary<string, Permit> _values = new();
    private readonly AsyncLock _lock = new();

    public bool RenewalsSucceed { get; set; } = true;

    protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
    {
        if (targetMethod is null || args is null)
            throw new InvalidOperationException("A Redis utility method was invoked without method metadata or arguments.");

        return targetMethod.Name switch
        {
            nameof(IRedisUtil.SetIfNotExists) when args.Length == 4 => SetIfNotExists((string) args[0]!, (string) args[1]!,
                (TimeSpan?) args[2], (CancellationToken) args[3]!),
            nameof(IRedisUtil.GetString) when args.Length == 2 => GetString((string) args[0]!, (CancellationToken) args[1]!),
            nameof(IRedisUtil.CountExisting) when args.Length == 2 => CountExisting((IReadOnlyList<string>) args[0]!,
                (CancellationToken) args[1]!),
            nameof(IRedisUtil.RemoveIfEqual) when args.Length == 3 => RemoveIfEqual((string) args[0]!, (string) args[1]!,
                (CancellationToken) args[2]!),
            nameof(IRedisUtil.ExpireIfEqual) when args.Length == 4 => ExpireIfEqual((string) args[0]!, (string) args[1]!,
                (TimeSpan) args[2]!, (CancellationToken) args[3]!),
            nameof(IRedisUtil.Remove) when args.Length == 3 => Remove((string) args[0]!, (CancellationToken) args[2]!),
            _ => throw new NotSupportedException($"The test Redis utility does not implement {targetMethod}.")
        };
    }

    private async ValueTask<bool> SetIfNotExists(string key, string value, TimeSpan? expiration, CancellationToken cancellationToken)
    {
        using Releaser releaser = await _lock.Lock(cancellationToken).NoSync();
        RemoveIfExpired(key);

        if (_values.ContainsKey(key))
            return false;

        _values[key] = new Permit(value, DateTimeOffset.UtcNow + expiration!.Value);
        return true;
    }

    private async ValueTask<string?> GetString(string key, CancellationToken cancellationToken)
    {
        using Releaser releaser = await _lock.Lock(cancellationToken).NoSync();
        RemoveIfExpired(key);
        return _values.TryGetValue(key, out Permit? permit) ? permit.Token : null;
    }

    private async ValueTask<long?> CountExisting(IReadOnlyList<string> keys, CancellationToken cancellationToken)
    {
        using Releaser releaser = await _lock.Lock(cancellationToken).NoSync();
        long count = 0;

        for (var i = 0; i < keys.Count; i++)
        {
            string key = keys[i];
            RemoveIfExpired(key);

            if (_values.ContainsKey(key))
                count++;
        }

        return count;
    }

    private async ValueTask<bool> RemoveIfEqual(string key, string expectedValue, CancellationToken cancellationToken)
    {
        using Releaser releaser = await _lock.Lock(cancellationToken).NoSync();
        RemoveIfExpired(key);

        if (!_values.TryGetValue(key, out Permit? permit) || permit.Token != expectedValue)
            return false;

        _values.Remove(key);
        return true;
    }

    private async ValueTask<bool> ExpireIfEqual(string key, string expectedValue, TimeSpan expiration, CancellationToken cancellationToken)
    {
        using Releaser releaser = await _lock.Lock(cancellationToken).NoSync();
        RemoveIfExpired(key);

        if (!RenewalsSucceed || !_values.TryGetValue(key, out Permit? permit) || permit.Token != expectedValue)
            return false;

        _values[key] = permit with {ExpiresAt = DateTimeOffset.UtcNow + expiration};
        return true;
    }

    private async ValueTask Remove(string key, CancellationToken cancellationToken)
    {
        using Releaser releaser = await _lock.Lock(cancellationToken).NoSync();
        _values.Remove(key);
    }

    private void RemoveIfExpired(string key)
    {
        if (_values.TryGetValue(key, out Permit? permit) && permit.ExpiresAt <= DateTimeOffset.UtcNow)
            _values.Remove(key);
    }

    private sealed record Permit(string Token, DateTimeOffset ExpiresAt);
}
