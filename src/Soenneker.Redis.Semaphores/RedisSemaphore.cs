using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Soenneker.Extensions.Task;
using Soenneker.Extensions.ValueTask;
using Soenneker.Redis.Semaphores.Abstract;
using Soenneker.Redis.Util.Abstract;
using Soenneker.Utils.Random;

namespace Soenneker.Redis.Semaphores;

public sealed class RedisSemaphore : IRedisSemaphore
{
    private static readonly RedisSemaphoreSettings _defaultSettings = new(TimeSpan.FromMinutes(1), TimeSpan.FromMilliseconds(100), true,
        TimeSpan.FromSeconds(20), TimeSpan.FromSeconds(12), TimeSpan.FromSeconds(1));

    private readonly IRedisUtil _redisUtil;
    private readonly ILogger<RedisSemaphore> _logger;
    private readonly bool _log;

    public RedisSemaphore(IConfiguration config, IRedisUtil redisUtil, ILogger<RedisSemaphore> logger)
    {
        _redisUtil = redisUtil;
        _logger = logger;
        _log = config.GetValue<bool>("Azure:Redis:Log");
    }

    public ValueTask<RedisSemaphoreHandle?> TryAcquire(string semaphoreName, int maxCount, RedisSemaphoreOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        RedisSemaphoreSettings settings = ValidateAndResolve(semaphoreName, maxCount, options);
        return TryAcquire(semaphoreName, maxCount, settings, cancellationToken);
    }

    private async ValueTask<RedisSemaphoreHandle?> TryAcquire(string semaphoreName, int maxCount, RedisSemaphoreSettings settings,
        CancellationToken cancellationToken)
    {
        var permitToken = Guid.NewGuid().ToString("N");
        int startingSlot = maxCount == 1 ? 0 : RandomUtil.Next(maxCount);

        for (var offset = 0; offset < maxCount; offset++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var slot = (int) (((long) startingSlot + offset) % maxCount);
            string permitKey = BuildPermitKey(semaphoreName, slot);
            long acquisitionStartedTimestamp = Stopwatch.GetTimestamp();

            bool acquired = await _redisUtil.SetIfNotExists(permitKey, permitToken, settings.LeaseDuration, cancellationToken)
                                                    .NoSync();

            if (!acquired)
                continue;

            if (_log && _logger.IsEnabled(LogLevel.Debug))
                _logger.LogDebug("Acquired Redis semaphore permit ({semaphoreName}, slot {slot})", semaphoreName, slot);

            var handle = new RedisSemaphoreHandle(this, semaphoreName, slot, permitKey, permitToken, settings, acquisitionStartedTimestamp);
            handle.StartRenewal();
            return handle;
        }

        if (_log && _logger.IsEnabled(LogLevel.Debug))
            _logger.LogDebug("No permits are available for Redis semaphore ({semaphoreName})", semaphoreName);

        return null;
    }

    public async ValueTask<RedisSemaphoreHandle> Acquire(string semaphoreName, int maxCount, RedisSemaphoreOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        RedisSemaphoreSettings settings = ValidateAndResolve(semaphoreName, maxCount, options);

        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();

            RedisSemaphoreHandle? handle = await TryAcquire(semaphoreName, maxCount, settings, cancellationToken).NoSync();

            if (handle is not null)
                return handle;

            await Task.Delay(settings.RetryInterval, cancellationToken).NoSync();
        }
    }

    public async ValueTask<int> GetAcquiredCount(string semaphoreName, int maxCount, CancellationToken cancellationToken = default)
    {
        ValidateNameAndCount(semaphoreName, maxCount);
        var permitKeys = new string[maxCount];

        for (var slot = 0; slot < maxCount; slot++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            permitKeys[slot] = BuildPermitKey(semaphoreName, slot);
        }

        long? acquiredCount = await _redisUtil.CountExisting(permitKeys, cancellationToken).NoSync();
        return acquiredCount is null ? 0 : (int) acquiredCount.Value;
    }

    public async ValueTask<RedisSemaphoreStatus> GetStatus(string semaphoreName, int maxCount,
        CancellationToken cancellationToken = default)
    {
        int acquiredCount = await GetAcquiredCount(semaphoreName, maxCount, cancellationToken).NoSync();

        return new RedisSemaphoreStatus
        {
            SemaphoreName = semaphoreName,
            MaxCount = maxCount,
            AcquiredCount = acquiredCount
        };
    }

    public async ValueTask ForceReleaseAll(string semaphoreName, int maxCount, CancellationToken cancellationToken = default)
    {
        ValidateNameAndCount(semaphoreName, maxCount);
        if (_log)
            _logger.LogWarning("Forcibly releasing all permits for Redis semaphore ({semaphoreName})", semaphoreName);

        for (var slot = 0; slot < maxCount; slot++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            await _redisUtil.Remove(BuildPermitKey(semaphoreName, slot), cancellationToken: cancellationToken).NoSync();
        }
    }

    internal async ValueTask<bool> Release(string semaphoreName, int slot, string permitKey, string permitToken,
        CancellationToken cancellationToken = default)
    {
        bool released = await _redisUtil.RemoveIfEqual(permitKey, permitToken, cancellationToken).NoSync();

        if (_log && _logger.IsEnabled(LogLevel.Debug))
        {
            if (released)
                _logger.LogDebug("Released Redis semaphore permit ({semaphoreName}, slot {slot})", semaphoreName, slot);
            else
                _logger.LogDebug("Redis semaphore permit was not released because it expired or changed ownership ({semaphoreName}, slot {slot})",
                    semaphoreName, slot);
        }

        return released;
    }

    internal ValueTask<bool> Renew(string permitKey, string permitToken, TimeSpan leaseDuration,
        CancellationToken cancellationToken = default)
    {
        return _redisUtil.ExpireIfEqual(permitKey, permitToken, leaseDuration, cancellationToken);
    }

    internal void LogPermitLost(string semaphoreName, int slot)
    {
        if (_log)
            _logger.LogWarning("Lost ownership of Redis semaphore permit ({semaphoreName}, slot {slot})", semaphoreName, slot);
    }

    internal void LogRenewalFailure(Exception exception, string semaphoreName, int slot)
    {
        if (_log)
            _logger.LogError(exception, "Redis semaphore renewal loop failed ({semaphoreName}, slot {slot})", semaphoreName, slot);
    }

    private static string BuildPermitKey(string semaphoreName, int slot) => $"{{{semaphoreName}}}:semaphore:{slot}";

    private static RedisSemaphoreSettings ValidateAndResolve(string semaphoreName, int maxCount, RedisSemaphoreOptions? options)
    {
        ValidateNameAndCount(semaphoreName, maxCount);

        if (options is null)
            return _defaultSettings;

        TimeSpan leaseDuration = options.LeaseDuration;

        if (leaseDuration <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(options), "Lease duration must be greater than zero.");

        if (options.RetryInterval <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(options), "Retry interval must be greater than zero.");

        TimeSpan renewalInterval = options.RenewalInterval ?? TimeSpan.FromTicks(Math.Max(1, leaseDuration.Ticks / 3));
        TimeSpan safetyMargin = options.RenewalSafetyMargin ?? TimeSpan.FromTicks(Math.Max(1, leaseDuration.Ticks / 5));

        if (renewalInterval <= TimeSpan.Zero || renewalInterval >= leaseDuration)
            throw new ArgumentOutOfRangeException(nameof(options), "Renewal interval must be greater than zero and less than the lease duration.");

        if (safetyMargin <= TimeSpan.Zero || safetyMargin >= leaseDuration)
            throw new ArgumentOutOfRangeException(nameof(options), "Renewal safety margin must be greater than zero and less than the lease duration.");

        if (renewalInterval >= leaseDuration - safetyMargin)
            throw new ArgumentOutOfRangeException(nameof(options), "Renewal interval must occur before the lease safety deadline.");

        TimeSpan renewalRetryInterval = TimeSpan.FromTicks(Math.Max(1, Math.Min(TimeSpan.FromSeconds(1).Ticks, renewalInterval.Ticks / 4)));

        return new RedisSemaphoreSettings(leaseDuration, options.RetryInterval, options.AutoRenew, renewalInterval, safetyMargin,
            renewalRetryInterval);
    }

    private static void ValidateNameAndCount(string semaphoreName, int maxCount)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(semaphoreName);

        if (semaphoreName.Contains('{') || semaphoreName.Contains('}'))
            throw new ArgumentException("Semaphore names cannot contain Redis hash-tag braces ('{' or '}').", nameof(semaphoreName));

        if (maxCount <= 0)
            throw new ArgumentOutOfRangeException(nameof(maxCount), "Maximum count must be greater than zero.");
    }
}

internal readonly record struct RedisSemaphoreSettings(TimeSpan LeaseDuration, TimeSpan RetryInterval, bool AutoRenew,
    TimeSpan RenewalInterval, TimeSpan RenewalSafetyMargin, TimeSpan RenewalRetryInterval);
