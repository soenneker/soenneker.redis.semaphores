using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Soenneker.Atomics.ValueBools;
using Soenneker.Extensions.Task;
using Soenneker.Extensions.ValueTask;
using Soenneker.Utils.Random;

namespace Soenneker.Redis.Semaphores;

/// <summary>
/// Represents ownership of a permit acquired from a distributed Redis semaphore.
/// </summary>
public sealed class RedisSemaphoreHandle : IAsyncDisposable
{
    private readonly RedisSemaphore _semaphore;
    private readonly RedisSemaphoreSettings _settings;
    private readonly string _permitKey;
    private readonly CancellationTokenSource _renewalCancellation = new();
    private readonly CancellationTokenSource _permitLostCancellation = new();
    private Task? _renewalTask;
    private long _lastRenewedTimestamp;
    private ValueAtomicBool _disposed;
    private ValueAtomicBool _permitLost;

    /// <summary>The name of the semaphore that owns this permit.</summary>
    public string SemaphoreName { get; }

    /// <summary>The zero-based Redis slot occupied by this permit.</summary>
    public int Slot { get; }

    /// <summary>The unique ownership token stored for this permit.</summary>
    public string PermitToken { get; }

    /// <summary>The duration for which this permit was leased.</summary>
    public TimeSpan LeaseDuration { get; }

    /// <summary>A token that is canceled when this handle can no longer maintain ownership of its permit.</summary>
    public CancellationToken PermitLostToken => _permitLostCancellation.Token;

    /// <summary>Indicates that automatic renewal could not maintain ownership of the permit.</summary>
    public bool IsPermitLost => _permitLost.Read();

    internal RedisSemaphoreHandle(RedisSemaphore semaphore, string semaphoreName, int slot, string permitKey, string permitToken,
        RedisSemaphoreSettings settings, long acquisitionStartedTimestamp)
    {
        _semaphore = semaphore;
        _settings = settings;
        _permitKey = permitKey;
        SemaphoreName = semaphoreName;
        Slot = slot;
        PermitToken = permitToken;
        LeaseDuration = settings.LeaseDuration;
        _lastRenewedTimestamp = acquisitionStartedTimestamp;
    }

    internal void StartRenewal()
    {
        if (_settings.AutoRenew)
            _renewalTask = RunRenewalLoop();
    }

    /// <summary>
    /// Atomically renews this permit only if it is still owned by this handle.
    /// </summary>
    /// <param name="cancellationToken">Token used to cancel the operation.</param>
    /// <returns>true if atomically renews this permit only if it is still owned by this handle; otherwise, false.</returns>
    public async ValueTask<bool> Renew(CancellationToken cancellationToken = default)
    {
        if (_disposed.Read() || IsPermitLost)
            return false;

        long renewalStartedTimestamp = Stopwatch.GetTimestamp();
        bool renewed = await _semaphore.Renew(_permitKey, PermitToken, LeaseDuration, cancellationToken).NoSync();

        if (renewed)
            Volatile.Write(ref _lastRenewedTimestamp, renewalStartedTimestamp);

        return renewed;
    }

    private async Task RunRenewalLoop()
    {
        CancellationToken cancellationToken = _renewalCancellation.Token;
        using var renewalTimer = new PeriodicTimer(AddJitter(_settings.RenewalInterval));

        try
        {
            while (await renewalTimer.WaitForNextTickAsync(cancellationToken).NoSync())
            {
                if (await Renew(cancellationToken).NoSync())
                    continue;

                while (Stopwatch.GetElapsedTime(Volatile.Read(ref _lastRenewedTimestamp)) < LeaseDuration - _settings.RenewalSafetyMargin)
                {
                    await Task.Delay(AddJitter(_settings.RenewalRetryInterval), cancellationToken).NoSync();

                    if (await Renew(cancellationToken).NoSync())
                        break;
                }

                if (Stopwatch.GetElapsedTime(Volatile.Read(ref _lastRenewedTimestamp)) >= LeaseDuration - _settings.RenewalSafetyMargin)
                {
                    MarkPermitLost();
                    return;
                }
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
        }
        catch (Exception exception)
        {
            _semaphore.LogRenewalFailure(exception, SemaphoreName, Slot);
            MarkPermitLost();
        }
    }

    private static TimeSpan AddJitter(TimeSpan interval)
    {
        double multiplier = 0.8 + RandomUtil.NextDouble() * 0.2;
        return TimeSpan.FromTicks(Math.Max(1, (long) (interval.Ticks * multiplier)));
    }

    private void MarkPermitLost()
    {
        if (!_permitLost.TrySetTrue())
            return;

        _semaphore.LogPermitLost(SemaphoreName, Slot);

        try
        {
            _permitLostCancellation.Cancel();
        }
        catch (AggregateException exception)
        {
            _semaphore.LogRenewalFailure(exception, SemaphoreName, Slot);
        }
    }

    /// <summary>
    /// Releases this permit if it is still owned by this handle. Repeated calls have no effect.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        if (!_disposed.TrySetTrue())
            return;

        await _renewalCancellation.CancelAsync().NoSync();

        if (_renewalTask is not null)
            await _renewalTask.NoSync();

        try
        {
            _ = await _semaphore.Release(SemaphoreName, Slot, _permitKey, PermitToken, CancellationToken.None).NoSync();
        }
        finally
        {
            _renewalCancellation.Dispose();
            _permitLostCancellation.Dispose();
        }
    }
}
