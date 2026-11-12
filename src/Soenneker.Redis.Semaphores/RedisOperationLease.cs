using System;
using System.Threading;
using System.Threading.Tasks;
using Soenneker.Redis.Semaphores.Abstract;

namespace Soenneker.Redis.Semaphores;

/// <summary>Scopes an exclusive semaphore permit and links caller cancellation with lost ownership.</summary>
public sealed class RedisOperationLease : IAsyncDisposable
{
    private readonly RedisSemaphoreHandle _handle;
    private readonly CancellationTokenSource _work;

    /// <summary>Cancellation for all work protected by this permit.</summary>
    public CancellationToken Token => _work.Token;

    private RedisOperationLease(RedisSemaphoreHandle handle, CancellationToken cancellationToken)
    {
        _handle = handle;
        _work = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, handle.PermitLostToken);
    }

    /// <summary>Acquires one auto-renewing permit. Waiting and ownership are managed by the semaphore library.</summary>
    /// <param name="semaphore">The distributed semaphore service.</param>
    /// <param name="key">The shared operation name; all participants must use the same name and a count of one.</param>
    /// <param name="lifetime">The renewable permit lifetime, independent of the acquisition deadline.</param>
    /// <param name="cancellationToken">Cancels acquisition and protected work.</param>
    /// <param name="waitTimeout">An optional acquisition deadline. Background jobs may rely on their caller's deadline.</param>
    public static async ValueTask<RedisOperationLease> Acquire(IRedisSemaphore semaphore, string key, TimeSpan lifetime,
        CancellationToken cancellationToken, TimeSpan? waitTimeout = null)
    {
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        if (waitTimeout.HasValue)
        {
            if (waitTimeout.Value <= TimeSpan.Zero) throw new ArgumentOutOfRangeException(nameof(waitTimeout));
            deadline.CancelAfter(waitTimeout.Value);
        }

        RedisSemaphoreHandle handle;
        try
        {
            handle = await semaphore.Acquire(key, 1, new RedisSemaphoreOptions { LeaseDuration = lifetime }, deadline.Token);
            if (deadline.IsCancellationRequested)
            {
                await handle.DisposeAsync();
                deadline.Token.ThrowIfCancellationRequested();
            }
        }
        catch (OperationCanceledException exception) when (!cancellationToken.IsCancellationRequested && deadline.IsCancellationRequested)
        {
            throw new TimeoutException($"Timed out waiting for operation '{key}'.", exception);
        }

        return new RedisOperationLease(handle, cancellationToken);
    }

    /// <summary>Attempts one acquisition for background work that should skip an already-owned operation.</summary>
    public static async ValueTask<RedisOperationLease?> TryAcquire(IRedisSemaphore semaphore, string key, TimeSpan lifetime,
        CancellationToken cancellationToken)
    {
        RedisSemaphoreHandle? handle = await semaphore.TryAcquire(key, 1,
            new RedisSemaphoreOptions { LeaseDuration = lifetime }, cancellationToken);
        if (handle == null) return null;
        if (cancellationToken.IsCancellationRequested)
        {
            await handle.DisposeAsync();
            cancellationToken.ThrowIfCancellationRequested();
        }
        return new RedisOperationLease(handle, cancellationToken);
    }

    public async ValueTask DisposeAsync()
    {
        try { await _handle.DisposeAsync(); }
        finally { _work.Dispose(); }
    }
}
