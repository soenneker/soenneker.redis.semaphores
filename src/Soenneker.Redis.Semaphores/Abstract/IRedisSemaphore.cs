using System;
using System.Threading;
using System.Threading.Tasks;

namespace Soenneker.Redis.Semaphores.Abstract;

/// <summary>
/// Coordinates a bounded number of distributed permit holders through Redis leases.
/// </summary>
public interface IRedisSemaphore
{
    /// <summary>
    /// Attempts to acquire one permit from a named semaphore.
    /// </summary>
    /// <param name="semaphoreName">The name of the semaphore.</param>
    /// <param name="maxCount">The maximum number of concurrent permit holders.</param>
    /// <param name="options">Lease, retry, and automatic-renewal settings. Defaults are used when omitted.</param>
    /// <param name="cancellationToken">A token to observe while waiting for Redis.</param>
    /// <returns>An owning handle when a permit was acquired; otherwise <c>null</c>.</returns>
    ValueTask<RedisSemaphoreHandle?> TryAcquire(string semaphoreName, int maxCount, RedisSemaphoreOptions? options = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Waits until one permit from a named semaphore can be acquired.
    /// </summary>
    /// <param name="semaphoreName">The name of the semaphore.</param>
    /// <param name="maxCount">The maximum number of concurrent permit holders.</param>
    /// <param name="options">Lease, retry, and automatic-renewal settings. Defaults are used when omitted.</param>
    /// <param name="cancellationToken">A token that cancels the wait.</param>
    /// <returns>An owning handle for the acquired permit.</returns>
    ValueTask<RedisSemaphoreHandle> Acquire(string semaphoreName, int maxCount, RedisSemaphoreOptions? options = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Gets the number of permits currently in use for a named semaphore.
    /// </summary>
    /// <param name="semaphoreName">Name of the semaphore to target.</param>
    /// <param name="maxCount">The number of permit slots used by this semaphore.</param>
    /// <param name="cancellationToken">Token used to cancel the operation.</param>
    /// <returns>The number of permit keys that exist at the time of the call.</returns>
    ValueTask<int> GetAcquiredCount(string semaphoreName, int maxCount, CancellationToken cancellationToken = default);

    /// <summary>
    /// Gets a point-in-time distributed capacity snapshot for a named semaphore.
    /// </summary>
    /// <param name="semaphoreName">Name of the semaphore to target.</param>
    /// <param name="maxCount">The maximum number of permits used by callers of this semaphore.</param>
    /// <param name="cancellationToken">A token to observe while inspecting Redis.</param>
    /// <returns>The point-in-time capacity snapshot.</returns>
    ValueTask<RedisSemaphoreStatus> GetStatus(string semaphoreName, int maxCount, CancellationToken cancellationToken = default);

    /// <summary>
    /// Forcibly removes all permit slots for a named semaphore.
    /// </summary>
    /// <param name="semaphoreName">Name of the semaphore to target.</param>
    /// <param name="maxCount">The number of permit slots to remove.</param>
    /// <param name="cancellationToken">Token used to cancel the operation.</param>
    /// <returns>A task that completes when the force release all operation is complete.</returns>
    /// <remarks>This operation ignores ownership and should only be used for administrative recovery.</remarks>
    ValueTask ForceReleaseAll(string semaphoreName, int maxCount, CancellationToken cancellationToken = default);
}
