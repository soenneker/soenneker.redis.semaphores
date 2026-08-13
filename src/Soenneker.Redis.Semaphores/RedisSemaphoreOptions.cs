using System;

namespace Soenneker.Redis.Semaphores;

/// <summary>
/// Configures acquisition and automatic renewal of a Redis semaphore permit.
/// </summary>
public sealed class RedisSemaphoreOptions
{
    /// <summary>
    /// How long Redis retains a permit without a successful renewal. This is a runner-health timeout, not a job-duration limit.
    /// </summary>
    public TimeSpan LeaseDuration { get; set; } = TimeSpan.FromMinutes(1);

    /// <summary>
    /// How often an unavailable semaphore is retried by <see cref="Abstract.IRedisSemaphore.Acquire"/>.
    /// </summary>
    public TimeSpan RetryInterval { get; set; } = TimeSpan.FromMilliseconds(100);

    /// <summary>
    /// Whether the acquired handle automatically renews its lease until it is disposed. Defaults to <c>true</c>.
    /// </summary>
    public bool AutoRenew { get; set; } = true;

    /// <summary>
    /// How often the permit is renewed. Defaults to one third of <see cref="LeaseDuration"/>.
    /// </summary>
    public TimeSpan? RenewalInterval { get; set; }

    /// <summary>
    /// How far before the locally expected expiration renewal must succeed. Defaults to one fifth of <see cref="LeaseDuration"/>.
    /// </summary>
    public TimeSpan? RenewalSafetyMargin { get; set; }
}
