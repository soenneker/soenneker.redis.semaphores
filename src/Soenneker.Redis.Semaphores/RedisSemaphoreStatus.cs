namespace Soenneker.Redis.Semaphores;

/// <summary>
/// Represents a point-in-time distributed capacity snapshot for a Redis semaphore.
/// </summary>
public sealed record RedisSemaphoreStatus
{
    /// <summary>The name of the semaphore.</summary>
    public required string SemaphoreName { get; init; }

    /// <summary>The maximum number of permits.</summary>
    public required int MaxCount { get; init; }

    /// <summary>The number of permits currently held.</summary>
    public required int AcquiredCount { get; init; }

    /// <summary>The number of permits currently available.</summary>
    public int AvailableCount => MaxCount - AcquiredCount;

    /// <summary>The fraction of capacity currently in use, from 0 through 1.</summary>
    public double Utilization => (double) AcquiredCount / MaxCount;

    /// <summary>Indicates whether every permit is currently held.</summary>
    public bool IsFull => AcquiredCount >= MaxCount;

    /// <summary>Indicates whether at least one permit is currently available.</summary>
    public bool IsAvailable => AcquiredCount < MaxCount;
}
