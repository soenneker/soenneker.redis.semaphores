[![](https://img.shields.io/nuget/v/soenneker.redis.semaphores.svg?style=for-the-badge)](https://www.nuget.org/packages/soenneker.redis.semaphores/)
[![](https://img.shields.io/github/actions/workflow/status/soenneker/soenneker.redis.semaphores/publish-package.yml?style=for-the-badge)](https://github.com/soenneker/soenneker.redis.semaphores/actions/workflows/publish-package.yml)
[![](https://img.shields.io/nuget/dt/soenneker.redis.semaphores.svg?style=for-the-badge)](https://www.nuget.org/packages/soenneker.redis.semaphores/)
[![](https://img.shields.io/github/actions/workflow/status/soenneker/soenneker.redis.semaphores/build-and-test.yml?label=build%20and%20test&style=for-the-badge)](https://github.com/soenneker/soenneker.redis.semaphores/actions/workflows/build-and-test.yml)
[![](https://img.shields.io/github/actions/workflow/status/soenneker/soenneker.redis.semaphores/codeql.yml?label=CodeQL&style=for-the-badge)](https://github.com/soenneker/soenneker.redis.semaphores/actions/workflows/codeql.yml)

# Soenneker.Redis.Semaphores

Distributed, lease-based semaphores backed by Redis.

## Installation

```bash
dotnet add package Soenneker.Redis.Semaphores
```

## Registration

```csharp
services.AddRedisSemaphoreAsScoped();
```

`AddRedisSemaphoreAsSingleton()` is also available. Both registrations add `Soenneker.Redis.Util` and its dependencies.

Logging is disabled by default. Enable Redis logging through configuration:

```json
{
  "Azure": {
    "Redis": {
      "Log": true
    }
  }
}
```

## Usage

Try to acquire a permit without waiting:

```csharp
RedisSemaphoreHandle? handle = await semaphore.TryAcquire(
    "imports",
    maxCount: 4,
    cancellationToken: cancellationToken);

if (handle is null)
    return;

await using (handle)
{
    await RunImport(cancellationToken);
}
```

Or wait until a permit becomes available:

```csharp
await using RedisSemaphoreHandle handle = await semaphore.Acquire(
    "imports",
    maxCount: 4,
    cancellationToken: cancellationToken);
```

Permits automatically renew while their handles are alive, so the same API supports jobs lasting milliseconds, minutes, or hours. The default one-minute lease measures runner health rather than expected job duration. Disposing the handle stops renewal and releases only its permit. If a runner crashes, renewal stops and Redis recovers the abandoned permit when its lease expires.

Pass the handle's loss token into cooperative work so it stops if Redis ownership can no longer be maintained:

```csharp
await using RedisSemaphoreHandle handle = await semaphore.Acquire(
    "automation-jobs",
    maxCount: 10,
    cancellationToken: cancellationToken);

using CancellationTokenSource jobCancellation = CancellationTokenSource.CreateLinkedTokenSource(
    cancellationToken,
    handle.PermitLostToken);

await RunJob(jobCancellation.Token);
```

Lease and retry behavior can be tuned when necessary:

```csharp
var options = new RedisSemaphoreOptions
{
    LeaseDuration = TimeSpan.FromMinutes(1),
    RenewalInterval = TimeSpan.FromSeconds(20),
    RenewalSafetyMargin = TimeSpan.FromSeconds(10),
    RetryInterval = TimeSpan.FromMilliseconds(100)
};

await using RedisSemaphoreHandle handle = await semaphore.Acquire(
    "automation-jobs",
    maxCount: 10,
    options,
    cancellationToken);
```

Renewal is owner-safe: `Soenneker.Redis.Util` uses a Redis transaction to compare the unique permit token and extend its expiration atomically. An expired handle cannot renew or release a permit subsequently acquired by another runner. No Lua scripts are used.

All callers for a semaphore name must use the same `maxCount`. If `PermitLostToken` is canceled, the runner should stop the job before the lease can be acquired elsewhere.
Semaphore names cannot contain `{` or `}` because the library uses Redis hash tags to keep all permit keys for a semaphore in the same cluster slot.

## Capacity visibility

Query the distributed permit usage at any time:

```csharp
RedisSemaphoreStatus status = await semaphore.GetStatus(
    "automation-jobs",
    maxCount: 10,
    cancellationToken);

// status.MaxCount       = 10
// status.AcquiredCount  = permits currently held across all runners
// status.AvailableCount = permits currently available
// status.Utilization    = fraction from 0 through 1
// status.IsFull
// status.IsAvailable
```

The result is a point-in-time snapshot. Permit state can change immediately after it is read.

## Explicitly managed permits

For durable workers that already renew their own leases, use the explicit-database API without starting a second renewal loop:

```csharp
RedisSemaphorePermit? permit = await RedisSemaphore.TryAcquirePermit(
    database, "flywheel:{namespace}:function:imports", 4, TimeSpan.FromMinutes(1), cancellationToken);
```

The returned permit contains its key and owner token. `RenewPermit` and `ReleasePermit` use the strict atomic helpers in `Soenneker.Redis.Util` and propagate connection failures. For a transaction that updates dependent state, add `permit.OwnershipCondition` to `RedisAtomicTransaction`, then queue renewal or deletion of `permit.Key` in that same transaction. This prevents a stale owner from updating dependent state after losing its permit.

The key prefix is fully qualified and may carry a Redis Cluster hash tag; the caller must place dependent transaction keys in the same slot. Permits expire if abandoned, and no handle disposal or background timer is required. A failed/ambiguous acquisition can leave a permit until expiry. Do not release a permit after an unknown dependent commit outcome unless that operation's recovery protocol proves it is safe.

All acquisitions must agree on `maxCount`. Applications supporting runtime limit changes must coordinate them with existing executions; changing the slot count alone cannot safely lower a limit while higher slots are occupied.

Local development references the sibling `soenneker.redis.util` project for its new atomics. Coordinate the Util package release and package dependency version before publishing Semaphores. The `RedisSemaphorePermitTests` use `REDIS_TEST_CONNECTION` (or `FLYWHEEL_TEST_REDIS`), defaulting to localhost:6379.
