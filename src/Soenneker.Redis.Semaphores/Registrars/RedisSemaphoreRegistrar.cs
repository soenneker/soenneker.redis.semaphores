using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Soenneker.Redis.Semaphores.Abstract;
using Soenneker.Redis.Util.Registrars;

namespace Soenneker.Redis.Semaphores.Registrars;

/// <summary>
/// A utility library providing distributed semaphores backed by Redis.
/// </summary>
public static class RedisSemaphoreRegistrar
{
    /// <summary>
    /// Adds <see cref="IRedisSemaphore"/> as a singleton service. <para/>
    /// </summary>
    /// <param name="services">Service collection that receives the registration.</param>
    /// <returns>The same service collection, so additional registrations can be chained.</returns>
    public static IServiceCollection AddRedisSemaphoreAsSingleton(this IServiceCollection services)
    {
        services.AddRedisUtilAsSingleton()
                .TryAddSingleton<IRedisSemaphore, RedisSemaphore>();

        return services;
    }

    /// <summary>
    /// Adds <see cref="IRedisSemaphore"/> as a scoped service. <para/>
    /// </summary>
    /// <param name="services">Service collection that receives the registration.</param>
    /// <returns>The same service collection, so additional registrations can be chained.</returns>
    public static IServiceCollection AddRedisSemaphoreAsScoped(this IServiceCollection services)
    {
        services.AddRedisUtilAsScoped()
                .TryAddScoped<IRedisSemaphore, RedisSemaphore>();

        return services;
    }
}
