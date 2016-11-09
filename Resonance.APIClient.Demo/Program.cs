using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.PlatformAbstractions;
using Microsoft.Extensions.Logging;
using Resonance.Repo;
using Resonance.Models;

namespace Resonance.APIClient.Demo
{
    public class Program
    {
        private static IServiceProvider serviceProvider;

        public static void Main(string[] args)
        {
            var serviceCollection = new ServiceCollection();
            ConfigureServices(serviceCollection);
            serviceProvider = serviceCollection.BuildServiceProvider();

            var publisher = serviceProvider.GetRequiredService<IEventPublisherAsync>();
            var consumer = serviceProvider.GetRequiredService<IEventConsumerAsync>();

            var subs = consumer.GetSubscriptionsAsync().GetAwaiter().GetResult();

            if (subs.Count() > 0)
            {
                var sub = consumer.AddOrUpdateSubscriptionAsync(subs.First()).GetAwaiter().GetResult();
                sub.MaxDeliveries += 1;
                sub = consumer.AddOrUpdateSubscriptionAsync(subs.First()).GetAwaiter().GetResult();
            }
            var sub2 = consumer.GetSubscriptionByNameAsync(subs.First().Name).GetAwaiter().GetResult();
            var sub3 = consumer.GetSubscriptionAsync(subs.First().Id.Value).GetAwaiter().GetResult();
            //var sub = consumer.AddOrUpdateSubscriptionAsync(new Subscription { Name = "TestSub", Ordered = true, TopicSubscriptions = new List<TopicSubscription>() });
        }


        private static void ConfigureServices(IServiceCollection serviceCollection)
        {
            var builder = new ConfigurationBuilder()
                .SetBasePath(PlatformServices.Default.Application.ApplicationBasePath)
                .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true);
            //builder.AddEnvironmentVariables();
            var config = builder.Build();

            // Add IConfiguration dependency (reason: allows access to config from any injected component)
            serviceCollection.AddSingleton<IConfiguration>(config);

            ILoggerFactory loggerFactory = new LoggerFactory()
                .AddConsole(LogLevel.Information)
                .AddDebug(LogLevel.Trace);
            serviceCollection.AddSingleton<ILoggerFactory>(loggerFactory);


            // Configure EventPublisher & EventConsumer
            var apiBaseAddress = new Uri(config["Resonance:API:BaseAddress"], UriKind.Absolute);
            serviceCollection.AddTransient<IEventPublisherAsync>((p) =>
            {
                return new APIEventPublisher(apiBaseAddress);
            });
            serviceCollection.AddTransient<IEventConsumerAsync>((p) =>
            {
                return new APIEventConsumer(apiBaseAddress);
            });
        }
    }
}
