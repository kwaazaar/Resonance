using Microsoft.Extensions.DependencyInjection;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using System.IO;
using Resonance.Repo;
using Resonance.Models;

namespace Resonance.Demo
{
    public class Program
    {
        private static IServiceProvider serviceProvider;
        private static string topicId = "c2581735-00d2-421c-9e65-34195a134d37";
        private static string subscriptionId = "e1379e77-4c5c-4326-bedd-09250271d545";

        public static void Main(string[] args)
        {
            var serviceCollection = new ServiceCollection();
            ConfigureServices(serviceCollection);
            serviceProvider = serviceCollection.BuildServiceProvider();

            InitRepo();
        }

        private static void ConfigureServices(IServiceCollection serviceCollection)
        {
            var builder = new ConfigurationBuilder()
                //.SetBasePath(PlatformServices.Default.Application.ApplicationBasePath)
                .SetBasePath(AppContext.BaseDirectory)
                .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true);
            //builder.AddEnvironmentVariables();
            var config = builder.Build();


            // Add IConfiguration dependency (reason: allows access to config from any injected component)
            serviceCollection.AddSingleton<IConfiguration>(config);

            // Configure IDbConnection dependency (reason: may be required by IEventingRepo dependencies)
            var connectionString = config.GetConnectionString("Resonance");
            serviceCollection.AddTransient<IDbConnection>((p) => {
                return new SqlConnection(connectionString);
            });

            // Configure IEventingRepo dependency (reason: the repo that must be used in this app)
            serviceCollection.AddTransient<IEventingRepo, MsSqlEventingRepo>();

            // Configure EventPublisher
            serviceCollection.AddTransient<IEventPublisher, EventPublisher>();
        }

        private static void InitRepo()
        {
            var eventingRepo = serviceProvider.GetRequiredService<IEventingRepo>();

            var topic = eventingRepo.GetTopic(topicId);
            if (topic == null)
                eventingRepo.AddOrUpdateTopic(new Topic
                {
                    Id = topicId,
                    Name = "Demo Topic",
                    Notes = "This topic is for demo purposes. Nothing to see here, move along!",
                });

            var subscription = eventingRepo.GetSubscription(subscriptionId);
            if (subscription == null)
                eventingRepo.AddOrUpdateSubscription(new Subscription
                {
                    Id = subscriptionId,
                    Name = "Demo Subscription",
                    TopicId = topicId,
                    Enabled = true,
                    DeliveryDelay = 3,
                    MaxDeliveries = 2,
                    Ordered = true,
                    TimeToLive = 60,
                });

            var publisher = serviceProvider.GetRequiredService<IEventPublisher>();
            var x = new
            {
                Name = "Robert",
                Age = 40,
            };
            publisher.Publish(topicName: "Demo Topic", payload: x);
        }
    }
}
