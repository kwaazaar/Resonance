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
using Microsoft.Extensions.PlatformAbstractions;

namespace Resonance.Demo
{
    public class Program
    {
        

        private static IServiceProvider serviceProvider;

        public static void Main(string[] args)
        {
            var serviceCollection = new ServiceCollection();
            ConfigureServices(serviceCollection);
            serviceProvider = serviceCollection.BuildServiceProvider();

            //Application application = new Application(serviceCollection);
            var eventingRepo = serviceProvider.GetRequiredService<IEventingRepo>();
            var topics = eventingRepo.GetTopics(null);
        }

        private static void ConfigureServices(IServiceCollection serviceCollection)
        {
            var builder = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                //.SetBasePath(AppContext.BaseDirectory)
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
        }

    }
}
