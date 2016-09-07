using Microsoft.Extensions.DependencyInjection;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using System.IO;

namespace Resonance.Demo
{
    public class Program
    {
        public static void Main(string[] args)
        {
            IServiceCollection serviceCollection = new ServiceCollection();
            ConfigureServices(serviceCollection);
            //Application application = new Application(serviceCollection);
        }

        private static void ConfigureServices(IServiceCollection serviceCollection)
        {
            var builder = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true);
            //builder.AddEnvironmentVariables();

            var config = builder.Build();
            serviceCollection.AddSingleton<IConfiguration>(config);

            var connectionString = config.GetConnectionString("Resonance");
            serviceCollection.AddTransient<IDbConnection>((p) => {
                return new SqlConnection(connectionString);
            });
        }

    }
}
