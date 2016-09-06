using Microsoft.Extensions.DependencyInjection;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Threading.Tasks;

namespace Resonance.Demo
{
    public class Program
    {
        public static void Main(string[] args)
        {
            IServiceCollection serviceCollection = new ServiceCollection();
            ConfigureServices(serviceCollection);
            Application application = new Application(serviceCollection);
        }

        private static void ConfigureServices(IServiceCollection serviceCollection)
        {
            IDbConnection conn = new SqlConnection();
            serviceCollection.AddInstance<IDbConnection>(conn);
        }

    }
}
