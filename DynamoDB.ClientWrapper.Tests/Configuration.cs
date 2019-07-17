namespace DynamoDB.ClientWrapper.Tests
{
    using System.IO;
    using System.Reflection;

    using Microsoft.Extensions.Configuration;

    internal class Configuration
    {
        private static IConfiguration configuration;

        public static IConfiguration Current
        {
            get
            {
                if (configuration == null)
                {
                    configuration = GetIConfigurationRoot(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location));
                }
                
                return configuration;
            }
        }
        
        private static IConfigurationRoot GetIConfigurationRoot(string outputPath)
        {
            return new ConfigurationBuilder()
                .SetBasePath(outputPath)
                .AddJsonFile("appsettings.json", true, true)
                .AddEnvironmentVariables()
                .Build();
        }
    }
}