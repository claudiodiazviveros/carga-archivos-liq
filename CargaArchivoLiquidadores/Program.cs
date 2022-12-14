using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Serilog;
using System;
using System.IO;
using System.Threading.Tasks;
using CargaArchivoLiquidadores.Interfaces;
using CargaArchivoLiquidadores.Activities;

namespace CargaArchivoLiquidadores
{
    public class Program
    {
        public static IConfiguration configuration;

        static int Main(string[] args)
        {
            Log.Logger = new LoggerConfiguration()
                 .WriteTo.Console(Serilog.Events.LogEventLevel.Debug)
                 .MinimumLevel.Debug()
                 .Enrich.FromLogContext()
                 .CreateLogger();

            try
            {
                MainAsync(args).Wait();
                return 0;
            }
            catch
            {
                return 1;
            }
        }

        static async Task MainAsync(string[] args)
        {
            ServiceCollection services = new ServiceCollection();
            ConfigureServices(services);

            try
            {
                using (var serviceProvider = services.BuildServiceProvider())
                {
                    var app = serviceProvider.GetService<App>();
                    await app.Run();
                }
            }
            catch (Exception ex)
            {
                Log.Fatal(ex.Message);
            }
            finally
            {
                Log.CloseAndFlush();
            }

            Console.ReadKey();
        }

        private static void ConfigureServices(IServiceCollection serviceCollection)
        {
            serviceCollection.AddSingleton(LoggerFactory.Create(builder =>
            {
                builder.AddSerilog(dispose: true);
            }));

            serviceCollection.AddLogging();

            configuration = new ConfigurationBuilder()
                .SetBasePath(Directory.GetParent(AppContext.BaseDirectory).FullName)
                .AddJsonFile("appsettings.json", false)
                .Build();

            serviceCollection.AddSingleton<IConfiguration>(configuration);
            serviceCollection.AddScoped<App>()
                .AddScoped<IFileClasificacionBiomedica, FileClasificacionBiomedica>()
                .AddScoped<IFileMedicamentos, FileMedicamentos>()
                .AddScoped<IFileCarta, FileCarta>()
                .AddScoped<IFileLiquidacion, FileLiquidacion>()
                .AddScoped<ILoadMaestroSolicitud, LoadMaestroSolicitud>()
                .AddScoped<ILoadMaestroDetalleSolicitud, LoadMaestroDetalleSolicitud>()
                .AddScoped<ILoadFileDetalleSolicitud, LoadFileDetalleSolicitud>()
                .AddScoped<ILoadFileSolicitud, LoadFileSolicitud>()
                .AddScoped<IFileDeduPlan, FileDeduPlan>()
                .AddScoped<IFileDeduCobertura, FileDeduCobertura>()                
                .AddScoped<IFileDeduCobDet, FileDeduCobDet>()
                .AddScoped<IFileDeduFamiliar, FileDeduFamiliar>();
        }
    }
}
