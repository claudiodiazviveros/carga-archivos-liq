using CargaArchivoLiquidadores.Interfaces;
using Microsoft.Extensions.Configuration;
using Serilog;
using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace CargaArchivoLiquidadores.Activities
{
    public class FileDeduCobertura : IFileDeduCobertura
    {
        private readonly IConfiguration _configuration;
        private readonly string folderIn = $@"{Environment.CurrentDirectory}\Input";
        private readonly string folderOut = $@"{Environment.CurrentDirectory}\Output";

        public FileDeduCobertura(IConfiguration configuration)
        {
            _configuration = configuration;
        }

        public async Task CreateScript()
        {
            DirectoryInfo di = new DirectoryInfo(folderIn);
            foreach (var fi in di.GetFiles("DEDU_COBERTURA*.txt"))
            {
                Log.Information("Inicia Deducibles coberturas");

                // Lee todas las lineas del archivo plano.
                string[] lines = await File.ReadAllLinesAsync(fi.FullName);
                Log.Information($"Registros en archivo: {lines.Count()}");

                // Importa lineas en clase.
                var deduCoberturas = DeduCobertura.Import(lines);

                // Crea instrucciones sql.
                var result = DeduCobertura.StatementSql(deduCoberturas);

                // Guarda instrucciones sql en archivo script.
                await File.WriteAllTextAsync($@"{folderOut}\DEDU_COBERTURA.sql", result);

                Log.Information("Termino Deducibles coberturas");
            }
        }
    }
}
