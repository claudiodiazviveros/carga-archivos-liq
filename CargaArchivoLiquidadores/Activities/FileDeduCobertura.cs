using CargaArchivoLiquidadores.Interfaces;
using Microsoft.Extensions.Configuration;
using System;
using System.IO;
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
                // Lee todas las lineas del archivo plano.
                string[] lines = await File.ReadAllLinesAsync(fi.FullName);

                // Importa lineas en clase.
                var deduCoberturas = DeduCobertura.Import(lines);

                // Crea instrucciones sql.
                var result = DeduCobertura.StatementSql(deduCoberturas);

                // Guarda instrucciones sql en archivo script.
                await File.WriteAllTextAsync($@"{folderOut}\DEDU_COBERTURA.sql", result);
            }
        }
    }
}
