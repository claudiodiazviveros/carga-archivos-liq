using CargaArchivoLiquidadores.Interfaces;
using Microsoft.Extensions.Configuration;
using Serilog;
using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace CargaArchivoLiquidadores.Activities
{
    public class FileCarta : IFileCarta
    {
        private readonly IConfiguration _configuration;
        private readonly string folderIn = $@"{Environment.CurrentDirectory}\Input";
        private readonly string folderOut = $@"{Environment.CurrentDirectory}\Output";

        public FileCarta(IConfiguration configuration)
        {
            _configuration = configuration;
        }

        public async Task CreateScript()
        {
            DirectoryInfo di = new DirectoryInfo(folderIn);
            foreach (var fi in di.GetFiles("CARTAS*.txt"))
            {
                Log.Information("Inicia Cartas de rechazo");

                // Lee todas las lineas del archivo plano.
                string[] lines = await File.ReadAllLinesAsync(fi.FullName);
                Log.Information($"Registros en archivo: {lines.Count()}");

                // Importa lineas en clase.
                var cartas = Carta.Import(lines);

                // Crea instrucciones sql.
                var result = Carta.StatementSql(cartas);

                // Guarda instrucciones sql en archivo script.
                await File.WriteAllTextAsync($@"{folderOut}\CARTAS.sql", result);

                Log.Information("Termino Cartas de rechazo");
            }
        }
    }
}
