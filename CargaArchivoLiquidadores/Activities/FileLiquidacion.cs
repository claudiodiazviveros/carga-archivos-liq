using CargaArchivoLiquidadores.Interfaces;
using Microsoft.Extensions.Configuration;
using Serilog;
using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace CargaArchivoLiquidadores.Activities
{
    public class FileLiquidacion : IFileLiquidacion
    {
        private readonly IConfiguration _configuration;
        private readonly string folderIn = $@"{Environment.CurrentDirectory}\Input";
        private readonly string folderOut = $@"{Environment.CurrentDirectory}\Output";

        public FileLiquidacion(IConfiguration configuration)
        {
            _configuration = configuration;
        }

        public async Task CreateScript()
        {
            DirectoryInfo di = new DirectoryInfo(folderIn);
            foreach (var fi in di.GetFiles("LIQUIDACIONES*.txt"))
            {
                Log.Information("Inicia Liquidaciones");

                // Lee todas las lineas del archivo plano.
                string[] lines = await File.ReadAllLinesAsync(fi.FullName);
                Log.Information($"Registros en archivo: {lines.Count()}");

                // Importa lineas en clase.
                var liquidaciones = Liquidacion.Import(lines);

                // Crea instrucciones sql 'REMESA, SOLICITUD, DETALLE_SOLICITUD'.
                var result = Liquidacion.StatementSql(liquidaciones);

                // Guarda instrucciones sql en archivo script.
                await File.WriteAllTextAsync($@"{folderOut}\LIQUIDACIONES.sql", result);

                Log.Information("Termino Liquidaciones");
            }
        }
    }
}
