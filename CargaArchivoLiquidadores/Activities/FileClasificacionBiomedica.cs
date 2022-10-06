using CargaArchivoLiquidadores.Interfaces;
using Dapper;
using Microsoft.Extensions.Configuration;
using Serilog;
using System;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace CargaArchivoLiquidadores.Activities
{
    public class FileClasificacionBiomedica : IFileClasificacionBiomedica
    {
        private readonly IConfiguration _configuration;
        private readonly string folderIn = $@"{Environment.CurrentDirectory}\Input";
        private readonly string folderOut = $@"{Environment.CurrentDirectory}\Output";

        public FileClasificacionBiomedica(IConfiguration configuration)
        {
            _configuration = configuration;
        }

        public async Task CreateScript()
        {
            DirectoryInfo di = new DirectoryInfo(folderIn);
            foreach (var fi in di.GetFiles("CLASIF_BIOMEDICA*.txt"))
            {
                Log.Information("Inicia Clasificaciones Biomedica");

                // Lee todas las lineas del archivo plano.
                string[] lines = await File.ReadAllLinesAsync(fi.FullName);
                Log.Information($"Registros en archivo: {lines.Count()}");

                // Importa lineas en clase.
                var clasifBiomedicas = ClasifBiomedica.Import(lines);

                // Crea instrucciones sql.
                var result = ClasifBiomedica.StatementSql(clasifBiomedicas);

                // Guarda instrucciones sql en archivo script.
                await File.WriteAllTextAsync($@"{folderOut}\CLASIFICACION_BIOMEDICA.sql", result);

                Log.Information("Termino Clasificaciones Biomedica");
            }
        }

        private string QueryRow(int currentRow, string[] campos)
        {
            using (var connection = new SqlConnection(_configuration.GetConnectionString("DataConnection")))
            {
                var selectQuery = $"SELECT ISNULL([CLBI_ID_CLASIFICACION], 0) FROM [dbo].[CLASIFICACION_BIOMEDICA] WHERE [CLBI_COD_CLASIFICACION] = @idClasificacion";
                var clasificacionID = connection.Query<Int32>(selectQuery, new
                {
                    idClasificacion = campos[0].Trim()
                });

                if (clasificacionID.FirstOrDefault() == 0)
                {
                    int vigencia = 1;

                    string insertQuery = $"INSERT INTO [dbo].[CLASIFICACION_BIOMEDICA]([CLBI_COD_CLASIFICACION], [CLBI_DES_CLASIFICACION], [CLBI_USU_ULT_ACT], [CLBI_USU_CREACION], [CLBI_FEC_ULT_ACT], [CLBI_FEC_CREACION], [CLBI_ES_VIGENTE], [CLBI_FEC_EXTRACCION], [CLBI_ORIGEN_DATO]) ";
                    string defaultValues = $" VALUES('{campos[0]}', '{campos[1]}', 'BATCH', 'BATCH', '{DateTime.Now.ToString("yyyyMMdd")}', '{DateTime.Now.ToString("yyyyMMdd")}', {vigencia}, '{campos[2]}', 'P')";

                    return insertQuery + defaultValues;
                }
            }

            return "";
        }
    }
}
