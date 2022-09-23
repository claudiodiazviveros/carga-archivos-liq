using CargaArchivoLiquidadores.Interfaces;
using Dapper;
using Microsoft.Extensions.Configuration;
using Serilog;
using System;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Threading.Tasks;

namespace CargaArchivoLiquidadores.Activities
{
    public class FileDeduFamiliar : IFileDeduFamiliar
    {
        private readonly IConfiguration _configuration;
        private readonly string folderIn = $@"{Environment.CurrentDirectory}\Input";
        private readonly string folderOut = $@"{Environment.CurrentDirectory}\Output";

        public FileDeduFamiliar(IConfiguration configuration)
        {
            _configuration = configuration;
        }

        public async Task CreateScript()
        {
            DirectoryInfo di = new DirectoryInfo(folderIn);
            foreach (var fi in di.GetFiles("DEDU_FAMILIAR*.txt"))
            {
                // Lee todas las lineas del archivo plano.
                string[] lines = await File.ReadAllLinesAsync(fi.FullName);

                // Importa lineas en clase.
                var deduFamiliars = DeduFamiliar.Import(lines);

                // Crea instrucciones sql.
                var result = DeduFamiliar.StatementSql(deduFamiliars);

                // Guarda instrucciones sql en archivo script.
                await File.WriteAllTextAsync($@"{folderOut}\DEDU_FAMILIAR.sql", result);
            }
        }

        private bool LoadData()
        {
            int succeedCount = 0;
            int failedCount = 0;
            int currentRow = 2;

            string folder = $@"{Environment.CurrentDirectory}\INBOX";

            DirectoryInfo di = new DirectoryInfo(folder);
            foreach (var fi in di.GetFiles("DEDU_FAMILIAR*.txt"))
            {
                Log.Information("Inicio Carga de archivo: DEDU_FAMILIAR");

                using (var file = new StreamReader(fi.FullName))
                {
                    string row = file.ReadLine();   // Excluye primera Linea Resumen

                    while ((row = file.ReadLine()) != null)
                    {
                        string[] arrayFields = row.Split('|');

                        var aggregated = AddRecord(arrayFields, currentRow);

                        succeedCount = succeedCount + (aggregated ? 1 : 0);
                        failedCount = failedCount + (!aggregated ? 1 : 0);
                        currentRow++;
                    }

                    Log.Information($"Registros correctos: {succeedCount}");
                    Log.Information($"Registros erroneos: {failedCount}");
                    Log.Information($"Registros totales: {succeedCount + failedCount}");
                }

                Log.Information("Termino Carga de archivo: DEDU_FAMILIAR");
            }

            return true;
        }

        private bool AddRecord(string[] fields, int currentRow)
        {
            try
            {
                using (var connection = new SqlConnection(_configuration.GetConnectionString("DataConnection")))
                {
                    if (connection.State == ConnectionState.Closed)
                        connection.Open();

                    string query = $"INSERT INTO [dbo].[DEDUCIBLE_FAMILIAR] ([DF_PROVEEDOR], [DF_POLIZA], [DF_CODIGO_PLAN], [DF_TIPO_DEDUCIBLE], [DF_PLAZO], [DF_TIT_SOLO], [DF_TIT_1_CARGA], [DF_TIT_2_CARGAS], [DF_TIT_2_O_MAS], [DF_TIT_3_CARGAS], [DF_TIT_3_O_MAS], [DF_TIT_4_CARGAS], [DF_TIT_5_O_MAS], [DF_FEC_MODIFICACION], [DF_FEC_CREACION], [DF_FEC_INICIO_VIG], [DF_FEC_TERMINO_VIG], [DF_PLAN_PERSONAL], [DF_CODIGO_GRUPO]) VALUES (@DF_PROVEEDOR, @DF_POLIZA, @DF_CODIGO_PLAN, @DF_TIPO_DEDUCIBLE, @DF_PLAZO, @DF_TIT_SOLO, @DF_TIT_1_CARGA, @DF_TIT_2_CARGAS, @DF_TIT_2_O_MAS, @DF_TIT_3_CARGAS, @DF_TIT_3_O_MAS, @DF_TIT_4_CARGAS, @DF_TIT_5_O_MAS, @DF_FEC_MODIFICACION, @DF_FEC_CREACION, @DF_FEC_INICIO_VIG, @DF_FEC_TERMINO_VIG, @DF_PLAN_PERSONAL, @DF_CODIGO_GRUPO)";

                    var parameters = new DynamicParameters();
                    parameters.Add("DF_PROVEEDOR", fields[0], DbType.String);
                    parameters.Add("DF_POLIZA", fields[1], DbType.String);
                    parameters.Add("DF_CODIGO_PLAN", fields[2], DbType.String);
                    parameters.Add("DF_TIPO_DEDUCIBLE", fields[3], DbType.String);
                    parameters.Add("DF_PLAZO", fields[4], DbType.String);
                    parameters.Add("DF_TIT_SOLO", Convert.ToDecimal(fields[5]), DbType.Decimal);
                    parameters.Add("DF_TIT_1_CARGA", Convert.ToDecimal(fields[6]), DbType.Decimal);
                    parameters.Add("DF_TIT_2_CARGAS", Convert.ToDecimal(fields[7]), DbType.Decimal);
                    parameters.Add("DF_TIT_2_O_MAS", Convert.ToDecimal(fields[8]), DbType.Decimal);
                    parameters.Add("DF_TIT_3_CARGAS", Convert.ToDecimal(fields[9]), DbType.Decimal);
                    parameters.Add("DF_TIT_3_O_MAS", Convert.ToDecimal(fields[10]), DbType.Decimal);
                    parameters.Add("DF_TIT_4_CARGAS", Convert.ToDecimal(fields[11]), DbType.Decimal);
                    parameters.Add("DF_TIT_5_O_MAS", Convert.ToDecimal(fields[12]), DbType.Decimal);
                    parameters.Add("DF_FEC_MODIFICACION", DBNull.Value, DbType.String);
                    parameters.Add("DF_FEC_CREACION", Convert.ToDateTime(fields[13]), DbType.Date);
                    parameters.Add("DF_FEC_INICIO_VIG", fields[14], DbType.String);
                    parameters.Add("DF_FEC_TERMINO_VIG", fields[15], DbType.String);
                    parameters.Add("DF_PLAN_PERSONAL", fields[16], DbType.String);
                    parameters.Add("DF_CODIGO_GRUPO", fields[17], DbType.String);
                    connection.Execute(query, commandType: CommandType.Text, param: parameters);
                }
                return true;
            }
            catch (Exception ex)
            {
                Log.Error($"Fila [{currentRow}] {ex.Message}");
            }

            return false;
        }
    }
}
