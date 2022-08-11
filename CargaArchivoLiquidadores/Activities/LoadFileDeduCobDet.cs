using CargaArchivoLiquidadores.Interfaces;
using Microsoft.Extensions.Configuration;
using System;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using Dapper;
using Serilog;

namespace CargaArchivoLiquidadores.Activities
{
    public class LoadFileDeduCobDet : ILoadFileDeduCobDet
    {
        private readonly IConfiguration _configuration;

        public LoadFileDeduCobDet(IConfiguration configuration)
        {
            _configuration = configuration;
        }

        public bool LoadData()
        {
            int succeedCount = 0;
            int failedCount = 0;
            int currentRow = 2;

            string folder = $@"{Environment.CurrentDirectory}\INBOX";

            DirectoryInfo di = new DirectoryInfo(folder);
            foreach (var fi in di.GetFiles("DEDU_COB_DET*.txt"))
            {
                Log.Information("Inicio Carga de archivo: DEDU_COB_DET");

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

                Log.Information("Termino Carga de archivo: DEDU_COB_DET");
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

                    string query = $"INSERT INTO [dbo].[DEDUCIBLE_COBERTURA_DET] ([DCD_PROVEEDOR], [DCD_POLIZA], [DCD_CODIGO_PLAN], [DCD_COD_COBERTURA], [DCD_COD_GRUPO], [DCD_NOMBRE_GRUPO], [DCD_TIPO_COB], [DCD_FEC_MODIFICACION], [DCD_FEC_CREACION]) VALUES (@DCD_PROVEEDOR, @DCD_POLIZA, @DCD_CODIGO_PLAN, @DCD_COD_COBERTURA, @DCD_COD_GRUPO, @DCD_NOMBRE_GRUPO, @DCD_TIPO_COB, @DCD_FEC_MODIFICACION, @DCD_FEC_CREACION)";

                    var parameters = new DynamicParameters();
                    parameters.Add("DCD_PROVEEDOR", fields[0], DbType.String);
                    parameters.Add("DCD_POLIZA", fields[1], DbType.String);
                    parameters.Add("DCD_CODIGO_PLAN", fields[2], DbType.String);
                    parameters.Add("DCD_COD_COBERTURA", fields[3], DbType.String);
                    parameters.Add("DCD_COD_GRUPO", fields[4], DbType.String);
                    parameters.Add("DCD_NOMBRE_GRUPO", fields[5], DbType.String);
                    parameters.Add("DCD_TIPO_COB", "NO_FARMACIA", DbType.String);
                    parameters.Add("DCD_FEC_MODIFICACION", DBNull.Value, DbType.String);
                    parameters.Add("DCD_FEC_CREACION", Convert.ToDateTime(fields[6]), DbType.DateTime);
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
