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
    public class LoadFileDeduPlan : ILoadFileDeduPlan
    {
        private readonly IConfiguration _configuration;

        public LoadFileDeduPlan(IConfiguration configuration)
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
            foreach (var fi in di.GetFiles("DEDU_PLAN*.txt"))
            {
                Log.Information("Inicio Carga de archivo: DEDU_PLAN");

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

                Log.Information("Termino Carga de archivo: DEDU_PLAN");
            }

            return true;
        }

        public void SaveScript()
        {
            string folderIn = $@"{Environment.CurrentDirectory}\INBOX";
            string folderOut = $@"{Environment.CurrentDirectory}\OUT";

            DirectoryInfo di = new DirectoryInfo(folderIn);
            foreach (var fi in di.GetFiles("DEDU_PLAN*.txt"))
            {
                Log.Information("Inicio Script: DEDU_PLAN");

                using (var file = new StreamReader(fi.FullName))
                using (StreamWriter sw = new StreamWriter($@"{folderOut}\DEDU_PLAN.sql"))
                {
                    string row = file.ReadLine();   // Excluye primera Linea Resumen

                    while ((row = file.ReadLine()) != null)
                    {
                        string[] arrayFields = row.Split('|');
                        string line = QueryRow(arrayFields);

                        sw.WriteLine(line);
                    }
                }

                Log.Information("Termino Script: DEDU_PLAN");
            }
        }

        private bool AddRecord(string[] fields, int currentRow)
        {
            try
            {
                using (var connection = new SqlConnection(_configuration.GetConnectionString("DataConnection")))
                {
                    if (connection.State == ConnectionState.Closed)
                        connection.Open();

                    string query = $"INSERT INTO [dbo].[DEDUCIBLE_PLAN] ([DP_PROVEEDOR], [DP_POLIZA], [DP_CONTRATANTE], [DP_CODIGO_PLAN], [DP_TIPO_DEDUCIBLE], [DP_MAX_DEDUCIBLE], [DP_PLAZO], [DP_MAX_PERSONA], [DP_MAX_GRUPO], [DP_TOPE_GENERAL_PLAN], [DP_FEC_MODIFICACION], [DP_FEC_CREACION], [DP_FEC_INICIO_VIG], [DP_FEC_TERMINO_VIG], [DP_PLAN_PERSONAL], [DP_CODIGO_GRUPO]) VALUES(@DP_PROVEEDOR, @DP_POLIZA, @DP_CONTRATANTE, @DP_CODIGO_PLAN, @DP_TIPO_DEDUCIBLE, @DP_MAX_DEDUCIBLE, @DP_PLAZO, @DP_MAX_PERSONA, @DP_MAX_GRUPO, @DP_TOPE_GENERAL_PLAN, @DP_FEC_MODIFICACION, @DP_FEC_CREACION, @DP_FEC_INICIO_VIG, @DP_FEC_TERMINO_VIG, @DP_PLAN_PERSONAL, @DP_CODIGO_GRUPO)";

                    var parameters = new DynamicParameters();
                    parameters.Add("DP_PROVEEDOR", fields[0], DbType.String);
                    parameters.Add("DP_POLIZA", fields[1], DbType.String);
                    parameters.Add("DP_CONTRATANTE", fields[2], DbType.String);
                    parameters.Add("DP_CODIGO_PLAN", fields[3], DbType.String);
                    parameters.Add("DP_TIPO_DEDUCIBLE", fields[4], DbType.String);
                    parameters.Add("DP_MAX_DEDUCIBLE", Convert.ToDecimal(fields[5]), DbType.Decimal);
                    parameters.Add("DP_PLAZO", Convert.ToInt32(fields[6]), DbType.Int32);
                    parameters.Add("DP_MAX_PERSONA", Convert.ToInt32(fields[7]), DbType.Int32);
                    parameters.Add("DP_MAX_GRUPO", Convert.ToDecimal(fields[8]), DbType.Decimal);
                    parameters.Add("DP_TOPE_GENERAL_PLAN", Convert.ToDecimal(fields[9]), DbType.Decimal);
                    parameters.Add("DP_FEC_MODIFICACION", fields[10], DbType.String);
                    parameters.Add("DP_FEC_INICIO_VIG", fields[11], DbType.String);
                    parameters.Add("DP_FEC_TERMINO_VIG", fields[12], DbType.String);
                    parameters.Add("DP_PLAN_PERSONAL", fields[13], DbType.String);
                    parameters.Add("DP_CODIGO_GRUPO", Convert.ToInt32(fields[14]), DbType.Int32);
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

        private string QueryRow(string[] fields)
        {
            string DP_PROVEEDOR = fields[0];
            string DP_POLIZA = fields[1];



            //parameters.Add("DP_POLIZA", fields[1], DbType.String);
            //parameters.Add("DP_CONTRATANTE", fields[2], DbType.String);
            //parameters.Add("DP_CODIGO_PLAN", fields[3], DbType.String);
            //parameters.Add("DP_TIPO_DEDUCIBLE", fields[4], DbType.String);
            //parameters.Add("DP_MAX_DEDUCIBLE", Convert.ToDecimal(fields[5]), DbType.Decimal);
            //parameters.Add("DP_PLAZO", Convert.ToInt32(fields[6]), DbType.Int32);
            //parameters.Add("DP_MAX_PERSONA", Convert.ToInt32(fields[7]), DbType.Int32);
            //parameters.Add("DP_MAX_GRUPO", Convert.ToDecimal(fields[8]), DbType.Decimal);
            //parameters.Add("DP_TOPE_GENERAL_PLAN", Convert.ToDecimal(fields[9]), DbType.Decimal);
            //parameters.Add("DP_FEC_MODIFICACION", fields[10], DbType.String);
            //parameters.Add("DP_FEC_INICIO_VIG", fields[11], DbType.String);
            //parameters.Add("DP_FEC_TERMINO_VIG", fields[12], DbType.String);
            //parameters.Add("DP_PLAN_PERSONAL", fields[13], DbType.String);
            //parameters.Add("DP_CODIGO_GRUPO", Convert.ToInt32(fields[14]), DbType.Int32);



            string query = $"INSERT INTO [dbo].[DEDUCIBLE_PLAN] ([DP_PROVEEDOR], [DP_POLIZA], [DP_CONTRATANTE], [DP_CODIGO_PLAN], [DP_TIPO_DEDUCIBLE], [DP_MAX_DEDUCIBLE], [DP_PLAZO], [DP_MAX_PERSONA], [DP_MAX_GRUPO], [DP_TOPE_GENERAL_PLAN], [DP_FEC_MODIFICACION], [DP_FEC_CREACION], [DP_FEC_INICIO_VIG], [DP_FEC_TERMINO_VIG], [DP_PLAN_PERSONAL], [DP_CODIGO_GRUPO]) VALUES('{DP_PROVEEDOR}', '{DP_POLIZA}', @DP_CONTRATANTE, @DP_CODIGO_PLAN, @DP_TIPO_DEDUCIBLE, @DP_MAX_DEDUCIBLE, @DP_PLAZO, @DP_MAX_PERSONA, @DP_MAX_GRUPO, @DP_TOPE_GENERAL_PLAN, @DP_FEC_MODIFICACION, @DP_FEC_CREACION, @DP_FEC_INICIO_VIG, @DP_FEC_TERMINO_VIG, @DP_PLAN_PERSONAL, @DP_CODIGO_GRUPO)";


            return query;
        }
    }
}
