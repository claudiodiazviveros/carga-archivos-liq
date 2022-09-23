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
    public class FileMedicamentos : IFileMedicamentos
    {
        private readonly IConfiguration _configuration;
        private readonly string folderIn = $@"{Environment.CurrentDirectory}\Input";
        private readonly string folderOut = $@"{Environment.CurrentDirectory}\Output";

        public FileMedicamentos(IConfiguration configuration)
        {
            _configuration = configuration;
        }

        public async Task CreateScript()
        {
            DirectoryInfo di = new DirectoryInfo(folderIn);
            foreach (var fi in di.GetFiles("MEDICAMENTOS*.txt"))
            {
                // Lee todas las lineas del archivo plano.
                string[] lines = await File.ReadAllLinesAsync(fi.FullName);

                // Importa lineas en clase.
                var medicamentos = Medicamento.Import(lines);

                // Crea instrucciones sql.
                var result = Medicamento.StatementSql(medicamentos);

                // Guarda instrucciones sql en archivo script.
                await File.WriteAllTextAsync($@"{folderOut}\MEDICAMENTOS.sql", result);
            }
        }

        public void SaveScript()
        {
            int currentRow = 2;
            string folderIn = $@"{Environment.CurrentDirectory}\INBOX";
            string folderOut = $@"{Environment.CurrentDirectory}\OUTPUT";

            DirectoryInfo di = new DirectoryInfo(folderIn);
            foreach (var fi in di.GetFiles("MEDICAMENTOS*.txt"))
            {
                Log.Information("Inicio Script: MEDICAMENTOS");

                using (var file = new StreamReader(fi.FullName))
                using (StreamWriter sw = new StreamWriter($@"{folderOut}\MEDICAMENTOS.sql"))
                {
                    string row = file.ReadLine();   // Excluye primera Linea Resumen

                    while ((row = file.ReadLine()) != null)
                    {
                        string[] arrayFields = row.Split('|');
                        string line = QueryRow(currentRow, arrayFields);
                        var result = line != "" ? true : false;

                        if (result)
                            sw.WriteLine(line);

                        currentRow++;
                    }
                }

                Log.Information("Termino Script: MEDICAMENTOS");
            }
        }

        private string QueryRow(int currentRow, string[] campos)
        {
            using (var connection = new SqlConnection(_configuration.GetConnectionString("DataConnection")))
            {
                var selectQuery = $"SELECT ISNULL([CLBI_ID_CLASIFICACION], 0) FROM [dbo].[MEDICAMENTO] WHERE MDTO_COD_MEDICAMENTO = @codMedicamento";
                var medicamentoID = connection.Query(selectQuery, new
                {
                    codMedicamento = campos[0],
                });

                if (medicamentoID.AsList().Count == 0)
                {
                    var selectQueryBiomedico = $"SELECT TOP(1) ISNULL([CLBI_ID_CLASIFICACION], 0) FROM [dbo].[CLASIFICACION_BIOMEDICA] WHERE [CLBI_COD_CLASIFICACION]= @idClasificacion";
                    var clasificacionID = connection.Query<Int32>(selectQueryBiomedico, new
                    {
                        idClasificacion = campos[2]
                    });

                    int idClass = Convert.ToInt32(clasificacionID.FirstOrDefault());

                    if (idClass == 0)
                    {
                        Log.Information($"Falta Clasificación BioMédica {campos[0]} - {campos[1]}: en la línea  {currentRow}, se debe agregar en la tabla maestra");
                    }

                    int vigencia = 1;

                    string insertQuery = @"INSERT INTO [dbo].[MEDICAMENTO]([CLBI_ID_CLASIFICACION],[MDTO_COD_MEDICAMENTO],[MDTO_DES_MEDICAMENTO],[MDTO_USU_ULT_ACT],[MDTO_USU_CREACION],[MDTO_FEC_ULT_ACT],[MDTO_FEC_CREACION] ,[MDTO_ES_VIGENTE],[MDTO_ORIGEN_DATO]) ";

                    string defaultValues = $" VALUES('{idClass}', '{campos[0]}', '{campos[1]}', 'BATCH', 'BATCH', '{DateTime.Now.ToString("yyyyMMdd")}', '{campos[3]}', {vigencia},'P')";

                    return insertQuery + defaultValues;
                }
            }

            return "";
        }
    }
}
