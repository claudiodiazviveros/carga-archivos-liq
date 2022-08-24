using CargaArchivoLiquidadores.Interfaces;
using Dapper;
using Microsoft.Extensions.Configuration;
using Serilog;
using System;
using System.Data.SqlClient;
using System.IO;
using System.Linq;

namespace CargaArchivoLiquidadores.Activities
{
    public class LoadFileMedicamentos : ILoadFileMedicamentos
    {
        private readonly IConfiguration _configuration;

        public LoadFileMedicamentos(IConfiguration configuration)
        {
            _configuration = configuration;
        }

        //public bool LoadData()
        //{
        //    string folder = $@"{Environment.CurrentDirectory}\INBOX";

        //    DirectoryInfo di = new DirectoryInfo(folder);
        //    foreach (var fi in di.GetFiles("MEDICAMENTOS*.txt"))
        //    {
        //        using (var file = new StreamReader(fi.FullName))
        //        {
        //            string row;

        //            file.ReadLine();

        //            using (var connection = new SqlConnection(_configuration.GetConnectionString("DataConnection")))
        //            {
        //                while ((row = file.ReadLine()) != null)
        //                {
        //                    var campos = row.Split('|');

        //                    try
        //                    {
        //                        var selectQuery = $"SELECT ISNULL([CLBI_ID_CLASIFICACION], 0) FROM [dbo].[MEDICAMENTO] WHERE MDTO_COD_MEDICAMENTO = @codMedicamento";
        //                        var medicamentoID = connection.Query(selectQuery, new
        //                        {
        //                            codMedicamento = campos[0],
        //                        });


        //                        if (medicamentoID.AsList().Count == 0)
        //                        {
        //                            var selectQueryBiomedico = $"SELECT TOP(1) ISNULL([CLBI_ID_CLASIFICACION], 0) FROM [dbo].[CLASIFICACION_BIOMEDICA] WHERE [CLBI_COD_CLASIFICACION]= @idClasificacion";
        //                            var clasificacionID = connection.Query<Int32>(selectQueryBiomedico, new
        //                            {
        //                                idClasificacion = campos[2]
        //                            });

        //                            int idClass = Convert.ToInt32(clasificacionID.FirstOrDefault());

        //                            AddRecord(connection, idClass, campos);
        //                        }
        //                    }
        //                    catch (Exception ex)
        //                    {
        //                        Log.Error($"Fila [{row}] {ex.Message}");
        //                    }

        //                }
        //            }

        //        }
        //    }

        //    return true;
        //}

        //private bool AddRecord(SqlConnection connection, int idClass, string[] campos)
        //{
        //    string insertQuery = @"INSERT INTO [dbo].[MEDICAMENTO]([CLBI_ID_CLASIFICACION],[MDTO_COD_MEDICAMENTO],[MDTO_DES_MEDICAMENTO],[MDTO_USU_ULT_ACT],[MDTO_USU_CREACION],[MDTO_FEC_ULT_ACT],[MDTO_FEC_CREACION] ,[MDTO_ES_VIGENTE],[MDTO_ORIGEN_DATO]) VALUES (@CLBI_ID_CLASIFICACION, @MDTO_COD_MEDICAMENTO, @MDTO_DES_MEDICAMENTO, @MDTO_USU_ULT_ACT, @MDTO_USU_CREACION, @MDTO_FEC_ULT_ACT, @MDTO_FEC_CREACION , @MDTO_ES_VIGENTE, @MDTO_ORIGEN_DATO)";

        //    try
        //    {
        //        connection.Execute(insertQuery, new
        //        {
        //            CLBI_ID_CLASIFICACION = idClass,
        //            MDTO_COD_MEDICAMENTO = campos[0],
        //            MDTO_DES_MEDICAMENTO = campos[1],
        //            MDTO_USU_ULT_ACT = "BATCH",
        //            MDTO_USU_CREACION = "BATCH",
        //            MDTO_FEC_ULT_ACT = DateTime.Now,
        //            MDTO_FEC_CREACION = campos[3],
        //            MDTO_ES_VIGENTE = true,
        //            MDTO_ORIGEN_DATO = "P"
        //        });
        //        return true;
        //    }
        //    catch (Exception ex)
        //    {
        //        Log.Error($"Fila [{campos}] {ex.Message}");
        //    }
        //    return false;
        //}

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
