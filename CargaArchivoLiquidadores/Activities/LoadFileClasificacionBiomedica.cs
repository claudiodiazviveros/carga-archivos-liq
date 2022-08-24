using CargaArchivoLiquidadores.Interfaces;
using Dapper;
using Microsoft.Extensions.Configuration;
using Serilog;
using System;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;

namespace CargaArchivoLiquidadores.Activities
{
    public class LoadFileClasificacionBiomedica: ILoadFileClasificacionBiomedica
    {
        private readonly IConfiguration _configuration;

        public LoadFileClasificacionBiomedica(IConfiguration configuration)
        {
            _configuration = configuration;
        }

        public void SaveScript()
        {
            int currentRow = 2;
            string folderIn = $@"{Environment.CurrentDirectory}\INBOX";
            string folderOut = $@"{Environment.CurrentDirectory}\OUTPUT";

            DirectoryInfo di = new DirectoryInfo(folderIn);
            foreach (var fi in di.GetFiles("CLASIF_BIOMEDICA*.txt"))
            {
                Log.Information("Inicio Script: CLASIF_BIOMEDICA");

                using (var file = new StreamReader(fi.FullName))
                using (StreamWriter sw = new StreamWriter($@"{folderOut}\CLASIFICACION_BIOMEDICA.sql"))
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

                Log.Information("Termino Script: CLASIF_BIOMEDICA");
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
