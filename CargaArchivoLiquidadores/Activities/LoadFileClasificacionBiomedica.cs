using CargaArchivoLiquidadores.Interfaces;
using Dapper;
using Microsoft.Extensions.Configuration;
using System;
using System.Data;
using System.Data.SqlClient;
using System.IO;

namespace CargaArchivoLiquidadores.Activities
{
    public class LoadFileClasificacionBiomedica: ILoadFileClasificacionBiomedica
    {
        private readonly IConfiguration _configuration;

        public LoadFileClasificacionBiomedica(IConfiguration configuration)
        {
            _configuration = configuration;
        }

        public bool LoadData()
        {
            string folder = $@"{Environment.CurrentDirectory}\INBOX";

            DirectoryInfo di = new DirectoryInfo(folder);
            foreach (var fi in di.GetFiles("CLASIF_BIOMEDICA*.txt"))
            {
                using (var file = new StreamReader(fi.FullName))
                {
                    string row;

                    file.ReadLine();

                    using (var connection = new SqlConnection(_configuration.GetConnectionString("DataConnection")))
                    {
                        if (connection.State == ConnectionState.Closed)
                            connection.Open();

                        //using (SqlTransaction tran = connection.BeginTransaction())
                        //{
                        while ((row = file.ReadLine()) != null)
                        {
                            var campos = row.Split('|');

                            try
                            {
                                string insertQuery = @"INSERT INTO [dbo].[CLASIFICACION_BIOMEDICA]([CLBI_COD_CLASIFICACION], [CLBI_DES_CLASIFICACION], [CLBI_USU_ULT_ACT], [CLBI_USU_CREACION], [CLBI_FEC_ULT_ACT], [CLBI_FEC_CREACION], [CLBI_ES_VIGENTE], [CLBI_FEC_EXTRACCION], [CLBI_ORIGEN_DATO]) VALUES (@CLBI_COD_CLASIFICACION, @CLBI_DES_CLASIFICACION, @CLBI_USU_ULT_ACT, @CLBI_USU_CREACION, @CLBI_FEC_ULT_ACT, @CLBI_FEC_CREACION, @CLBI_ES_VIGENTE, @CLBI_FEC_EXTRACCION, @CLBI_ORIGEN_DATO)";

                                var selectQuery = $"SELECT ISNULL([CLBI_ID_CLASIFICACION], 0) FROM [dbo].[CLASIFICACION_BIOMEDICA] WHERE [CLBI_COD_CLASIFICACION]= @idClasificacion";
                                var clasificacionID = connection.Query(selectQuery, new
                                {
                                    idClasificacion = campos[0]
                                });

                                if (clasificacionID.AsList().Count == 0)
                                {
                                    var result = connection.Execute(insertQuery, new
                                    {
                                        CLBI_COD_CLASIFICACION = campos[0],
                                        CLBI_DES_CLASIFICACION = campos[1],
                                        CLBI_USU_ULT_ACT = "BATCH",
                                        CLBI_USU_CREACION = "BATCH",
                                        CLBI_FEC_ULT_ACT = DateTime.Now,
                                        CLBI_FEC_CREACION = DateTime.Now,
                                        CLBI_ES_VIGENTE = true,
                                        CLBI_FEC_EXTRACCION = campos[2],
                                        CLBI_ORIGEN_DATO = "P"
                                    });
                                }
                            }
                            catch (Exception ex)
                            {
                                //tran.Rollback();
                                throw;
                            }
                        }

                        //tran.Commit();
                        //}

                    }
                }
            }

            return true;
        }
    }
}
