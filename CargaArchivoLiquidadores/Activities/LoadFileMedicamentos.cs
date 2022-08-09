using Dapper;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;

namespace CargaArchivoLiquidadores.Activities
{
    public class LoadFileMedicamentos
    {
        public static bool LoadData(string conexion)
        {
            string folder = $@"{Environment.CurrentDirectory}\INBOX";

            DirectoryInfo di = new DirectoryInfo(folder);
            foreach (var fi in di.GetFiles(""))
            {
                using (var file = new StreamReader(fi.FullName))
                {
                    string row;

                    file.ReadLine();

                    using (var connection = new SqlConnection(conexion))
                    
                    while ((row = file.ReadLine()) != null)
                    {
                        var campos = row.Split('|');

                        try
                        {
                            string insertQuery = @"INSERT INTO [dbo].[MEDICAMENTO]([CLBI_ID_CLASIFICACION],[MDTO_COD_MEDICAMENTO],[MDTO_DES_MEDICAMENTO],[MDTO_USU_ULT_ACT],[MDTO_USU_CREACION],[MDTO_FEC_ULT_ACT],[MDTO_FEC_CREACION] ,[MDTO_ES_VIGENTE],[MDTO_ORIGEN_DATO]) VALUES (@CLBI_ID_CLASIFICACION, @MDTO_COD_MEDICAMENTO, @MDTO_DES_MEDICAMENTO, @MDTO_USU_ULT_ACT, @MDTO_USU_CREACION, @MDTO_FEC_ULT_ACT, @MDTO_FEC_CREACION , @MDTO_ES_VIGENTE, @MDTO_ORIGEN_DATO)";

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

                                var result = connection.Execute(insertQuery, new
                                {
                                    CLBI_ID_CLASIFICACION = idClass,
                                    MDTO_COD_MEDICAMENTO = campos[0],
                                    MDTO_DES_MEDICAMENTO = campos[1],
                                    MDTO_USU_ULT_ACT = "BATCH",
                                    MDTO_USU_CREACION = "BATCH",
                                    MDTO_FEC_ULT_ACT = DateTime.Now,
                                    MDTO_FEC_CREACION = campos[3],
                                    MDTO_ES_VIGENTE = true,
                                    MDTO_ORIGEN_DATO = "P"
                                });
                            }
                        }
                        catch (Exception ex)
                        {
                            throw;
                        }

                    }
                }
            }

            return true;
        }
    }
}
