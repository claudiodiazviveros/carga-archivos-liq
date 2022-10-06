using Dapper;
using Microsoft.Extensions.Configuration;
using Serilog;
using System;
using System.Data.SqlClient;
using System.Linq;
using System.Text;

namespace CargaArchivoLiquidadores
{
    public partial class Medicamento
    {
        public static Medicamento[] Import(string[] lines)
        {
            return (from line in

                        from inner in lines.Skip(1) select inner.Split('|')

                    select new Medicamento()
                    {
                        CodigoMedicamento = line[0],
                        DescripcionMedicamento = line[1],
                        CodigoClasificacionBiomedica = line[2],
                        FechaExtraccion = line[3],

                    }).ToArray();
        }

        public static string StatementSql(Medicamento[] medicamentos)
        {
            StringBuilder sb = new StringBuilder();

            int newRows = 0;

            foreach (var item in medicamentos)
            {
                try
                {
                    if (IsExist(item.CodigoMedicamento))
                    {
                        continue;
                    }

                    int CLBI_ID_CLASIFICACION_FK = Get_ID_CLASIFICACION(item.CodigoClasificacionBiomedica);

                    string sql = "INSERT INTO [dbo].[MEDICAMENTO]([CLBI_ID_CLASIFICACION],[MDTO_COD_MEDICAMENTO],[MDTO_DES_MEDICAMENTO],[MDTO_USU_ULT_ACT],[MDTO_USU_CREACION],[MDTO_FEC_ULT_ACT],[MDTO_FEC_CREACION] ,[MDTO_ES_VIGENTE],[MDTO_ORIGEN_DATO]) " +
                        $"VALUES ({CLBI_ID_CLASIFICACION_FK}, {item.CodigoMedicamento}, '{item.DescripcionMedicamento}', 'BATCH', 'BATCH', NULL, '{item.FechaExtraccion}', 1, 'P')";
                    sb.AppendLine(sql);

                    newRows++;
                }
                catch (Exception ex)
                {
                    Log.Error(ex.Message);
                }
            }

            Log.Information($"Registros nuevos: {newRows}");

            return sb.ToString();
        }

        private static bool IsExist(string codigoMedicamento)
        {
            using (var connection = new SqlConnection(Program.configuration.GetConnectionString("DataConnection")))
            {
                string sql = $"SELECT 1 FROM MEDICAMENTO WHERE MDTO_COD_MEDICAMENTO = '{codigoMedicamento}'";
                return connection.QuerySingleOrDefault<bool>(sql);
            }
        }

        private static int Get_ID_CLASIFICACION(string codigoClasificacionBiomedica)
        {
            int ret = 0;

            using (var connection = new SqlConnection(Program.configuration.GetConnectionString("DataConnection")))
            {
                string sql = $"SELECT TOP 1 CLBI_ID_CLASIFICACION FROM CLASIFICACION_BIOMEDICA WHERE CLBI_COD_CLASIFICACION = '{codigoClasificacionBiomedica}'";
                ret = connection.QueryFirstOrDefault<int>(sql);
            }

            if (ret == 0) throw new Exception($"CLASIFICACION_BIOMEDICA {codigoClasificacionBiomedica} no existente!");

            return ret;
        }
    }
}