using System;
using System.Linq;
using System.Text;

namespace CargaArchivoLiquidadores
{
    public partial class ClasifBiomedica
    {
        public static ClasifBiomedica[] Import(string[] lines)
        {
            return (from line in

                        from inner in lines.Skip(1) select inner.Split('|')

                    select new ClasifBiomedica()
                    {
                        CodigoClasificacion = Int32.Parse(line[0]),
                        DescripcionClasificacion = line[1],
                        FechaExtraccion = line[2],
                    }).ToArray();
        }

        public static string StatementSql(ClasifBiomedica[] clasifBiomedicas)
        {
            StringBuilder sb = new StringBuilder();

            foreach (var item in clasifBiomedicas)
            {
                string sql = "INSERT INTO [dbo].[CLASIFICACION_BIOMEDICA] ([CLBI_COD_CLASIFICACION], [CLBI_DES_CLASIFICACION], [CLBI_USU_ULT_ACT], [CLBI_USU_CREACION], [CLBI_FEC_ULT_ACT], [CLBI_FEC_CREACION], [CLBI_ES_VIGENTE], [CLBI_FEC_EXTRACCION], [CLBI_ORIGEN_DATO]) " +
                    $"VALUES('{item.CodigoClasificacion}', '{item.DescripcionClasificacion}', 'BATCH', 'BATCH', GETDATE(), GETDATE(), 1, '{item.FechaExtraccion}', 'P')";

                sb.AppendLine(sql);
            }

            return sb.ToString();
        }
    }
}