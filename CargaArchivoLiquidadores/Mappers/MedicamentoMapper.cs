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

            foreach (var item in medicamentos)
            {
                int ID_CLASIFICACION_FK = 0;

                string sql = "INSERT INTO [dbo].[MEDICAMENTO]([CLBI_ID_CLASIFICACION],[MDTO_COD_MEDICAMENTO],[MDTO_DES_MEDICAMENTO],[MDTO_USU_ULT_ACT],[MDTO_USU_CREACION],[MDTO_FEC_ULT_ACT],[MDTO_FEC_CREACION] ,[MDTO_ES_VIGENTE],[MDTO_ORIGEN_DATO]) " +
                    $"VALUES ({ID_CLASIFICACION_FK}, {item.CodigoMedicamento}, '{item.DescripcionMedicamento}', 'BATCH', 'BATCH', NULL, '{item.FechaExtraccion}', 1, 'P')";

                sb.AppendLine(sql);
            }

            return sb.ToString();
        }
    }
}