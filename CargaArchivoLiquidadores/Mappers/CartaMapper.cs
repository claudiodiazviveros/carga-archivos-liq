using System.Linq;
using System.Text;

namespace CargaArchivoLiquidadores
{
    public partial class Carta
    {
        public static Carta[] Import(string[] lines)
        {
            return (from line in

                        from inner in lines.Skip(1) select inner.Split('|')

                    select new Carta()
                    {
                        Proveedor = line[0],
                        NumeroSolicitud = line[1],
                        NumeroCarta = line[2],
                        FechaCarta = line[3],
                        CodigoPrestacion = line[4],
                        CantidadDocumentosDevueltos = line[5],
                        CodigoMotivo = line[6],
                        DescripcionMotivo = line[7],
                        Glosa = line[8],
                        EstadoCarta = line[9],
                        IndicadorEnvio = line[10],
                        Liquidador = line[11],
                        DireccionPaciente = line[12],
                        MontoTotal = line[13],
                        FechaExtraccion = line[14],

                    }).ToArray();
        }

        public static string StatementSql(Carta[] cartas)
        {
            StringBuilder sb = new StringBuilder();

            foreach (var item in cartas)
            {
                int SLCD_ID_SOLICITUD_FK = 0;
                int PRVD_ID_PROVEEDOR_FK = 0;
                int LIQU_ID_LIQUIDADOR_FK = 0;

                string sql = "INSERT INTO [dbo].[CARTAS_RECHAZO] ([SLCD_ID_SOLICITUD], [CRTA_SECUENCIAL_CARTA], [PRVD_ID_PROVEEDOR], [LIQU_ID_LIQUIDADOR], [CRTA_EST_CARTA], [CRTA_COD_MOT_RECHAZO], [CRTA_DES_MOT_RECHAZO], [CRTA_TIP_ENVIO], [CRTA_NUM_CARTA], [CRTA_FEC_CARTA], [CRTA_GLOSA], [CRTA_DIR_PACIENTE], [CRTA_MTO_RECHAZO], [CRTA_DOC_DEVUELTOS], [CRTA_FEC_CREACION], [CRTA_FEC_ULT_ACT], [CRTA_USU_CREACION], [CRTA_USU_ULT_ACT], [CRTA_ES_VIGENTE], [CRTA_ORIGEN_DATO]) " +
                    $"VALUES ({SLCD_ID_SOLICITUD_FK}, [CRTA_SECUENCIAL_CARTA], {PRVD_ID_PROVEEDOR_FK}, {LIQU_ID_LIQUIDADOR_FK}, '{item.EstadoCarta}', {item.CodigoMotivo}, '{item.DescripcionMotivo}', '{item.IndicadorEnvio}', {item.NumeroCarta}, '{item.FechaCarta}', '{item.Glosa}', '{item.DireccionPaciente}', {item.MontoTotal}, {item.CantidadDocumentosDevueltos}, '{item.FechaExtraccion}', NULL, 'BATCH', 'BATCH', 1, 'P')";

                sb.AppendLine(sql);
            }

            return sb.ToString();
        }
    }
}