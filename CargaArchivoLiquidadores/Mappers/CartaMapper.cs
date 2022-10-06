using Dapper;
using Microsoft.Extensions.Configuration;
using Serilog;
using System;
using System.Data.SqlClient;
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

            int newRows = 0;
            string sql;

            if (cartas.Count() > 0)
            {
                sql = $"DECLARE @ID_SOLICITUD INT";
                sb.AppendLine(sql);
            }

            foreach (var item in cartas)
            {
                try
                {
                    int PRVD_ID_PROVEEDOR_FK = Get_ID_PROVEEDOR(item.Proveedor);
                    int LIQU_ID_LIQUIDADOR_FK = Get_ID_LIQUIDADOR(item.Liquidador);

                    sql = $"SET @ID_SOLICITUD = (SELECT TOP 1 SLCD_ID_SOLICITUD FROM SOLICITUD WHERE SLCD_NUM_SOLICITUD = '{item.NumeroSolicitud}')";
                    sb.AppendLine(sql);

                    sql = "INSERT INTO [dbo].[CARTAS_RECHAZO] ([SLCD_ID_SOLICITUD], [CRTA_SECUENCIAL_CARTA], [PRVD_ID_PROVEEDOR], [LIQU_ID_LIQUIDADOR], [CRTA_EST_CARTA], [CRTA_COD_MOT_RECHAZO], [CRTA_DES_MOT_RECHAZO], [CRTA_TIP_ENVIO], [CRTA_NUM_CARTA], [CRTA_FEC_CARTA], [CRTA_GLOSA], [CRTA_DIR_PACIENTE], [CRTA_MTO_RECHAZO], [CRTA_DOC_DEVUELTOS], [CRTA_FEC_CREACION], [CRTA_FEC_ULT_ACT], [CRTA_USU_CREACION], [CRTA_USU_ULT_ACT], [CRTA_ES_VIGENTE], [CRTA_ORIGEN_DATO]) " + 
                        $"VALUES (@ID_SOLICITUD, 1, {PRVD_ID_PROVEEDOR_FK}, {LIQU_ID_LIQUIDADOR_FK}, '{item.EstadoCarta}', {item.CodigoMotivo}, '{item.DescripcionMotivo}', '{item.IndicadorEnvio}', {item.NumeroCarta}, '{item.FechaCarta}', '{item.Glosa}', '{item.DireccionPaciente}', {item.MontoTotal}, {item.CantidadDocumentosDevueltos}, '{item.FechaExtraccion}', NULL, 'BATCH', 'BATCH', 1, 'P')";
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

        private static int Get_ID_LIQUIDADOR(string liquidador)
        {
            int ret = 0;

            using (var connection = new SqlConnection(Program.configuration.GetConnectionString("DataConnection")))
            {
                string sql = $"SELECT TOP 1 LIQU_ID_LIQUIDADOR FROM LIQUIDADOR WHERE LIQU_COD_LIQUIDADOR = '{liquidador}'";
                ret = connection.QueryFirstOrDefault<int>(sql);
            }

            if (ret == 0) throw new Exception($"LIQUIDADOR {liquidador} no existente!");

            return ret;
        }

        private static int Get_ID_PROVEEDOR(string proveedor)
        {
            int ret = 0;

            using (var connection = new SqlConnection(Program.configuration.GetConnectionString("DataConnection")))
            {
                string sql = $"SELECT TOP 1 PRVD_ID_PROVEEDOR FROM PROVEEDOR WHERE PRVD_COD_PROVEEDOR = '{proveedor}'";
                ret = connection.QueryFirstOrDefault<int>(sql);
            }

            if (ret == 0) throw new Exception($"PROVEEDOR {proveedor} no existente!");

            return ret;
        }
    }
}