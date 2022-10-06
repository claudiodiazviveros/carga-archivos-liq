using Serilog;
using System;
using System.Linq;
using System.Text;

namespace CargaArchivoLiquidadores
{
    public partial class DeduCobDet
    {
        public static DeduCobDet[] Import(string[] lines)
        {
            return (from line in

                        from inner in lines.Skip(1) select inner.Split('|')

                    select new DeduCobDet()
                    {
                        Proveedor = line[0],
                        Poliza = line[1],
                        CodigoPlan = line[2],
                        CodigoCobertura = line[3],
                        CodigoGrupo = line[4],
                        NombreGrupo = line[5],
                        FechaExtraccion = line[6],

                    }).ToArray();
        }

        public static string StatementSql(DeduCobDet[] deduCobDets)
        {
            StringBuilder sb = new StringBuilder();
            
            int newRows = 0;

            foreach (var item in deduCobDets)
            {
                try
                {
                    string sql = "INSERT INTO [dbo].[DEDUCIBLE_COBERTURA_DET] ([DCD_PROVEEDOR], [DCD_POLIZA], [DCD_CODIGO_PLAN], [DCD_COD_COBERTURA], [DCD_COD_GRUPO], [DCD_NOMBRE_GRUPO], [DCD_TIPO_COB], [DCD_FEC_MODIFICACION], [DCD_FEC_CREACION]) " +
                        $"VALUES ('{item.Proveedor}', {item.Poliza}, '{item.CodigoPlan}', {item.CodigoCobertura}, {item.CodigoGrupo}, '{item.NombreGrupo}', 'NO_FARMACIA', NULL, '{item.FechaExtraccion}')";
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
    }
}