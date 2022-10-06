using Serilog;
using System;
using System.Linq;
using System.Text;

namespace CargaArchivoLiquidadores
{
    public partial class DeduCobertura
    {
        public static DeduCobertura[] Import(string[] lines)
        {
            return (from line in

                        from inner in lines.Skip(1) select inner.Split('|')

                    select new DeduCobertura()
                    {
                        Proveedor = line[0],
                        Poliza = line[1],
                        CodigoPlan = line[2],
                        TipoDeducible = line[3],
                        Plazo = line[4],
                        CodigoCobertura = line[5],
                        MaximoDeducible = line[6],
                        MaximoPersona = line[7],
                        FechaExtraccion = line[8],

                    }).ToArray();
        }

        public static string StatementSql(DeduCobertura[] deduCoberturas)
        {
            StringBuilder sb = new StringBuilder();

            int newRows = 0;

            foreach (var item in deduCoberturas)
            {
                try
                {
                    string maximoDeducible = string.IsNullOrEmpty(item.MaximoDeducible) ? "0" : item.MaximoDeducible.Replace(",", ".");
                    string maximoPersona = string.IsNullOrEmpty(item.MaximoPersona) ? "0" : item.MaximoPersona.Replace(",", ".");

                    string sql = "INSERT INTO [dbo].[DEDUCIBLE_COBERTURA] ([DC_PROVEEDOR], [DC_POLIZA], [DC_CODIGO_PLAN], [DC_TIPO_DEDUCIBLE], [DC_PLAZO], [DC_COD_COBERTURA], [DC_MAX_DEDUCIBLE], [DC_MAX_PERSONA], [DC_FEC_MODIFICACION], [DC_FEC_CREACION]) " +
                        $"VALUES ('{item.Proveedor}', {item.Poliza}, '{item.CodigoPlan}', '{item.TipoDeducible}', {item.Plazo}, {item.CodigoCobertura}, {maximoDeducible}, {maximoPersona}, NULL, {item.FechaExtraccion})";
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