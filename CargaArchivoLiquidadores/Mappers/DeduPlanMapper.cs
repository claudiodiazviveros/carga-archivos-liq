using Serilog;
using System;
using System.Linq;
using System.Text;

namespace CargaArchivoLiquidadores
{
    public partial class DeduPlan
    {
        public static DeduPlan[] Import(string[] lines)
        {
            return (from line in

                        from inner in lines.Skip(1) select inner.Split('|')

                    select new DeduPlan()
                    {
                        Proveedor = line[0],
                        Poliza = line[1],
                        Contratante = line[2],
                        CodigoPlan = line[3],
                        TipoDeducible = line[4],
                        MaximoDeducible = line[5],
                        Plazo = line[6],
                        MaximoPersona = line[7],
                        MaximoGrupo = line[8],
                        TopeGeneralPlan = line[9],
                        FechaExtraccion = line[10],
                        FechaInicioVigencia = line[11],
                        FechaTerminoVigencia = line[12],
                        PlanPersonal = line[13],
                        CodigoGrupo = line[14],

                    }).ToArray();
        }

        public static string StatementSql(DeduPlan[] deduPlans)
        {
            StringBuilder sb = new StringBuilder();

            int newRows = 0;

            foreach (var item in deduPlans)
            {
                try
                {
                    string maximoDeducible = string.IsNullOrEmpty(item.MaximoDeducible) ? "0" : item.MaximoDeducible.Replace(",", ".");
                    string maximoGrupo = string.IsNullOrEmpty(item.MaximoGrupo) ? "0" : item.MaximoGrupo.Replace(",", ".");
                    string topeGeneralPlan = string.IsNullOrEmpty(item.TopeGeneralPlan) ? "0" : item.TopeGeneralPlan.Replace(",", ".");

                    string sql = $"INSERT INTO [dbo].[DEDUCIBLE_PLAN] ([DP_PROVEEDOR], [DP_POLIZA], [DP_CONTRATANTE], [DP_CODIGO_PLAN], [DP_TIPO_DEDUCIBLE], [DP_MAX_DEDUCIBLE], [DP_PLAZO], [DP_MAX_PERSONA], [DP_MAX_GRUPO], [DP_TOPE_GENERAL_PLAN], [DP_FEC_MODIFICACION], [DP_FEC_CREACION], [DP_FEC_INICIO_VIG], [DP_FEC_TERMINO_VIG], [DP_PLAN_PERSONAL], [DP_CODIGO_GRUPO]) " +
                        $"VALUES ('{item.Proveedor}', {item.Poliza}, '{item.Contratante}', '{item.CodigoPlan}', '{item.TipoDeducible}', {maximoDeducible}, {item.Plazo}, {item.MaximoPersona}, {maximoGrupo}, {topeGeneralPlan}, NULL, '{item.FechaExtraccion}', '{item.FechaInicioVigencia}', '{item.FechaTerminoVigencia}', '{item.PlanPersonal}', '{item.CodigoGrupo}')";
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