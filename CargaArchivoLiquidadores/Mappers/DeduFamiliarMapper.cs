using System.Linq;
using System.Text;

namespace CargaArchivoLiquidadores
{
    public partial class DeduFamiliar
    {
        public static DeduFamiliar[] Import(string[] lines)
        {
            return (from line in

                        from inner in lines.Skip(1) select inner.Split('|')

                    select new DeduFamiliar()
                    {
                        Proveedor = line[0],
                        Poliza = line[1],
                        CodigoPlan = line[2],
                        TipoDeducible = line[3],
                        Plazo = line[4],
                        TitularSolo = line[5],
                        Titular1Carga = line[6],
                        Titular2Cargas = line[7],
                        Titular2Omas = line[8],
                        Titular3Cargas = line[9],
                        Titular3OmasCargas = line[10],
                        Titular4Cargas = line[11],
                        Titular5OMASCARGAS = line[12],
                        FechaExtraccion = line[13],
                        FechaInicioVigencia = line[14],
                        FechaTerminoVigencia = line[15],
                        PlanPersonal = line[16],
                        CodigoGrupo = line[17],
                    }).ToArray();
        }

        public static string StatementSql(DeduFamiliar[] deduFamiliares)
        {
            StringBuilder sb = new StringBuilder();

            foreach (var item in deduFamiliares)
            {
                string titularSolo = string.IsNullOrEmpty(item.TitularSolo) ? "0" : item.TitularSolo.Replace(",", ".");
                string titular1Carga = string.IsNullOrEmpty(item.Titular1Carga) ? "0" : item.Titular1Carga.Replace(",", ".");
                string titular2Cargas = string.IsNullOrEmpty(item.Titular2Cargas) ? "0" : item.Titular2Cargas.Replace(",", ".");
                string titular2Omas = string.IsNullOrEmpty(item.Titular2Omas) ? "0" : item.Titular2Omas.Replace(",", ".");
                string titular3Cargas = string.IsNullOrEmpty(item.Titular3Cargas) ? "0" : item.Titular3Cargas.Replace(",", ".");
                string titular3OmasCargas = string.IsNullOrEmpty(item.Titular3OmasCargas) ? "0" : item.Titular3OmasCargas.Replace(",", ".");
                string titular4Cargas = string.IsNullOrEmpty(item.Titular4Cargas) ? "0" : item.Titular4Cargas.Replace(",", ".");
                string titular5OmasCargas = string.IsNullOrEmpty(item.Titular5OMASCARGAS) ? "0" : item.Titular5OMASCARGAS.Replace(",", ".");

                string sql = "INSERT INTO [dbo].[DEDUCIBLE_FAMILIAR] ([DF_PROVEEDOR], [DF_POLIZA], [DF_CODIGO_PLAN], [DF_TIPO_DEDUCIBLE], [DF_PLAZO], [DF_TIT_SOLO], [DF_TIT_1_CARGA], [DF_TIT_2_CARGAS], [DF_TIT_2_O_MAS], [DF_TIT_3_CARGAS], [DF_TIT_3_O_MAS], [DF_TIT_4_CARGAS], [DF_TIT_5_O_MAS], [DF_FEC_MODIFICACION], [DF_FEC_CREACION], [DF_FEC_INICIO_VIG], [DF_FEC_TERMINO_VIG], [DF_PLAN_PERSONAL], [DF_CODIGO_GRUPO]) " +
                    $"VALUES ('{item.Proveedor}', {item.Poliza}, '{item.CodigoPlan}', '{item.TipoDeducible}', {item.Plazo}, {titularSolo}, {titular1Carga}, {titular2Cargas}, {titular2Omas}, {titular3Cargas}, {titular3OmasCargas}, {titular4Cargas}, {titular5OmasCargas}, NULL, GETDATE(), '{item.FechaInicioVigencia}', '{item.FechaTerminoVigencia}', '{item.PlanPersonal}', '{item.CodigoGrupo}')";

                sb.AppendLine(sql);
            }

            return sb.ToString();
        }
    }
}