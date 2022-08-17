using CargaArchivoLiquidadores.Interfaces;
using Dapper;
using Microsoft.Extensions.Configuration;
using Serilog;
using System;
using System.Data.SqlClient;
using System.IO;
using System.Linq;

namespace CargaArchivoLiquidadores.Activities
{
    public class LoadFileSolicitud : ILoadFileSolicitud
    {
        private readonly IConfiguration _configuration;
        private readonly ILoadMaestroSolicitud _loadMaestroSolicitud;

        public LoadFileSolicitud(IConfiguration configuration, ILoadMaestroSolicitud loadMaestroSolicitud)
        {
            _configuration = configuration;
            _loadMaestroSolicitud = loadMaestroSolicitud;
        }

        public bool LoadData()
        {
            int currentRow = 2;
            string folder = $@"{Environment.CurrentDirectory}\INBOX";

            DirectoryInfo di = new DirectoryInfo(folder);

            foreach (var fi in di.GetFiles("LIQUIDACIONES*.txt"))
            {
                using (var file = new StreamReader(fi.FullName))
                {
                    string row;

                    file.ReadLine();

                    using (var connection = new SqlConnection(_configuration.GetConnectionString("DataConnection")))
                    {
                        while ((row = file.ReadLine()) != null)
                        {
                            var campos = row.Split('|');

                            try
                            {
                                var selectQuery = $"SELECT * FROM SOLICITUD WHERE SLCD_CORRELATIVO_INTERNO = @numSolicitud AND PRVD_ID_PROVEEDOR = 2";
                                var medicamentoID = connection.Query(selectQuery, new
                                {
                                    numSolicitud = campos[58],
                                });

                                if (medicamentoID.AsList().Count == 0)
                                {
                                    int idPol = _loadMaestroSolicitud.GetIdPoliza(currentRow, campos);
                                    int idCobt = _loadMaestroSolicitud.GetIdCobertura(currentRow, campos);
                                    int idFormaPago = _loadMaestroSolicitud.GetFormaPago(currentRow, campos);
                                    int idTipoCarga = _loadMaestroSolicitud.GetTipoCarga(currentRow, campos);

                                    //if validar si el ID es 0, no haga el insert
                                    AddSolicitud(connection, campos, idPol, idCobt, idFormaPago, idTipoCarga);

                                    currentRow++;
                                }
                            }
                            catch (Exception ex)
                            {
                                throw;
                            }
                        }
                    }
                }
            }

            return true;
        }

        private bool AddSolicitud(SqlConnection connection, string[] campos, int idPol, int idCobt, int idFormaPago, int idTipoCarga)
        {
            string insertQuery = @"INSERT INTO [dbo].[SOLICITUD]([PLZA_ID_POLIZA], " +
                "[SLCD_COD_POLIZA], [RMSA_ID_REMESA], [ISAP_ID_ISAPRE], [PERS_RUT_CONTRATANTE], [PERS_RUT_TITULAR], [PERS_RUT_PACIENTE], [TPCG_ID_TIP_CARGA], " +
                " [LIQU_ID_LIQUIDADOR], [PRVD_ID_PROVEEDOR], [SCSL_ID_SUCURSAL], [SLCD_NUM_SOLICITUD], [SLCD_CORRELATIVO_INTERNO], [SLCD_FEC_PRESENTACION], " +
                "[SLCD_FEC_PAGO], [FMPR_ID_FORMA_PAGO], [BNCO_ID_BANCO], [TPAD_ID_TIP_ADMINISTRACION], [TPCB_ID_COBERTURA], [PERS_RUT_CORREDOR], [ESDS_ID_ESTADO], " +
                "[GRIN_ID_GRUPO_ING_INFORMADO], [POIN_ID_POOL_INFORMADO], [SLCD_FECHA_ESTADO], [SLCD_CTA_CORRIENTE], [SLCD_NUM_EGRESO], [SLCD_RECEP_CHEQUE], [SLCD_SDO_DISPONIBLE_UF], " +
                "[SLCD_FEC_OCURRENCIA], [SLCD_FEC_CREACION], [SLCD_FEC_ULT_ACT], [SLCD_USU_CREACION], [SLCD_USU_ULT_ACT], [SLCD_ES_VIGENTE], [NPLI_ID_PLAN_INFORMADO], " +
                "[SLCD_OBS_SOLICITUD], [SLCD_PAG_CONTRATANTE], [SLCD_FEC_DIGITACION], [SLCD_FEC_PAGO_PROYECTADA], [SLCD_DES_PAGO], [SLCD_CANT_DOCUMENTOS], [SLCD_MTO_PRESTACION], [SLCD_MTO_DEDUCIBLE], " +
                "[SLCD_MTO_PAGO], [SLCD_FEC_CONTABLE], [SLCD_NUN_DENUNCIA], [SLCD_GLOSA_MEDICAMENTO], [SLCD_OBSERVACION_2])";

            string defaultValues = @"VALUES("+idPol+", " +
                ""+campos[0]+", @RMSA_ID_REMESA, "+ campos[73] + ", "+ campos[3] + ", "+ campos[14] + ", "+ campos[19] + ", "+idTipoCarga+", " +
                "@LIQU_ID_LIQUIDADOR, @PRVD_ID_PROVEEDOR, @SCSL_ID_SUCURSAL, @SLCD_NUM_SOLICITUD, @SLCD_CORRELATIVO_INTERNO, @SLCD_FEC_PRESENTACION, " +
                "@SLCD_FEC_PAGO, "+idFormaPago+", @BNCO_ID_BANCO, @TPAD_ID_TIP_ADMINISTRACION, "+idCobt+", @PERS_RUT_CORREDOR, @ESDS_ID_ESTADO, " +
                "@GRIN_ID_GRUPO_ING_INFORMADO, @POIN_ID_POOL_INFORMADO, @SLCD_FECHA_ESTADO, @SLCD_CTA_CORRIENTE, @SLCD_NUM_EGRESO, @SLCD_RECEP_CHEQUE, @SLCD_SDO_DISPONIBLE_UF, " +
                "@SLCD_FEC_OCURRENCIA, @SLCD_FEC_CREACION, @SLCD_FEC_ULT_ACT, @SLCD_USU_CREACION, @SLCD_USU_ULT_ACT, @SLCD_ES_VIGENTE, @NPLI_ID_PLAN_INFORMADO, " +
                "@SLCD_OBS_SOLICITUD, @SLCD_PAG_CONTRATANTE, @SLCD_FEC_DIGITACION, @SLCD_FEC_PAGO_PROYECTADA, @SLCD_DES_PAGO, @SLCD_CANT_DOCUMENTOS, @SLCD_MTO_PRESTACION, @SLCD_MTO_DEDUCIBLE, " +
                "@SLCD_MTO_PAGO, @SLCD_FEC_CONTABLE, @SLCD_NUN_DENUNCIA, @SLCD_GLOSA_MEDICAMENTO, @SLCD_OBSERVACION_2)";

            try
            {
                //connection.Execute(insertQuery, new
                //{
                //    PLZA_ID_POLIZA = idPol,
                //    PLZA_COD_POLIZA = campos[0],
                //    ISAP_ID_ISAPRE = campos[73],
                //    TPCB_ID_COBERTURA = idCobt,
                //    FMPR_ID_FORMA_PAGO = idFormaPago,
                //    PERS_RUT_CONTRATANTE = campos[3],
                //    PERS_RUT_TITULAR = campos[14],
                //    PERS_RUT_PACIENTE = campos[19],
                //    PERS_RUT_CORREDOR = campos[6],
                //    SLCD_OBSERVACION_2 = campos[86],

                //});
                Console.WriteLine(insertQuery);
                Console.WriteLine(defaultValues);
                return true;
            }
            catch (Exception ex)
            {
                Log.Error($"Fila [{campos}] {ex.Message}");
            }
            return false;
        }
    }
}
