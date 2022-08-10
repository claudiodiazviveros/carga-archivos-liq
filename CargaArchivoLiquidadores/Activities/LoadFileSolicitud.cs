using CargaArchivoLiquidadores.Interfaces;
using Dapper;
using Microsoft.Extensions.Configuration;
using System;
using System.Data.SqlClient;
using System.IO;
using System.Linq;

namespace CargaArchivoLiquidadores.Activities
{
    public class LoadFileSolicitud : ILoadFileSolicitud
    {
        private readonly IConfiguration _configuration;

        public LoadFileSolicitud(IConfiguration configuration)
        {
            _configuration = configuration;
        }

        public bool LoadData()
        {
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
                                string insertQuery = @"INSERT INTO [dbo].[SOLICITUD]([SLCD_ID_SOLICITUD], [PLZA_ID_POLIZA], " +
                                       "[SLCD_COD_POLIZA], [RMSA_ID_REMESA], [ISAP_ID_ISAPRE], [PERS_RUT_CONTRATANTE], [PERS_RUT_TITULAR], [PERS_RUT_PACIENTE], [TPCG_ID_TIP_CARGA], " +
                                       " [LIQU_ID_LIQUIDADOR], [PRVD_ID_PROVEEDOR], [SCSL_ID_SUCURSAL], [SLCD_NUM_SOLICITUD], [SLCD_CORRELATIVO_INTERNO], [SLCD_FEC_PRESENTACION], " +
                                       "[SLCD_FEC_PAGO], [FMPR_ID_FORMA_PAGO], [BNCO_ID_BANCO], [TPAD_ID_TIP_ADMINISTRACION], [TPCB_ID_COBERTURA], [PERS_RUT_CORREDOR], [ESDS_ID_ESTADO], " +
                                       "[GRIN_ID_GRUPO_ING_INFORMADO], [POIN_ID_POOL_INFORMADO], [SLCD_FECHA_ESTADO], [SLCD_CTA_CORRIENTE], [SLCD_NUM_EGRESO], [SLCD_RECEP_CHEQUE], [SLCD_SDO_DISPONIBLE_UF], " +
                                       "[SLCD_FEC_OCURRENCIA], [SLCD_FEC_CREACION], [SLCD_FEC_ULT_ACT], [SLCD_USU_CREACION], [SLCD_USU_ULT_ACT], [SLCD_ES_VIGENTE], [NPLI_ID_PLAN_INFORMADO], " +
                                       "[SLCD_OBS_SOLICITUD], [SLCD_PAG_CONTRATANTE], [SLCD_FEC_DIGITACION], [SLCD_FEC_PAGO_PROYECTADA], [SLCD_DES_PAGO], [SLCD_CANT_DOCUMENTOS], [SLCD_MTO_PRESTACION], [SLCD_MTO_DEDUCIBLE], " +
                                       "[SLCD_MTO_PAGO], [SLCD_FEC_CONTABLE], [SLCD_NUN_DENUNCIA], [SLCD_GLOSA_MEDICAMENTO], [SLCD_OBSERVACION_2])" +

                                       "VALUES(@SLCD_ID_SOLICITUD, @PLZA_ID_POLIZA, " +
                                       "@SLCD_COD_POLIZA, @RMSA_ID_REMESA, @ISAP_ID_ISAPRE, @PERS_RUT_CONTRATANTE, @PERS_RUT_TITULAR, @PERS_RUT_PACIENTE, @TPCG_ID_TIP_CARGA, " +
                                       " @LIQU_ID_LIQUIDADOR, @PRVD_ID_PROVEEDOR, @SCSL_ID_SUCURSAL, @SLCD_NUM_SOLICITUD, @SLCD_CORRELATIVO_INTERNO, @SLCD_FEC_PRESENTACION, " +
                                       "@SLCD_FEC_PAGO, @FMPR_ID_FORMA_PAGO, @BNCO_ID_BANCO, @TPAD_ID_TIP_ADMINISTRACION, @TPCB_ID_COBERTURA, @PERS_RUT_CORREDOR, @ESDS_ID_ESTADO, " +
                                       "@GRIN_ID_GRUPO_ING_INFORMADO, @POIN_ID_POOL_INFORMADO, @SLCD_FECHA_ESTADO, @SLCD_CTA_CORRIENTE, @SLCD_NUM_EGRESO, @SLCD_RECEP_CHEQUE, @SLCD_SDO_DISPONIBLE_UF, " +
                                       "@SLCD_FEC_OCURRENCIA, @SLCD_FEC_CREACION, @SLCD_FEC_ULT_ACT, @SLCD_USU_CREACION, @SLCD_USU_ULT_ACT, @SLCD_ES_VIGENTE, @NPLI_ID_PLAN_INFORMADO, " +
                                       "@SLCD_OBS_SOLICITUD, @SLCD_PAG_CONTRATANTE, @SLCD_FEC_DIGITACION, @SLCD_FEC_PAGO_PROYECTADA, @SLCD_DES_PAGO, @SLCD_CANT_DOCUMENTOS, @SLCD_MTO_PRESTACION, @SLCD_MTO_DEDUCIBLE, " +
                                       "@SLCD_MTO_PAGO, @SLCD_FEC_CONTABLE, @SLCD_NUN_DENUNCIA, @SLCD_GLOSA_MEDICAMENTO, @SLCD_OBSERVACION_2)";


                                var selectQuery = $"SELECT * FROM SOLICITUD WHERE SLCD_CORRELATIVO_INTERNO = @numSolicitud AND PRVD_ID_PROVEEDOR = 2";
                                var medicamentoID = connection.Query(selectQuery, new
                                {
                                    numSolicitud = campos[58],
                                });


                                if (medicamentoID.AsList().Count == 0)
                                {
                                    //Poliza
                                    var selectQueryPoliza = $"SELECT TOP(1) ISNULL([PLZA_ID_POLIZA], 0) FROM [dbo].[POLIZA] WHERE [PLZA_COD_POLIZA]= @poliza ORDER BY PLZA_ID_POLIZA DESC";
                                    var polizaID = connection.Query<Int32>(selectQueryPoliza, new
                                    {
                                        poliza = campos[0]
                                    });

                                    //Cobertura

                                    var selectTipoCobertura = @"SELECT TOP(1) ISNULL([TPCB_ID_COBERTURA], 0) FROM [dbo].[TIPO_COBERTURA] WHERE [TPCB_DES_COBERTURA]= @tipoCobertura ";
                                    var coberturaID = connection.Query<Int32>(selectTipoCobertura, new
                                    {
                                        poliza = campos[1]
                                    });

                                    //FormaPagoReembolso
                                    var selectFormPago = @"SELECT TOP(1) FMPR_COD_FORMA_PAGO FROM dbo.FORMA_PAGO_REEMBOLSO WHERE FMPR_COD_FORMA_PAGO = @formaPagoRembolso";
                                    var formPagoID = connection.Query<Int32>(selectFormPago, new
                                    {
                                        formaPagoRembolso = campos[2]
                                    });

                                    if (formPagoID.AsList().Count == 0)
                                    {
                                        var insertFormaPago = @"INSERT INTO FORMA_PAGO_REEMBOLSO " +
                                            " ([FMPR_COD_FORMA_PAGO], [FMPR_DES_FORMA_PAGO], [FMPR_FEC_CREACION], [FMPR_FEC_ULT_ACT], [FMPR_USU_CREACION], [FMPR_USU_ULT_ACT], [FMPR_ES_VIGENTE], [FMPR_ORIGEN_DATO])" +
                                            " VALUES (@FMPR_COD_FORMA_PAGO, @FMPR_DES_FORMA_PAGO, @FMPR_FEC_CREACION, @FMPR_FEC_ULT_ACT, @FMPR_USU_CREACION, @FMPR_USU_ULT_ACT, @FMPR_ES_VIGENTE, @FMPR_ORIGEN_DATO)";


                                        var resultForm = connection.Execute(insertFormaPago, new
                                        {
                                            FMPR_COD_FORMA_PAGO = campos[2],
                                            FMPR_DES_FORMA_PAGO = campos[2],
                                            FMPR_FEC_CREACION = DateTime.Now,
                                            FMPR_FEC_ULT_ACT = DateTime.Now,
                                            FMPR_USU_CREACION = "BATCH",
                                            FMPR_USU_ULT_ACT = "BATCH",
                                            FMPR_ES_VIGENTE = true,
                                            FMPR_ORIGEN_DATO = "P"
                                        });
                                    }


                                    int idPol = Convert.ToInt32(polizaID.FirstOrDefault());
                                    int idCobt = Convert.ToInt32(coberturaID.FirstOrDefault());

                                    var result = connection.Execute(insertQuery, new
                                    {
                                        PLZA_ID_POLIZA = idPol,
                                        PLZA_COD_POLIZA = campos[0],
                                        TPCB_ID_COBERTURA = idCobt
                                    });
                                }
                            }
                            catch (Exception)
                            {
                                throw;
                            }

                        }
                    }

                }
            }

            return true;
        }
    }
}
