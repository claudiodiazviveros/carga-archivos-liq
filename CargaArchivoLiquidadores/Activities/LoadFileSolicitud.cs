using CargaArchivoLiquidadores.Interfaces;
using Dapper;
using Microsoft.Extensions.Configuration;
using Serilog;
using System;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;

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

        public void SaveScript()
        {
            int currentRow = 2;
            string folderIn = $@"{Environment.CurrentDirectory}\INBOX";
            string folderOut = $@"{Environment.CurrentDirectory}\OUTPUT";

            DirectoryInfo di = new DirectoryInfo(folderIn);
            foreach (var fi in di.GetFiles("LIQUIDACIONES*.txt"))
            {
                Log.Information("Inicio Script: LIQUIDACIONES");

                using (var file = new StreamReader(fi.FullName))
                using (StreamWriter sw = new StreamWriter($@"{folderOut}\SOLICITUD.sql"))
                {
                    string row = file.ReadLine();   // Excluye primera Linea Resumen

                    while ((row = file.ReadLine()) != null)
                    {
                        string[] arrayFields = row.Split('|');
                        string line = QueryRow(currentRow, arrayFields);
                        var result = line != "" ? true : false;

                        if (result)
                            sw.WriteLine(line);

                        currentRow++;
                    }
                }

                Log.Information("Termino Script: LIQUIDACIONES");
            }
        }

        private string QueryRow(int currentRow, string[] campos)
        {
            float montoPrestacionUf = 0;


            using (var connection = new SqlConnection(_configuration.GetConnectionString("DataConnection")))
            {
                var selectQuery = $"SELECT * FROM SOLICITUD WHERE SLCD_CORRELATIVO_INTERNO = @correlativo AND PRVD_ID_PROVEEDOR = 2";
                var solicitudID = connection.Query(selectQuery, new
                {
                    correlativo = campos[58],
                });

                if (solicitudID.AsList().Count == 0)
                {
                    int idPoliza = _loadMaestroSolicitud.GetIdPoliza(currentRow, campos);
                    int idCobt = _loadMaestroSolicitud.GetIdCobertura(currentRow, campos);
                    int idFormaPago = _loadMaestroSolicitud.GetFormaPago(currentRow, campos);
                    int idTipoCarga = _loadMaestroSolicitud.GetTipoCarga(currentRow, campos);
                    int idLiquidador = _loadMaestroSolicitud.GetLiquidador(currentRow, campos);
                    int idProveedor = _loadMaestroSolicitud.GetProveedor(currentRow, campos);
                    int idSucursal = _loadMaestroSolicitud.GetSucursal(currentRow, campos);
                    int idBanco = _loadMaestroSolicitud.GetBanco(currentRow, campos);
                    int idTipoAdmin = _loadMaestroSolicitud.GetTipoAdministracion(currentRow, campos);
                    int idEstadoSolicitudReembolso = _loadMaestroSolicitud.GetEstadoSolicitudReembolso(currentRow, campos);
                    int idGrupoIngInformado = _loadMaestroSolicitud.GetGrupoIngInformado(currentRow, campos);
                    int idPool = _loadMaestroSolicitud.GetPoolInformado(currentRow, campos);
                    int idPlanInformado = _loadMaestroSolicitud.GetNombrePlanInformado(currentRow, campos);
                    int idRemesa = _loadMaestroSolicitud.GetRemesa(currentRow, campos);
                    int isapre = _loadMaestroSolicitud.GetIsapre(currentRow, campos);
                    int vigencia = 1;

                    //se debe calcular la suma de los montos para insertar el resultado en el caso que hayan solicitudes repetidas en el archivo
                    if (Convert.ToInt32(campos[40]) > 1)
                    {
                        SumAmounts(campos);
                    }

                    //if validar si el ID es 0, no haga el insert

                    string insertQuery = @"INSERT INTO [dbo].[SOLICITUD]([PLZA_ID_POLIZA], " +
                        "[SLCD_COD_POLIZA], [RMSA_ID_REMESA], [ISAP_ID_ISAPRE], [PERS_RUT_CONTRATANTE], [PERS_RUT_TITULAR], [PERS_RUT_PACIENTE], [TPCG_ID_TIP_CARGA], " +
                        " [LIQU_ID_LIQUIDADOR], [PRVD_ID_PROVEEDOR], [SCSL_ID_SUCURSAL], [SLCD_NUM_SOLICITUD], [SLCD_CORRELATIVO_INTERNO], [SLCD_FEC_PRESENTACION], " +
                        "[SLCD_FEC_PAGO], [FMPR_ID_FORMA_PAGO], [BNCO_ID_BANCO], [TPAD_ID_TIP_ADMINISTRACION], [TPCB_ID_COBERTURA], [PERS_RUT_CORREDOR], [ESDS_ID_ESTADO], " +
                        "[GRIN_ID_GRUPO_ING_INFORMADO], [POIN_ID_POOL_INFORMADO], [SLCD_FECHA_ESTADO], [SLCD_CTA_CORRIENTE], [SLCD_NUM_EGRESO], [SLCD_RECEP_CHEQUE], [SLCD_SDO_DISPONIBLE_UF], " +
                        "[SLCD_FEC_OCURRENCIA], [SLCD_FEC_CREACION], [SLCD_FEC_ULT_ACT], [SLCD_USU_CREACION], [SLCD_USU_ULT_ACT], [SLCD_ES_VIGENTE], [NPLI_ID_PLAN_INFORMADO], " +
                        "[SLCD_OBS_SOLICITUD], [SLCD_PAG_CONTRATANTE], [SLCD_FEC_DIGITACION], [SLCD_FEC_PAGO_PROYECTADA], [SLCD_DES_PAGO], [SLCD_CANT_DOCUMENTOS], [SLCD_MTO_PRESTACION], [SLCD_MTO_DEDUCIBLE], " +
                        "[SLCD_MTO_PAGO], [SLCD_FEC_CONTABLE], [SLCD_NUN_DENUNCIA], [SLCD_GLOSA_MEDICAMENTO], [SLCD_OBSERVACION_2])";

                    string defaultValues = $"VALUES('{idPoliza}', " +
                        $"'{campos[0]}', '{idRemesa}', '{isapre}', '{campos[3]}', '{campos[14]}', '{campos[19]}', '{idTipoCarga}', " +
                        $" '{idLiquidador}', '{idProveedor}', '{idSucursal}', '{campos[28]}', '{campos[58]}', '{campos[38]}', " +
                        $" '{campos[70]}', '{idFormaPago}, '{idBanco}', '{idTipoAdmin}', '{idCobt}', '{campos[6]}', '{idEstadoSolicitudReembolso}', " +
                        $" '{idGrupoIngInformado}', '{idPool}', '{campos[57]}', '{campos[68]}', '{campos[71]}', '{campos[69]}', '{campos[91]}', " +
                        $" '{campos[67]}', '{DateTime.Now.ToString("yyyyMMdd")}', '{DateTime.Now.ToString("yyyyMMdd")}', 'BATCH', 'BATCH', '{vigencia}', '{idPlanInformado}', " +
                        //$" '{campos[97]}', '{campos[98]}', '{campos[99]}', '{campos[100]}', '{campos[101]}', '{campos[108]}', '{campos[45]}', '{campos[51]}', " +
                        $" '{campos[103]}', '{campos[104]}', '{campos[99]}', '{campos[100]}', '{campos[107]}', '{campos[108]}', '{campos[45]}', '{campos[51]}', " +
                        //$" '{campos[53]}', '{campos[75]}', '{campos[111]}', '{campos[109]}', '{campos[86]}')";
                        $" '{campos[53]}', '{campos[75]}', '{campos[110]}', '{campos[109]}', '{campos[86]}')";

                    return insertQuery + defaultValues;
                }
                else
                {

                }

            }

            return "";
        }

        public void SumAmounts(string[] campos)
        { 

        }
    }
}
