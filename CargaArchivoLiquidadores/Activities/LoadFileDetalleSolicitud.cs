using CargaArchivoLiquidadores.Interfaces;
using Dapper;
using Microsoft.Extensions.Configuration;
using Serilog;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using System.Text;

namespace CargaArchivoLiquidadores.Activities
{
    public class LoadFileDetalleSolicitud : ILoadFileDetalleSolicitud
    {
        private readonly IConfiguration _configuration;
        private readonly ILoadMaestroSolicitud _loadMaestroSolicitud;
        private readonly ILoadMaestroDetalleSolicitud _loadMaestroDetalleSolicitud;

        public LoadFileDetalleSolicitud(IConfiguration configuration, ILoadMaestroSolicitud loadMaestroSolicitud, ILoadMaestroDetalleSolicitud loadMaestroDetalleSolicitud)
        {
            _configuration = configuration;
            _loadMaestroSolicitud = loadMaestroSolicitud;
            _loadMaestroDetalleSolicitud = loadMaestroDetalleSolicitud;
        }

        public void SaveScript()
        {
            int currentRow = 2;
            string folderIn = $@"{Environment.CurrentDirectory}\INBOX";
            string folderOut = $@"{Environment.CurrentDirectory}\OUTPUT";

            DirectoryInfo di = new DirectoryInfo(folderIn);
            foreach (var fi in di.GetFiles("LIQUIDACIONES*.txt"))
            {
                Log.Information("Inicio Script: LIQUIDACIONES DETALLE");

                using (var file = new StreamReader(fi.FullName))
                using (StreamWriter sw = new StreamWriter($@"{folderOut}\DETALLE_SOLICITUD.sql"))
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

                Log.Information("Termino Script: LIQUIDACIONES DETALLE");
            }
        }

        private string QueryRow(int currentRow, string[] campos)
        {
            using (var connection = new SqlConnection(_configuration.GetConnectionString("DataConnection")))
            {
                var selectQuery = $"SELECT TOP(1) * FROM DETALLE_SOLICITUD WHERE DTSL_CORRELATIVO_INTERNO = @correlativo  AND DTSL_SEC_LIQUIDACION = @secuencia";
                var detalleID = connection.Query(selectQuery, new
                {
                    correlativo = campos[58],
                    secuencia = campos[40]
                });

                if (detalleID.AsList().Count == 0)
                {
                    int idPoliza = _loadMaestroSolicitud.GetIdPoliza(currentRow, campos);
                    int prestacion = _loadMaestroDetalleSolicitud.GetPrestacion(currentRow, campos);
                    int estadoReembolso = _loadMaestroSolicitud.GetEstadoSolicitudReembolso(currentRow, campos);
                    int tipoReembolso = _loadMaestroDetalleSolicitud.GetTipoReembolso(currentRow, campos);
                    int grupoPrestacion = _loadMaestroDetalleSolicitud.GetGrupoPrestacion(currentRow, campos);
                    int medicamento = _loadMaestroDetalleSolicitud.GetMedicamento(currentRow, campos);
                    int diagnostico = _loadMaestroDetalleSolicitud.GetDiagnostico(currentRow, campos);
                    int isapre = _loadMaestroSolicitud.GetIsapre(currentRow, campos);
                    int tipoDocumento = _loadMaestroDetalleSolicitud.GetTipoDocumento(currentRow, campos);
                    int tipoAtencion = _loadMaestroDetalleSolicitud.GetTipoAtencion(currentRow, campos);
                    int plan = _loadMaestroDetalleSolicitud.GetPlanes(currentRow, campos);
                    int biomedico = _loadMaestroDetalleSolicitud.GetClasificacionBiomedica(currentRow, campos);
                    int tipoAdminstracion = _loadMaestroSolicitud.GetTipoAdministracion(currentRow, campos);
                    int tipoDeducible = _loadMaestroDetalleSolicitud.GetTipoDeducible(currentRow, campos);
                    string folioDocumento = campos[34];

                    int vigencia = 1;


                    //if validar si el ID es 0, no haga el insert

                    string insertQuery = $"INSERT INTO [dbo].[DETALLE_SOLICITUD] ([SLCD_ID_SOLICITUD],[ETDO_ID_ESTADO],[TPRB_ID_TIP_REEMBOLSO],[PRTC_ID_PRESTACION]" +
                        $",[GRPR_ID_GRUPO],[MDTO_ID_MEDICAMENTO],[DIAG_ID_DIAGNOSTICO],[PRDR_RUT_PRESTADOR],[TPDC_ID_TIP_DOCUMENTO],[DTSL_FOLIO_DOCUMENTO],[TPAT_ID_TIP_ATENCION],[PLAN_ID]" +
                        $",[ISAP_ID_ISAPRE],[CLBI_ID_CLASIFICACION],[DTSL_FEC_PRESTACION],[DTSL_SEC_LIQUIDACION] ,[DTSL_CANT_PRESTACIONES] ,[DTSL_MTO_PRESTACION_CL] ,[DTSL_MTO_PRESTACION_UF]" +
                        $",[DTSL_MTO_ISAPRE_CL],[DTSL_MTO_ISAPRE_UF],[DTSL_MTO_SOLICITADO_CL] ,[DTSL_MTO_SOLICITADO_UF] ,[DTSL_MTO_DEDUCIBLE_CL],[DTSL_MTO_DEDUCIBLE_UF],[DTSL_MTO_PAGO_CL]" +
                        $",[DTSL_MTO_PAGO_UF],[DTSL_MTO_RECHAZO_CL] ,[DTSL_MTO_RECHAZO_UF],[DTSL_FEC_ESTADO] ,[DTSL_PORCENTAJE_REEMBOLSO] ,[DTSL_OBSERVACIONES] ,[DTSL_FEC_CONTABLE]" +
                        $",[DTSL_CORR_BENEFI],[DTSL_SECUENCIAL],[DTSL_TOPE_ARANCEL],[DTSL_FEC_CREACION],[DTSL_FEC_ULT_ACT] ,[DTSL_USU_CREACION],[DTSL_USU_ULT_ACT] ,[DTSL_ES_VIGENTE] " +
                        $",[DTSL_CORRELATIVO_INTERNO],[TPAD_ID_TIP_ADMINISTRACION],[TDED_ID_TIPO_DEDUCIBLE],[DTSL_TOPE_DEDUCIBLE])  ";


                    string defaultValues = $"VALUES (@SLCD_ID_SOLICITUD ,'{estadoReembolso}','{tipoReembolso}' ,'{prestacion}','{grupoPrestacion}' ,'{medicamento}'"+
                       $", '{diagnostico}', '{campos[31]}', '{tipoDocumento}', '{folioDocumento}', '{tipoAtencion}', '{plan}'"+
                       $", '{isapre}', '{biomedico}', '{campos[37]}', '{campos[40]}', '{campos[43]}', '{campos[44]}'" +
                       $", '{campos[45]}', '{campos[46]}', '{campos[47]}', '{campos[48]}', '{campos[49]}', '{campos[50]}'" +
                       $", '{campos[51]}', '{campos[52]}', '{campos[53]}', '{campos[54]}', '{campos[55]}', '{campos[57]}'" +
                       $", '{campos[63]}', '{campos[72]}', '{campos[75]}', '{campos[80]}', NULL, '{campos[90]}'" +
                       $", '{DateTime.Now.ToString("yyyyMMdd")}', '{DateTime.Now.ToString("yyyyMMdd")}', 'BATCH', 'BATCH', {vigencia}, '{campos[58]}'" +
                       $", '{tipoAdminstracion}', '{tipoDeducible}', '{campos[95]}')";

                    return insertQuery + defaultValues;
                }
                else
                {

                }

            }

            return "";
        }
    }
}
