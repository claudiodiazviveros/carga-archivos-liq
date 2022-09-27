using Dapper;
using Microsoft.Extensions.Configuration;
using System;
using System.Data.SqlClient;
using System.Linq;
using System.Text;

namespace CargaArchivoLiquidadores
{
    public partial class Liquidacion
    {
        public static Liquidacion[] Import(string[] lines)
        {
            return (from line in

                        from inner in lines.Skip(1) select inner.Split('|')

                    select new Liquidacion()
                    {
                        Poliza = line[0],
                        TipoPoliza = line[1],
                        FormaPagoPoliza = line[2],
                        RutClienteContratante = line[3],
                        DigitoVerificadorContratante = line[4],
                        Contratante = line[5],
                        RutIntermediario = line[6],
                        DigitoVerificadorIntermediario = line[7],
                        ApellidoPaternoIntermediario = line[8],
                        ApellidoMaternoIntermediario = line[9],
                        NombreIntermediario = line[10],
                        TipoIntermediario = line[11],
                        Sucursal = line[12],
                        NombreSucursal = line[13],
                        RutAsegurado = line[14],
                        DvAsegurado = line[15],
                        ApellidoPaternoAsegurado = line[16],
                        ApellidoMaternoAsegurado = line[17],
                        NombreAsegurado = line[18],
                        RutPaciente = line[19],
                        DigitoVerificadorPaciente = line[20],
                        ApellidoPaternoPaciente = line[21],
                        ApellidoMaternoPaciente = line[22],
                        NombrePaciente = line[23],
                        Parentesco = line[24],
                        FechaNacimiento = line[25],
                        EdadPaciente = line[26],
                        SexoPaciente = line[27],
                        NumeroSolicitud = line[28],
                        CodigoDiagnostico = line[29],
                        DescripcionDiagnostico = line[30],
                        RutPrestador = line[31],
                        NombrePrestador = line[32],
                        TipoDocumento = line[33],
                        FolioDocumento = line[34],
                        AmbulatorioHospitalario = line[35],
                        PeriodoPrestacion = line[36],
                        FechaPrestacion = line[37],
                        FechaPresentacion = line[38],
                        PeriodoPresentacion = line[39],
                        SeqLiquidacion = line[40],
                        GrupoPrestacion = line[41],
                        DescripcionGrupoPrestacion = line[42],
                        CantidadPrestacion = line[43],
                        MontoPrestacion = line[44],
                        MontoPrestacionUf = line[45],
                        MontoIsapre = line[46],
                        MontoIsapreUf = line[47],
                        MontoSolicitado = line[48],
                        MontoSolicitadoUf = line[49],
                        MontoDeducible = line[50],
                        MontoDeducibleUf = line[51],
                        MontoPago = line[52],
                        MontoPagoUf = line[53],
                        MontoRechazo = line[54],
                        MontoRechazoUf = line[55],
                        Estado = line[56],
                        FechaEstado = line[57],
                        CorrSoliInterno = line[58],
                        NumeroRemesa = line[59],
                        FechaRemesa = line[60],
                        CodigoPrestacion = line[61],
                        DescripcionPrestacion = line[62],
                        PorcentajeReembolso = line[63],
                        CantidadPrestaciones = line[64],
                        FormaPagoLiquidacion = line[65],
                        CodigoBanco = line[66],
                        Banco = line[67],
                        CuentaCorriente = line[68],
                        RecepcionCheque = line[69],
                        FechaPago = line[70],
                        NumeroEgreso = line[71],
                        Observacion = line[72],
                        CodigoIsapre = line[73],
                        Isapre = line[74],
                        FechaContable = line[75],
                        CodigoPlan = line[76],
                        CodigoGrupo = line[77],
                        DescripcionGrupo = line[78],
                        PeriodoContable = line[79],
                        CorrBeneficiario = line[80],
                        TipoReembolso = line[81],
                        FechaInicioVigencia = line[82],
                        Proveedor = line[83],
                        Convenio = line[84],
                        Admin = line[85],
                        Observacion2 = line[86],
                        Liquidador = line[87],
                        CodigoMedicamento = line[88],
                        CodigoClasificacionBiometrica = line[89],
                        MontoTopeArancel = line[90],
                        MontoSaldoDisponible = line[91],
                        FechaExtraccion = line[92],
                        EstadoRemesa = line[93],
                        TipoDeducible = line[94],
                        TopeDeducible = line[95],
                        NombrePlan = line[96],
                        FechaCreacionRemesa = line[97],
                        UsuarioCreacionRemesa = line[98],
                        FechaRecepcionCompania = line[99],
                        Region = line[100],
                        SucursalRemesa = line[101],
                        FechaCierreRemesa = line[102],
                        ObservacionSolicitud = line[103],
                        PagoContratante = line[104],
                        FechaDigitacion = line[105],
                        FechaPagoProyectada = line[106],
                        DescripcionPago = line[107],
                        CantidadDocumentos = line[108],
                        GlosaMedicamentos = line[109],
                        PagoTerceros = line[110],
                        NumeroDenuncio = line[111],

                    }).ToArray();
        }

        public static string StatementSql(Liquidacion[] liquidaciones)
        {
            StringBuilder sb = new StringBuilder();

            var remesas = liquidaciones.GroupBy(o => o.NumeroRemesa).Select(o => o.First()).ToList();
            foreach (var item in remesas)
            {
                #region Tabla REMESA

                int CantSolicLiquidadas = liquidaciones.Count(o => o.NumeroRemesa == item.NumeroRemesa && o.Estado.StartsWith("LIQUIDADA"));
                int CantSolicPendientes = liquidaciones.Count(o => o.NumeroRemesa == item.NumeroRemesa && o.Estado.StartsWith("PENDIENTE"));
                int CantSolicRechazadas = liquidaciones.Count(o => o.NumeroRemesa == item.NumeroRemesa && o.Estado.StartsWith("RECHAZADA"));
                int CantSolicAnuladas = liquidaciones.Count(o => o.NumeroRemesa == item.NumeroRemesa && o.Estado.StartsWith("ANULADA"));

                int ESRE_ID_EST_REMESA_FK = Get_ID_EST_REMESA(item.EstadoRemesa);
                string REG_ID_REGION_FK = "NULL";
                string SURE_ID_SUCURSAL_FK = "NULL";

                string sql = "INSERT INTO [dbo].[REMESA] ([ESRE_ID_EST_REMESA], [RMSA_COD_POLIZA], [RMSA_NUM_REMESA], [RMSA_FEC_REMESA], [RMSA_CANT_SOLIC_LIQUIDADAS], [RMSA_CANT_SOLIC_PENDIENTES], [RMSA_CANT_SOLIC_RECHAZADAS], [RMSA_CANT_SOLIC_ANULADAS], [RMSA_FEC_CREACION], [RMSA_FEC_ULT_ACT], [RMSA_USU_CREACION], [RMSA_USU_ULT_ACT], [RMSA_ES_VIGENTE], [RMSA_ORIGEN_DATO], [REG_ID_REGION], [SURE_ID_SUCURSAL], [RMSA_FEC_CREACION_REMESA], [RMSA_USU_CREACION_REMESA], [RMSA_FEC_RECEP_COMPANIA], [RMSA_FEC_CIERRE_REMESA]) " +
                    $"VALUES ({ESRE_ID_EST_REMESA_FK}, {item.Poliza}, '{item.NumeroRemesa}', '{item.FechaRemesa}', {CantSolicLiquidadas}, {CantSolicPendientes}, {CantSolicRechazadas}, {CantSolicAnuladas}, '{item.FechaCreacionRemesa}', '{item.FechaCreacionRemesa}', 'BATCH', 'BATCH', 1, 'P', {REG_ID_REGION_FK}, {SURE_ID_SUCURSAL_FK}, NULL, '', '{item.FechaRecepcionCompania}', '{item.FechaCierreRemesa}')";

                sb.AppendLine(sql);

                #endregion

                #region Tabla SOLICITUD

                int PLZA_ID_POLIZA_FK = Get_ID_POLIZA(item.Poliza);
                int RMSA_ID_REMESA_FK = 0;
                int ISAP_ID_ISAPRE_FK = Get_ID_ISAPRE(item.CodigoIsapre);
                int TPCG_ID_TIP_CARGA_FK = 0;
                int LIQU_ID_LIQUIDADOR_FK = Get_ID_LIQUIDADOR(item.Liquidador);
                int PRVD_ID_PROVEEDOR_FK = Get_ID_PROVEEDOR(item.Proveedor);
                int SCSL_ID_SUCURSAL_FK = Get_ID_SUCURSAL(item.Sucursal);
                int FMPR_ID_FORMA_PAGO_FK = Get_ID_FORMA_PAGO(item.FormaPagoLiquidacion);
                int BNCO_ID_BANCO_FK = Get_ID_BANCO(item.CodigoBanco);
                int TPAD_ID_TIP_ADMINISTRACION_FK = 0;
                int TPCB_ID_COBERTURA_FK = 0;
                int ESDS_ID_ESTADO_FK = Get_ID_ESTADO(item.Estado);
                int GRIN_ID_GRUPO_ING_INFORMADO_FK = Get_ID_GRUPO_ING_INFORMADO(item.CodigoGrupo);
                int POIN_ID_POOL_INFORMADO_FK = 0;
                int NPLI_ID_PLAN_INFORMADO_FK = Get_ID_PLAN_INFORMADO(item.CodigoPlan);

                sql = "INSERT INTO [dbo].[SOLICITUD] ([PLZA_ID_POLIZA], [SLCD_COD_POLIZA], [RMSA_ID_REMESA], [ISAP_ID_ISAPRE], [PERS_RUT_CONTRATANTE], [PERS_RUT_TITULAR], [PERS_RUT_PACIENTE], [TPCG_ID_TIP_CARGA], [LIQU_ID_LIQUIDADOR], [PRVD_ID_PROVEEDOR], [SCSL_ID_SUCURSAL], [SLCD_NUM_SOLICITUD], [SLCD_CORRELATIVO_INTERNO], [SLCD_FEC_PRESENTACION], [SLCD_FEC_PAGO], [FMPR_ID_FORMA_PAGO], [BNCO_ID_BANCO], [TPAD_ID_TIP_ADMINISTRACION], [TPCB_ID_COBERTURA], [PERS_RUT_CORREDOR], [ESDS_ID_ESTADO], [GRIN_ID_GRUPO_ING_INFORMADO], [POIN_ID_POOL_INFORMADO], [SLCD_FECHA_ESTADO], [SLCD_CTA_CORRIENTE], [SLCD_NUM_EGRESO], [SLCD_RECEP_CHEQUE], [SLCD_SDO_DISPONIBLE_UF], [SLCD_FEC_OCURRENCIA], [SLCD_FEC_CREACION], [SLCD_FEC_ULT_ACT], [SLCD_USU_CREACION], [SLCD_USU_ULT_ACT], [SLCD_ES_VIGENTE], [NPLI_ID_PLAN_INFORMADO], [SLCD_OBS_SOLICITUD], [SLCD_PAG_CONTRATANTE], [SLCD_FEC_DIGITACION], [SLCD_FEC_PAGO_PROYECTADA], [SLCD_DES_PAGO], [SLCD_CANT_DOCUMENTOS], [SLCD_MTO_PRESTACION], [SLCD_MTO_DEDUCIBLE], [SLCD_MTO_PAGO], [SLCD_FEC_CONTABLE], [SLCD_NUN_DENUNCIA], [SLCD_GLOSA_MEDICAMENTO], [SLCD_OBSERVACION_2]) " +
                    $"VALUES ({PLZA_ID_POLIZA_FK}, {item.Poliza}, {RMSA_ID_REMESA_FK}, {ISAP_ID_ISAPRE_FK}, {item.RutClienteContratante}, {item.RutAsegurado}, {item.RutPaciente}, {TPCG_ID_TIP_CARGA_FK}, {LIQU_ID_LIQUIDADOR_FK}, {PRVD_ID_PROVEEDOR_FK}, {SCSL_ID_SUCURSAL_FK}, {item.NumeroSolicitud}, {item.CorrSoliInterno}, '{item.FechaPresentacion}', '{item.FechaPago}', {FMPR_ID_FORMA_PAGO_FK}, {BNCO_ID_BANCO_FK}, {TPAD_ID_TIP_ADMINISTRACION_FK}, {TPCB_ID_COBERTURA_FK}, {item.RutIntermediario}, {ESDS_ID_ESTADO_FK}, {GRIN_ID_GRUPO_ING_INFORMADO_FK}, {POIN_ID_POOL_INFORMADO_FK}, '{item.FechaEstado}', '{item.CuentaCorriente}', '{item.NumeroEgreso}', '{item.RecepcionCheque}', {item.MontoSaldoDisponible}, [SLCD_FEC_OCURRENCIA], '{item.FechaCreacionRemesa}', '{item.UsuarioCreacionRemesa}', 'BATCH', 'BATCH', 1, {NPLI_ID_PLAN_INFORMADO_FK}, '{item.ObservacionSolicitud}', '{item.PagoContratante}', '{item.FechaDigitacion}', '{item.FechaPagoProyectada}', '{item.DescripcionPago}', {item.CantidadDocumentos}, {item.MontoPrestacion}, {item.MontoDeducible}, {item.MontoPago}, [SLCD_FEC_CONTABLE], {item.NumeroDenuncio}, '{item.GlosaMedicamentos}', '{item.Observacion2}')";

                sb.AppendLine(sql);

                #endregion

                var solicitudes = liquidaciones.Where(o => o.NumeroRemesa == item.NumeroRemesa).ToList();
                foreach (var item0 in solicitudes)
                {
                    #region Tabla DETALLE_SOLICITUD

                    int SLCD_ID_SOLICITUD_FK = 0;
                    int ETDO_ID_ESTADO_FK = Get_ID_ESTADO(item0.Estado);
                    int TPRB_ID_TIP_REEMBOLSO_FK = Get_ID_TIP_REEMBOLSO(item0.TipoReembolso);
                    int PRTC_ID_PRESTACION_FK = Get_ID_PRESTACION(item0.CodigoPrestacion);
                    int GRPR_ID_GRUPO_FK = Get_ID_GRUPO(item0.CodigoGrupo);
                    int MDTO_ID_MEDICAMENTO_FK = Get_ID_MEDICAMENTO(item0.CodigoMedicamento);
                    int DIAG_ID_DIAGNOSTICO_FK = Get_ID_DIAGNOSTICO(item0.CodigoDiagnostico);
                    int TPDC_ID_TIP_DOCUMENTO_FK = Get_ID_TIP_DOCUMENTO(item0.TipoDocumento);
                    int TPAT_ID_TIP_ATENCION_FK = 0;
                    int CLBI_ID_CLASIFICACION_FK = Get_ID_CLASIFICACION(item0.CodigoClasificacionBiometrica);
                    int TDED_ID_TIPO_DEDUCIBLE_FK = Get_ID_TIPO_DEDUCIBLE(item0.TipoDeducible);

                    sql = "INSERT INTO [dbo].[DETALLE_SOLICITUD] ([SLCD_ID_SOLICITUD], [ETDO_ID_ESTADO], [TPRB_ID_TIP_REEMBOLSO], [PRTC_ID_PRESTACION], [GRPR_ID_GRUPO], [MDTO_ID_MEDICAMENTO], [DIAG_ID_DIAGNOSTICO], [PRDR_RUT_PRESTADOR], [TPDC_ID_TIP_DOCUMENTO], [DTSL_FOLIO_DOCUMENTO], [TPAT_ID_TIP_ATENCION], [PLAN_ID], [ISAP_ID_ISAPRE], [CLBI_ID_CLASIFICACION], [DTSL_FEC_PRESTACION], [DTSL_SEC_LIQUIDACION], [DTSL_CANT_PRESTACIONES], [DTSL_MTO_PRESTACION_CL], [DTSL_MTO_PRESTACION_UF], [DTSL_MTO_ISAPRE_CL], [DTSL_MTO_ISAPRE_UF], [DTSL_MTO_SOLICITADO_CL], [DTSL_MTO_SOLICITADO_UF], [DTSL_MTO_DEDUCIBLE_CL], [DTSL_MTO_DEDUCIBLE_UF], [DTSL_MTO_PAGO_CL], [DTSL_MTO_PAGO_UF], [DTSL_MTO_RECHAZO_CL], [DTSL_MTO_RECHAZO_UF], [DTSL_FEC_ESTADO], [DTSL_PORCENTAJE_REEMBOLSO], [DTSL_OBSERVACIONES], [DTSL_FEC_CONTABLE], [DTSL_CORR_BENEFI], [DTSL_SECUENCIAL], [DTSL_TOPE_ARANCEL], [DTSL_FEC_CREACION], [DTSL_FEC_ULT_ACT], [DTSL_USU_CREACION], [DTSL_USU_ULT_ACT], [DTSL_ES_VIGENTE], [DTSL_CORRELATIVO_INTERNO], [TPAD_ID_TIP_ADMINISTRACION], [TDED_ID_TIPO_DEDUCIBLE], [DTSL_TOPE_DEDUCIBLE]) " +
                        $"VALUES ({SLCD_ID_SOLICITUD_FK}, {ETDO_ID_ESTADO_FK}, {TPRB_ID_TIP_REEMBOLSO_FK}, {PRTC_ID_PRESTACION_FK}, {GRPR_ID_GRUPO_FK}, {MDTO_ID_MEDICAMENTO_FK}, {DIAG_ID_DIAGNOSTICO_FK}, [PRDR_RUT_PRESTADOR], {TPDC_ID_TIP_DOCUMENTO_FK}, [DTSL_FOLIO_DOCUMENTO], {TPAT_ID_TIP_ATENCION_FK}, [PLAN_ID], {ISAP_ID_ISAPRE_FK}, {CLBI_ID_CLASIFICACION_FK}, [DTSL_FEC_PRESTACION], [DTSL_SEC_LIQUIDACION], [DTSL_CANT_PRESTACIONES], [DTSL_MTO_PRESTACION_CL], [DTSL_MTO_PRESTACION_UF], [DTSL_MTO_ISAPRE_CL], [DTSL_MTO_ISAPRE_UF], [DTSL_MTO_SOLICITADO_CL], [DTSL_MTO_SOLICITADO_UF], [DTSL_MTO_DEDUCIBLE_CL], [DTSL_MTO_DEDUCIBLE_UF], [DTSL_MTO_PAGO_CL], [DTSL_MTO_PAGO_UF], [DTSL_MTO_RECHAZO_CL], [DTSL_MTO_RECHAZO_UF], [DTSL_FEC_ESTADO], [DTSL_PORCENTAJE_REEMBOLSO], [DTSL_OBSERVACIONES], [DTSL_FEC_CONTABLE], [DTSL_CORR_BENEFI], [DTSL_SECUENCIAL], [DTSL_TOPE_ARANCEL], [DTSL_FEC_CREACION], [DTSL_FEC_ULT_ACT], 'BATCH', 'BATCH', 1, [DTSL_CORRELATIVO_INTERNO], {TPAD_ID_TIP_ADMINISTRACION_FK}, {TDED_ID_TIPO_DEDUCIBLE_FK}, [DTSL_TOPE_DEDUCIBLE])";

                    sb.AppendLine(sql);

                    #endregion
                }
            }

            return sb.ToString();
        }

        private static int Get_ID_TIPO_DEDUCIBLE(string tipoDeducible)
        {
            throw new NotImplementedException();
        }

        private static int Get_ID_CLASIFICACION(string codigoClasificacionBiometrica)
        {
            throw new NotImplementedException();
        }

        private static int Get_ID_TIP_DOCUMENTO(string tipoDocumento)
        {
            throw new NotImplementedException();
        }

        private static int Get_ID_DIAGNOSTICO(string codigoDiagnostico)
        {
            throw new NotImplementedException();
        }

        private static int Get_ID_MEDICAMENTO(string codigoMedicamento)
        {
            throw new NotImplementedException();
        }

        private static int Get_ID_GRUPO(string codigoGrupo)
        {
            throw new NotImplementedException();
        }

        private static int Get_ID_PRESTACION(string codigoPrestacion)
        {
            throw new NotImplementedException();
        }

        private static int Get_ID_TIP_REEMBOLSO(string tipoReembolso)
        {
            throw new NotImplementedException();
        }

        private static int Get_ID_PLAN_INFORMADO(string codigoPlan)
        {
            throw new NotImplementedException();
        }

        private static int Get_ID_GRUPO_ING_INFORMADO(string codigoGrupo)
        {
            throw new NotImplementedException();
        }
        private static int Get_ID_EST_REMESA(string estadoRemesa)
        {
            using (var connection = new SqlConnection(Program.configuration.GetConnectionString("DataConnection")))
            {
                string sql = $"SELECT TOP 1 ESRE_ID_EST_REMESA FROM ESTADO_REMESA WHERE ESRE_COD_EST_REMESA = '{estadoRemesa}'";
                return connection.QueryFirstOrDefault<int>(sql);
            }
        }

        private static int Get_ID_ESTADO(string estado)
        {
            throw new NotImplementedException();
        }

        private static int Get_ID_BANCO(string codigoBanco)
        {
            throw new NotImplementedException();
        }

        private static int Get_ID_FORMA_PAGO(string formaPagoLiquidacion)
        {
            throw new NotImplementedException();
        }

        private static int Get_ID_SUCURSAL(string sucursal)
        {
            throw new NotImplementedException();
        }

        private static int Get_ID_PROVEEDOR(string proveedor)
        {
            throw new NotImplementedException();
        }

        private static int Get_ID_LIQUIDADOR(string liquidador)
        {
            throw new NotImplementedException();
        }

        private static int Get_ID_ISAPRE(string codigoIsapre)
        {
            throw new NotImplementedException();
        }

        private static int Get_ID_POLIZA(string poliza)
        {
            throw new NotImplementedException();
        }
    }
}