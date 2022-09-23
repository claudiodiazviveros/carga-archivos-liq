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

            foreach (var item in liquidaciones)
            {
                #region Tabla REMESA

                string sql = "";

                sb.AppendLine(sql);

                #endregion

                #region Tabla SOLICITUD

                int PLZA_ID_POLIZA_FK = 0;
                int RMSA_ID_REMESA_FK = 0;
                int ISAP_ID_ISAPRE_FK = 0;
                int TPCG_ID_TIP_CARGA_FK = 0;
                int LIQU_ID_LIQUIDADOR_FK = 0;
                int PRVD_ID_PROVEEDOR_FK = 0;
                int SCSL_ID_SUCURSAL_FK = 0;
                int FMPR_ID_FORMA_PAGO_FK = 0;
                int BNCO_ID_BANCO_FK = 0;
                int TPAD_ID_TIP_ADMINISTRACION_FK = 0;
                int TPCB_ID_COBERTURA_FK = 0;
                int ESDS_ID_ESTADO_FK = 0;
                int GRIN_ID_GRUPO_ING_INFORMADO_FK = 0;
                int POIN_ID_POOL_INFORMADO_FK = 0;
                int NPLI_ID_PLAN_INFORMADO_FK = 0;

                sql = "INSERT INTO [dbo].[SOLICITUD] ([PLZA_ID_POLIZA], [SLCD_COD_POLIZA], [RMSA_ID_REMESA], [ISAP_ID_ISAPRE], [PERS_RUT_CONTRATANTE], [PERS_RUT_TITULAR], [PERS_RUT_PACIENTE], [TPCG_ID_TIP_CARGA], [LIQU_ID_LIQUIDADOR], [PRVD_ID_PROVEEDOR], [SCSL_ID_SUCURSAL], [SLCD_NUM_SOLICITUD], [SLCD_CORRELATIVO_INTERNO], [SLCD_FEC_PRESENTACION], [SLCD_FEC_PAGO], [FMPR_ID_FORMA_PAGO], [BNCO_ID_BANCO], [TPAD_ID_TIP_ADMINISTRACION], [TPCB_ID_COBERTURA], [PERS_RUT_CORREDOR], [ESDS_ID_ESTADO], [GRIN_ID_GRUPO_ING_INFORMADO], [POIN_ID_POOL_INFORMADO], [SLCD_FECHA_ESTADO], [SLCD_CTA_CORRIENTE], [SLCD_NUM_EGRESO], [SLCD_RECEP_CHEQUE], [SLCD_SDO_DISPONIBLE_UF], [SLCD_FEC_OCURRENCIA], [SLCD_FEC_CREACION], [SLCD_FEC_ULT_ACT], [SLCD_USU_CREACION], [SLCD_USU_ULT_ACT], [SLCD_ES_VIGENTE], [NPLI_ID_PLAN_INFORMADO], [SLCD_OBS_SOLICITUD], [SLCD_PAG_CONTRATANTE], [SLCD_FEC_DIGITACION], [SLCD_FEC_PAGO_PROYECTADA], [SLCD_DES_PAGO], [SLCD_CANT_DOCUMENTOS], [SLCD_MTO_PRESTACION], [SLCD_MTO_DEDUCIBLE], [SLCD_MTO_PAGO], [SLCD_FEC_CONTABLE], [SLCD_NUN_DENUNCIA], [SLCD_GLOSA_MEDICAMENTO], [SLCD_OBSERVACION_2]) " +
                    $"VALUES ({PLZA_ID_POLIZA_FK}, {item.Poliza}, {RMSA_ID_REMESA_FK}, {ISAP_ID_ISAPRE_FK}, {item.RutClienteContratante}, {item.RutAsegurado}, {item.RutPaciente}, {TPCG_ID_TIP_CARGA_FK}, {LIQU_ID_LIQUIDADOR_FK}, {PRVD_ID_PROVEEDOR_FK}, {SCSL_ID_SUCURSAL_FK}, {item.NumeroSolicitud}, {item.CorrSoliInterno}, '{item.FechaPresentacion}', '{item.FechaPago}', {FMPR_ID_FORMA_PAGO_FK}, {BNCO_ID_BANCO_FK}, {TPAD_ID_TIP_ADMINISTRACION_FK}, {TPCB_ID_COBERTURA_FK}, {item.RutIntermediario}, {ESDS_ID_ESTADO_FK}, {GRIN_ID_GRUPO_ING_INFORMADO_FK}, {POIN_ID_POOL_INFORMADO_FK}, '{item.FechaEstado}', '{item.CuentaCorriente}', '{item.NumeroEgreso}', '{item.RecepcionCheque}', {item.MontoSaldoDisponible}, [SLCD_FEC_OCURRENCIA], '{item.FechaCreacionRemesa}', '{item.UsuarioCreacionRemesa}', 'BATCH', 'BATCH', 1, {NPLI_ID_PLAN_INFORMADO_FK}, '{item.ObservacionSolicitud}', '{item.PagoContratante}', '{item.FechaDigitacion}', '{item.FechaPagoProyectada}', '{item.DescripcionPago}', {item.CantidadDocumentos}, {item.MontoPrestacion}, {item.MontoDeducible}, {item.MontoPago}, [SLCD_FEC_CONTABLE], {item.NumeroDenuncio}, '{item.GlosaMedicamentos}', '{item.Observacion2}')";

                sb.AppendLine(sql);

                #endregion

                #region Tabla DETALLE_SOLICITUD

                sql = "";

                sb.AppendLine(sql);

                #endregion
            }

            return sb.ToString();
        }
    }
}