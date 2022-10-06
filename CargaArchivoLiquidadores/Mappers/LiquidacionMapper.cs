using Dapper;
using Microsoft.Extensions.Configuration;
using Serilog;
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
                        MontoPrestacion = Convert.ToDecimal(line[44].Replace(",", ".")),
                        MontoPrestacionUf = Convert.ToDecimal(line[45].Replace(",", ".")),
                        MontoIsapre = Convert.ToDecimal(line[46].Replace(",", ".")),
                        MontoIsapreUf = Convert.ToDecimal(line[47].Replace(",", ".")),
                        MontoSolicitado = Convert.ToDecimal(line[48].Replace(",", ".")),
                        MontoSolicitadoUf = Convert.ToDecimal(line[49].Replace(",", ".")),
                        MontoDeducible = Convert.ToDecimal(line[50].Replace(",", ".")),
                        MontoDeducibleUf = Convert.ToDecimal(line[51].Replace(",", ".")),
                        MontoPago = Convert.ToDecimal(line[52].Replace(",", ".")),
                        MontoPagoUf = Convert.ToDecimal(line[53].Replace(",", ".")),
                        MontoRechazo = Convert.ToDecimal(line[54].Replace(",", ".")),
                        MontoRechazoUf = Convert.ToDecimal(line[55].Replace(",", ".")),
                        Estado = line[56],
                        FechaEstado = line[57],
                        CorrSoliInterno = line[58],
                        NumeroRemesa = line[59],
                        FechaRemesa = line[60],
                        CodigoPrestacion = line[61],
                        DescripcionPrestacion = line[62],
                        PorcentajeReembolso = Convert.ToDecimal(string.IsNullOrEmpty(line[63]) ? "0" : line[63].Replace(",", ".")),
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
                        MontoSaldoDisponible = Convert.ToDecimal(line[91].Replace(",", ".")),
                        FechaExtraccion = line[92],
                        EstadoRemesa = line[93],
                        TipoDeducible = line[94],
                        TopeDeducible = Convert.ToDecimal(line[95].Replace(",", ".")),
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

            int newRows = 0;
            string sql;

            if (liquidaciones.Count() > 0)
            {
                sql = $"DECLARE @ID_REMESA INT";
                sb.AppendLine(sql);

                sql = $"DECLARE @ID_SOLICITUD INT";
                sb.AppendLine(sql);
            }

            // 1. Agrupar por poliza + remesa.
            var remesas = liquidaciones
                .GroupBy(o => new {o.Poliza, o.NumeroRemesa})
                .Select(o => o.First())
                .ToList();
            foreach (var item in remesas)
            {
                try
                {
                    #region Tabla REMESA

                    int CantSolicLiquidadas = liquidaciones.Count(o => o.Poliza == item.Poliza && o.NumeroRemesa == item.NumeroRemesa && o.Estado.StartsWith("LIQUIDADA"));
                    int CantSolicPendientes = liquidaciones.Count(o => o.Poliza == item.Poliza && o.NumeroRemesa == item.NumeroRemesa && o.Estado.StartsWith("PENDIENTE"));
                    int CantSolicRechazadas = liquidaciones.Count(o => o.Poliza == item.Poliza && o.NumeroRemesa == item.NumeroRemesa && o.Estado.StartsWith("RECHAZADA"));
                    int CantSolicAnuladas = liquidaciones.Count(o => o.Poliza == item.Poliza && o.NumeroRemesa == item.NumeroRemesa && o.Estado.StartsWith("ANULADA"));

                    int ESRE_ID_EST_REMESA_FK = Get_ID_EST_REMESA(item.EstadoRemesa);
                    int REG_ID_REGION_FK = Get_ID_REGION(item.Region);
                    string SURE_ID_SUCURSAL_FK = "NULL";

                    // Cambio formato fecha
                    string fechaRemesa = item.FechaRemesa.Substring(6, 4) + item.FechaRemesa.Substring(3, 2) + item.FechaRemesa.Substring(0, 2);

                    string fechaCreacionRemesa = "GETDATE()";
                    if (!string.IsNullOrEmpty(item.FechaCreacionRemesa))
                    {
                        fechaCreacionRemesa = "'" + item.FechaCreacionRemesa.Substring(6, 4) + item.FechaCreacionRemesa.Substring(3, 2) + item.FechaCreacionRemesa.Substring(0, 2) + "'";
                    }

                    string fechaRecepcionCompania = "GETDATE()";
                    if (!string.IsNullOrEmpty(item.FechaRecepcionCompania))
                    {
                        fechaRecepcionCompania = "'" + item.FechaRecepcionCompania.Substring(6, 4) + item.FechaRecepcionCompania.Substring(3, 2) + item.FechaRecepcionCompania.Substring(0, 2) + "'";
                    }

                    string fechaCierreRemesa = "GETDATE()";
                    if (!string.IsNullOrEmpty(item.FechaCierreRemesa))
                    {
                        fechaCierreRemesa = "'" + item.FechaCierreRemesa.Substring(6, 4) + item.FechaCierreRemesa.Substring(3, 2) + item.FechaCierreRemesa.Substring(0, 2) + "'";
                    }


                    // TSQL
                    sql = "INSERT INTO [dbo].[REMESA] ([ESRE_ID_EST_REMESA], [RMSA_COD_POLIZA], [RMSA_NUM_REMESA], [RMSA_FEC_REMESA], [RMSA_CANT_SOLIC_LIQUIDADAS], [RMSA_CANT_SOLIC_PENDIENTES], [RMSA_CANT_SOLIC_RECHAZADAS], [RMSA_CANT_SOLIC_ANULADAS], [RMSA_FEC_CREACION], [RMSA_FEC_ULT_ACT], [RMSA_USU_CREACION], [RMSA_USU_ULT_ACT], [RMSA_ES_VIGENTE], [RMSA_ORIGEN_DATO], [REG_ID_REGION], [SURE_ID_SUCURSAL], [RMSA_FEC_CREACION_REMESA], [RMSA_USU_CREACION_REMESA], [RMSA_FEC_RECEP_COMPANIA], [RMSA_FEC_CIERRE_REMESA]) " +
                        $"VALUES ({ESRE_ID_EST_REMESA_FK}, {item.Poliza}, '{item.NumeroRemesa}', '{fechaRemesa}', {CantSolicLiquidadas}, {CantSolicPendientes}, {CantSolicRechazadas}, {CantSolicAnuladas}, {fechaCreacionRemesa}, {fechaCreacionRemesa}, 'BATCH', 'BATCH', 1, 'P', {REG_ID_REGION_FK}, {SURE_ID_SUCURSAL_FK}, NULL, '-1', {fechaRecepcionCompania}, {fechaCierreRemesa})";
                    sb.AppendLine(sql);

                    sql = $"SET @ID_REMESA = SCOPE_IDENTITY()";
                    sb.AppendLine(sql);

                    #endregion

                    // 2. Filtro poliza + remesa y agrupo correlativo interno.
                    var solicitudes = liquidaciones
                        .Where(o => o.Poliza == item.Poliza && o.NumeroRemesa == item.NumeroRemesa)
                        .GroupBy(o => new { o.Poliza, o.NumeroRemesa, o.CorrSoliInterno })
                        .Select(o => o.First())
                        .ToList();
                    foreach (var item0 in solicitudes)
                    {                      
                        #region Tabla SOLICITUD

                        int PLZA_ID_POLIZA_FK = Get_ID_POLIZA(item0.Poliza);
                        string RMSA_ID_REMESA_FK = $"@ID_REMESA";
                        int ISAP_ID_ISAPRE_FK = Get_ID_ISAPRE(item0.CodigoIsapre);
                        int TPCG_ID_TIP_CARGA_FK = Get_ID_TIP_CARGA(item0.Parentesco);
                        int LIQU_ID_LIQUIDADOR_FK = Get_ID_LIQUIDADOR(item0.Liquidador);
                        int PRVD_ID_PROVEEDOR_FK = Get_ID_PROVEEDOR(item0.Proveedor);
                        int SCSL_ID_SUCURSAL_FK = Get_ID_SUCURSAL(item0.Sucursal);
                        int FMPR_ID_FORMA_PAGO_FK = Get_ID_FORMA_PAGO(item0.FormaPagoLiquidacion);
                        int BNCO_ID_BANCO_FK = Get_ID_BANCO(item0.CodigoBanco);
                        int TPAD_ID_TIP_ADMINISTRACION_FK = Get_TIP_ADMINISTRACION(item0.Admin);
                        int TPCB_ID_COBERTURA_FK = Get_ID_COBERTURA(item0.TipoPoliza);
                        int ESDS_ID_ESTADO_FK = Get_ID_ESTADO(item0.Estado);
                        int GRIN_ID_GRUPO_ING_INFORMADO_FK = Get_ID_GRUPO_ING_INFORMADO(item0.CodigoGrupo);
                        int POIN_ID_POOL_INFORMADO_FK = Get_ID_POOL_INFORMADO(item0.Convenio.Trim().Replace("�", " "));
                        int NPLI_ID_PLAN_INFORMADO_FK = Get_ID_PLAN_INFORMADO(item0.CodigoPlan);
                        string SLCD_FEC_OCURRENCIA = "GETDATE()";
                        string SLCD_FEC_CONTABLE = "GETDATE()";
                        string SLCD_FEC_ULT_ACT = "GETDATE()";

                        // Cambio formato fecha
                        string fechaPresentacion = item0.FechaPresentacion.Substring(6, 4) + item0.FechaPresentacion.Substring(3, 2) + item0.FechaPresentacion.Substring(0, 2);
                        string fechaPago = item0.FechaPago.Substring(6, 4) + item0.FechaPago.Substring(3, 2) + item0.FechaPago.Substring(0, 2);
                        string fechaEstado = item0.FechaEstado.Substring(6, 4) + item0.FechaEstado.Substring(3, 2) + item0.FechaEstado.Substring(0, 2);                      

                        string fechaDigitacion = "NULL";
                        if (!string.IsNullOrEmpty(item0.FechaDigitacion))
                        {
                            fechaDigitacion = "'" + item0.FechaDigitacion.Substring(6, 4) + item0.FechaDigitacion.Substring(3, 2) + item0.FechaDigitacion.Substring(0, 2) + "'";
                        }

                        string fechaPagoProyectada = "NULL";
                        if (!string.IsNullOrEmpty(item0.FechaPagoProyectada))
                        {
                            fechaPagoProyectada = "'" + item0.FechaPagoProyectada.Substring(6, 4) + item0.FechaPagoProyectada.Substring(3, 2) + item0.FechaPagoProyectada.Substring(0, 2) + "'";
                        }

                        // Suma de Monto disponible, Monto prestacion, Monto deducible
                        string montoSaldoDisponible = liquidaciones
                            .Where(o => o.Poliza == item0.Poliza && o.NumeroRemesa == item0.NumeroRemesa && o.CorrSoliInterno == item0.CorrSoliInterno)
                            .Sum(o => o.MontoSaldoDisponible)
                            .ToString("0", System.Globalization.CultureInfo.InvariantCulture);

                        string montoPrestacion = liquidaciones
                            .Where(o => o.Poliza == item0.Poliza && o.NumeroRemesa == item0.NumeroRemesa && o.CorrSoliInterno == item0.CorrSoliInterno)
                            .Sum(o => o.MontoPrestacion)
                            .ToString("0", System.Globalization.CultureInfo.InvariantCulture);

                        string montoDeducible = liquidaciones
                            .Where(o => o.Poliza == item0.Poliza && o.NumeroRemesa == item0.NumeroRemesa && o.CorrSoliInterno == item0.CorrSoliInterno)
                            .Sum(o => o.MontoDeducible)
                            .ToString("0", System.Globalization.CultureInfo.InvariantCulture);

                        string montoPago = liquidaciones
                            .Where(o => o.Poliza == item0.Poliza && o.NumeroRemesa == item0.NumeroRemesa && o.CorrSoliInterno == item0.CorrSoliInterno)
                            .Sum(o => o.MontoPago)
                            .ToString("0", System.Globalization.CultureInfo.InvariantCulture);

                        // TSQL
                        sql = "INSERT INTO [dbo].[SOLICITUD] ([PLZA_ID_POLIZA], [SLCD_COD_POLIZA], [RMSA_ID_REMESA], [ISAP_ID_ISAPRE], [PERS_RUT_CONTRATANTE], [PERS_RUT_TITULAR], [PERS_RUT_PACIENTE], [TPCG_ID_TIP_CARGA], [LIQU_ID_LIQUIDADOR], [PRVD_ID_PROVEEDOR], [SCSL_ID_SUCURSAL], [SLCD_NUM_SOLICITUD], [SLCD_CORRELATIVO_INTERNO], [SLCD_FEC_PRESENTACION], [SLCD_FEC_PAGO], [FMPR_ID_FORMA_PAGO], [BNCO_ID_BANCO], [TPAD_ID_TIP_ADMINISTRACION], [TPCB_ID_COBERTURA], [PERS_RUT_CORREDOR], [ESDS_ID_ESTADO], [GRIN_ID_GRUPO_ING_INFORMADO], [POIN_ID_POOL_INFORMADO], [SLCD_FECHA_ESTADO], [SLCD_CTA_CORRIENTE], [SLCD_NUM_EGRESO], [SLCD_RECEP_CHEQUE], [SLCD_SDO_DISPONIBLE_UF], [SLCD_FEC_OCURRENCIA], [SLCD_FEC_CREACION], [SLCD_FEC_ULT_ACT], [SLCD_USU_CREACION], [SLCD_USU_ULT_ACT], [SLCD_ES_VIGENTE], [NPLI_ID_PLAN_INFORMADO], [SLCD_OBS_SOLICITUD], [SLCD_PAG_CONTRATANTE], [SLCD_FEC_DIGITACION], [SLCD_FEC_PAGO_PROYECTADA], [SLCD_DES_PAGO], [SLCD_CANT_DOCUMENTOS], [SLCD_MTO_PRESTACION], [SLCD_MTO_DEDUCIBLE], [SLCD_MTO_PAGO], [SLCD_FEC_CONTABLE], [SLCD_NUN_DENUNCIA], [SLCD_GLOSA_MEDICAMENTO], [SLCD_OBSERVACION_2]) " +
                            $"VALUES ({PLZA_ID_POLIZA_FK}, {item0.Poliza}, {RMSA_ID_REMESA_FK}, {ISAP_ID_ISAPRE_FK}, {item0.RutClienteContratante}, {item0.RutAsegurado}, {item0.RutPaciente}, {TPCG_ID_TIP_CARGA_FK}, {LIQU_ID_LIQUIDADOR_FK}, {PRVD_ID_PROVEEDOR_FK}, {SCSL_ID_SUCURSAL_FK}, {item0.NumeroSolicitud}, {item0.CorrSoliInterno}, '{fechaPresentacion}', '{fechaPago}', {FMPR_ID_FORMA_PAGO_FK}, {BNCO_ID_BANCO_FK}, {TPAD_ID_TIP_ADMINISTRACION_FK}, {TPCB_ID_COBERTURA_FK}, {item0.RutIntermediario}, {ESDS_ID_ESTADO_FK}, {GRIN_ID_GRUPO_ING_INFORMADO_FK}, {POIN_ID_POOL_INFORMADO_FK}, '{fechaEstado}', '{item0.CuentaCorriente}', '{item0.NumeroEgreso}', '{item0.RecepcionCheque}', {montoSaldoDisponible}, {SLCD_FEC_OCURRENCIA}, {fechaCreacionRemesa}, {SLCD_FEC_ULT_ACT}, 'BATCH', 'BATCH', 1, {NPLI_ID_PLAN_INFORMADO_FK}, '{item0.ObservacionSolicitud}', '{item0.PagoContratante}', {fechaDigitacion}, {fechaPagoProyectada}, '{item0.DescripcionPago}', {item0.CantidadDocumentos}, {montoPrestacion}, {montoDeducible}, {montoPago}, {SLCD_FEC_CONTABLE}, {item0.NumeroDenuncio}, '{item0.GlosaMedicamentos}', '{item0.Observacion2}')";
                        sb.AppendLine(sql);

                        sql = $"SET @ID_SOLICITUD = SCOPE_IDENTITY()";
                        sb.AppendLine(sql);

                        #endregion

                        // 3. Filtro poliza + remesa + correlativo interno.
                        var detalles = liquidaciones
                            .Where(o => o.Poliza == item0.Poliza && o.NumeroRemesa == item0.NumeroRemesa && o.CorrSoliInterno == item0.CorrSoliInterno)
                            .ToList();
                        foreach (var item1 in detalles)
                        {
                            #region Tabla DETALLE_SOLICITUD

                            string SLCD_ID_SOLICITUD_FK = $"@ID_SOLICITUD";
                            int ETDO_ID_ESTADO_FK = Get_ID_ESTADO(item1.Estado);
                            int TPRB_ID_TIP_REEMBOLSO_FK = Get_ID_TIP_REEMBOLSO(item1.TipoReembolso);
                            int PRTC_ID_PRESTACION_FK = Get_ID_PRESTACION(item1.CodigoPrestacion);
                            int GRPR_ID_GRUPO_FK = Get_ID_GRUPO(item1.CodigoGrupo);
                            int MDTO_ID_MEDICAMENTO_FK = Get_ID_MEDICAMENTO(item1.CodigoMedicamento);
                            int DIAG_ID_DIAGNOSTICO_FK = Get_ID_DIAGNOSTICO(item1.CodigoDiagnostico);
                            int TPDC_ID_TIP_DOCUMENTO_FK = Get_ID_TIP_DOCUMENTO(item1.TipoDocumento);
                            int TPAT_ID_TIP_ATENCION_FK = Get_ID_TIP_ATENCION(item1.AmbulatorioHospitalario);
                            int CLBI_ID_CLASIFICACION_FK = Get_ID_CLASIFICACION(item1.CodigoClasificacionBiometrica);
                            int TDED_ID_TIPO_DEDUCIBLE_FK = Get_ID_TIPO_DEDUCIBLE(item1.TipoDeducible);
                            int PLAN_ID_FK = Get_PLAN_ID(item1.CodigoPlan, item1.Poliza);                            
                            string DTSL_SECUENCIAL = "NULL";
                            string SLCD_FEC_ULT_ACT1 = "GETDATE()";

                            // Formatea montos
                            montoPrestacion = item1.MontoPrestacion
                                .ToString("0", System.Globalization.CultureInfo.InvariantCulture);

                            string montoPrestacionUf = item1.MontoPrestacionUf
                                .ToString("0.000000", System.Globalization.CultureInfo.InvariantCulture);

                            montoDeducible = item1.MontoDeducible
                                .ToString("0", System.Globalization.CultureInfo.InvariantCulture);

                            string montoDeducibleUf = item1.MontoDeducibleUf
                                .ToString("0.000000", System.Globalization.CultureInfo.InvariantCulture);

                            string montoIsapre = item1.MontoIsapre
                                .ToString("0", System.Globalization.CultureInfo.InvariantCulture);

                            string montoIsapreUf = item1.MontoIsapreUf
                                .ToString("0.000000", System.Globalization.CultureInfo.InvariantCulture);

                            string montoSolicitado = item1.MontoSolicitado
                                .ToString("0", System.Globalization.CultureInfo.InvariantCulture);

                            string montoSolicitadoUf = item1.MontoSolicitadoUf
                                .ToString("0.000000", System.Globalization.CultureInfo.InvariantCulture);

                            montoPago = item1.MontoPago
                                .ToString("0", System.Globalization.CultureInfo.InvariantCulture);

                            string montoPagoUf = item1.MontoPagoUf
                                .ToString("0.000000", System.Globalization.CultureInfo.InvariantCulture);

                            string montoRechazo = item1.MontoRechazo
                                .ToString("0", System.Globalization.CultureInfo.InvariantCulture);

                            string montoRechazoUf = item1.MontoRechazoUf
                                .ToString("0.000000", System.Globalization.CultureInfo.InvariantCulture);

                            string topeDeducible = item1.TopeDeducible
                                .ToString("0.00", System.Globalization.CultureInfo.InvariantCulture);

                            string porcentajeReembolso = item1.PorcentajeReembolso
                                .ToString("0.00", System.Globalization.CultureInfo.InvariantCulture);

                            string cantidadPrestaciones = string.IsNullOrEmpty(item1.CantidadPrestaciones) ? "0" : item1.CantidadPrestaciones;


                            // Cambio formato fecha
                            string fechaCreacionRemesa1 = "GETDATE()";
                            if (!string.IsNullOrEmpty(item1.FechaCreacionRemesa))
                            {
                                fechaCreacionRemesa1 = "'" + item1.FechaCreacionRemesa.Substring(6, 4) + item1.FechaCreacionRemesa.Substring(3, 2) + item1.FechaCreacionRemesa.Substring(0, 2) + "'";
                            }

                            string fechaPrestacion = item1.FechaPrestacion.Substring(6, 4) + item1.FechaPrestacion.Substring(3, 2) + item1.FechaPrestacion.Substring(0, 2);
                            string fechaEstado1 = item1.FechaEstado.Substring(6, 4) + item1.FechaEstado.Substring(3, 2) + item1.FechaEstado.Substring(0, 2);
                            string fechaContable = item1.FechaContable.Substring(6, 4) + item1.FechaContable.Substring(3, 2) + item1.FechaContable.Substring(0, 2);
                            

                            // TSQL
                            sql = "INSERT INTO [dbo].[DETALLE_SOLICITUD] ([SLCD_ID_SOLICITUD], [ETDO_ID_ESTADO], [TPRB_ID_TIP_REEMBOLSO], [PRTC_ID_PRESTACION], [GRPR_ID_GRUPO], [MDTO_ID_MEDICAMENTO], [DIAG_ID_DIAGNOSTICO], [PRDR_RUT_PRESTADOR], [TPDC_ID_TIP_DOCUMENTO], [DTSL_FOLIO_DOCUMENTO], [TPAT_ID_TIP_ATENCION], [PLAN_ID], [ISAP_ID_ISAPRE], [CLBI_ID_CLASIFICACION], [DTSL_FEC_PRESTACION], [DTSL_SEC_LIQUIDACION], [DTSL_CANT_PRESTACIONES], [DTSL_MTO_PRESTACION_CL], [DTSL_MTO_PRESTACION_UF], [DTSL_MTO_ISAPRE_CL], [DTSL_MTO_ISAPRE_UF], [DTSL_MTO_SOLICITADO_CL], [DTSL_MTO_SOLICITADO_UF], [DTSL_MTO_DEDUCIBLE_CL], [DTSL_MTO_DEDUCIBLE_UF], [DTSL_MTO_PAGO_CL], [DTSL_MTO_PAGO_UF], [DTSL_MTO_RECHAZO_CL], [DTSL_MTO_RECHAZO_UF], [DTSL_FEC_ESTADO], [DTSL_PORCENTAJE_REEMBOLSO], [DTSL_OBSERVACIONES], [DTSL_FEC_CONTABLE], [DTSL_CORR_BENEFI], [DTSL_SECUENCIAL], [DTSL_TOPE_ARANCEL], [DTSL_FEC_CREACION], [DTSL_FEC_ULT_ACT], [DTSL_USU_CREACION], [DTSL_USU_ULT_ACT], [DTSL_ES_VIGENTE], [DTSL_CORRELATIVO_INTERNO], [TPAD_ID_TIP_ADMINISTRACION], [TDED_ID_TIPO_DEDUCIBLE], [DTSL_TOPE_DEDUCIBLE]) " +
                                $"VALUES ({SLCD_ID_SOLICITUD_FK}, {ETDO_ID_ESTADO_FK}, {TPRB_ID_TIP_REEMBOLSO_FK}, {PRTC_ID_PRESTACION_FK}, {GRPR_ID_GRUPO_FK}, {MDTO_ID_MEDICAMENTO_FK}, {DIAG_ID_DIAGNOSTICO_FK}, {item1.RutPrestador}, {TPDC_ID_TIP_DOCUMENTO_FK}, {item1.FolioDocumento}, {TPAT_ID_TIP_ATENCION_FK}, {PLAN_ID_FK}, {ISAP_ID_ISAPRE_FK}, {CLBI_ID_CLASIFICACION_FK}, '{fechaPrestacion}', {item1.SeqLiquidacion}, {cantidadPrestaciones}, {montoPrestacion}, {montoPrestacionUf}, {montoIsapre}, {montoIsapreUf}, {montoSolicitado}, {montoSolicitadoUf}, {montoDeducible}, {montoDeducibleUf}, {montoPago}, {montoPagoUf}, {montoRechazo}, {montoRechazoUf}, '{fechaEstado1}', {porcentajeReembolso}, '{item1.Observacion}', '{fechaContable}', {item1.CorrBeneficiario}, {DTSL_SECUENCIAL}, {item1.MontoTopeArancel}, {fechaCreacionRemesa1}, {SLCD_FEC_ULT_ACT1}, 'BATCH', 'BATCH', 1, {item1.CorrSoliInterno}, {TPAD_ID_TIP_ADMINISTRACION_FK}, {TDED_ID_TIPO_DEDUCIBLE_FK}, {topeDeducible})";
                            sb.AppendLine(sql);

                            #endregion
                        
                            newRows++;
                        }

                        sb.AppendLine("");
                    }
                }
                catch (Exception ex)
                {
                    Log.Error(ex.Message);
                }
            }

            Log.Information($"Registros nuevos: {newRows}");

            return sb.ToString();
        }

        private static int Get_PLAN_ID(string codigoPlan, string poliza)
        {
            int ret = 0;

            using (var connection = new SqlConnection(Program.configuration.GetConnectionString("DataConnection")))
            {
                string sql = $"SELECT TOP 1 PLAN_ID FROM PLANES WHERE PLAN_CODIGO_PLAN = '{codigoPlan}' AND PLAN_COD_POLIZA = '{poliza}'";
                ret = connection.QueryFirstOrDefault<int>(sql);
            }

            if (ret == 0) throw new Exception($"PLANES {codigoPlan} - {poliza} no existente!");

            return ret;
        }

        private static int Get_ID_TIP_ATENCION(string ambulatorioHospitalario)
        {
            int ret = 0;

            using (var connection = new SqlConnection(Program.configuration.GetConnectionString("DataConnection")))
            {
                string sql = $"SELECT TOP 1 TPAT_ID_TIP_ATENCION FROM TIPO_ATENCION WHERE TPAT_COD_TIP_ATENCION = '{ambulatorioHospitalario}'";
                ret = connection.QueryFirstOrDefault<int>(sql);
            }

            if (ret == 0) throw new Exception($"TIPO_ATENCION {ambulatorioHospitalario} no existente!");

            return ret;
        }

        private static int Get_ID_TIP_CARGA(string parentesco)
        {
            int ret = 0;

            using (var connection = new SqlConnection(Program.configuration.GetConnectionString("DataConnection")))
            {
                string sql = $"SELECT TOP 1 TPCG_ID_TIP_CARGA FROM TIPO_CARGA WHERE TPCG_DES_TIP_CARGA = '{parentesco}'";
                ret = connection.QueryFirstOrDefault<int>(sql);
            }

            if (ret == 0) throw new Exception($"TIPO_CARGA {parentesco} no existente!");

            return ret;
        }

        private static int Get_ID_COBERTURA(string tipoPoliza)
        {
            int ret = 0;

            using (var connection = new SqlConnection(Program.configuration.GetConnectionString("DataConnection")))
            {
                string sql = $"SELECT TOP 1 TPCB_ID_COBERTURA FROM TIPO_COBERTURA WHERE TPCB_DES_COBERTURA = '{tipoPoliza}'";
                ret = connection.QueryFirstOrDefault<int>(sql);
            }

            if (ret == 0) throw new Exception($"TIPO_COBERTURA {tipoPoliza} no existente!");

            return ret;
        }

        private static int Get_TIP_ADMINISTRACION(string admin)
        {
            int ret = 0;

            using (var connection = new SqlConnection(Program.configuration.GetConnectionString("DataConnection")))
            {
                string sql = $"SELECT TOP 1 TPAD_ID_TIP_ADMINISTRACION FROM TIPO_ADMINISTRACION WHERE TPAD_COD_TIP_ADMINISTRACION = '{admin}'";
                ret = connection.QueryFirstOrDefault<int>(sql);
            }

            if (ret == 0) throw new Exception($"TIPO_ADMINISTRACION {admin} no existente!");

            return ret;
        }

        private static int Get_ID_POOL_INFORMADO(string convenio)
        {
            int ret = 0;

            using (var connection = new SqlConnection(Program.configuration.GetConnectionString("DataConnection")))
            {
                string sql = $"SELECT TOP 1 POIN_ID_POOL_INFORMADO FROM POOL_INFORMADO WHERE POIN_NOM_POOL_INFORMADO = '{convenio}'";
                ret = connection.QueryFirstOrDefault<int>(sql);
            }

            if (ret == 0) throw new Exception($"POOL_INFORMADO {convenio} no existente!");

            return ret;
        }

        private static int Get_ID_REGION(string region)
        {
            int ret = 0;

            if (string.IsNullOrEmpty(region))
            {
                return 15;
            }

            using (var connection = new SqlConnection(Program.configuration.GetConnectionString("DataConnection")))
            {
                string sql = $"SELECT TOP 1 REG_ID_REGION FROM REGION WHERE REG_NOM_REGION = '{region}'";
                ret = connection.QueryFirstOrDefault<int>(sql);
            }

            if (ret == 0) throw new Exception($"REGION {region} no existente!");

            return ret;
        }

        private static int Get_ID_TIPO_DEDUCIBLE(string tipoDeducible)
        {
            int ret = 0;

            if (string.IsNullOrEmpty(tipoDeducible))
            {
                return 5;   // NO APLICA
            }

            using (var connection = new SqlConnection(Program.configuration.GetConnectionString("DataConnection")))
            {
                string sql = $"SELECT TOP 1 TDED_ID_TIPO_DEDUCIBLE FROM TIPO_DEDUCIBLE WHERE TDED_COD_TIPO_DEDUCIBLE = '{tipoDeducible}'";
                ret = connection.QueryFirstOrDefault<int>(sql);
            }

            if (ret == 0) throw new Exception($"TIPO_DEDUCIBLE {tipoDeducible} no existente!");

            return ret;
        }

        private static int Get_ID_CLASIFICACION(string codigoClasificacionBiometrica)
        {
            int ret = 0;

            using (var connection = new SqlConnection(Program.configuration.GetConnectionString("DataConnection")))
            {
                string sql = $"SELECT TOP 1 CLBI_ID_CLASIFICACION FROM CLASIFICACION_BIOMEDICA WHERE CLBI_COD_CLASIFICACION = '{codigoClasificacionBiometrica}'";
                ret = connection.QueryFirstOrDefault<int>(sql);
            }

            if (ret == 0) throw new Exception($"CLASIFICACION_BIOMEDICA {codigoClasificacionBiometrica} no existente!");

            return ret;
        }

        private static int Get_ID_TIP_DOCUMENTO(string tipoDocumento)
        {
            int ret = 0;

            using (var connection = new SqlConnection(Program.configuration.GetConnectionString("DataConnection")))
            {
                string sql = $"SELECT TOP 1 TPDC_ID_TIP_DOCUMENTO FROM TIPO_DOCUMENTO WHERE TPDC_COD_TIP_DOCUMENTO = '{tipoDocumento}'";
                ret = connection.QueryFirstOrDefault<int>(sql);
            }

            if (ret == 0) throw new Exception($"TIPO_DOCUMENTO {tipoDocumento} no existente!");

            return ret;
        }

        private static int Get_ID_DIAGNOSTICO(string codigoDiagnostico)
        {
            int ret = 0;

            using (var connection = new SqlConnection(Program.configuration.GetConnectionString("DataConnection")))
            {
                string sql = $"SELECT TOP 1 DIAG_ID_DIAGNOSTICO FROM DIAGNOSTICO WHERE DIAG_COD_DIAGNOSTICO = '{codigoDiagnostico}'";
                ret = connection.QueryFirstOrDefault<int>(sql);
            }

            if (ret == 0) throw new Exception($"DIAGNOSTICO {codigoDiagnostico} no existente!");

            return ret;
        }

        private static int Get_ID_MEDICAMENTO(string codigoMedicamento)
        {
            int ret = 0;

            using (var connection = new SqlConnection(Program.configuration.GetConnectionString("DataConnection")))
            {
                string sql = $"SELECT TOP 1 MDTO_ID_MEDICAMENTO FROM MEDICAMENTO WHERE MDTO_COD_MEDICAMENTO = '{codigoMedicamento}'";
                ret = connection.QueryFirstOrDefault<int>(sql);
            }

            if (ret == 0) throw new Exception($"MEDICAMENTO {codigoMedicamento} no existente!");

            return ret;
        }

        private static int Get_ID_GRUPO(string codigoGrupo)
        {
            int ret = 0;

            using (var connection = new SqlConnection(Program.configuration.GetConnectionString("DataConnection")))
            {
                string sql = $"SELECT TOP 1 GRPR_ID_GRUPO FROM GRUPO_PRESTACION WHERE GRPR_COD_GRUPO = '{codigoGrupo}'";
                ret = connection.QueryFirstOrDefault<int>(sql);
            }

            if (ret == 0) throw new Exception($"GRUPO_PRESTACION {codigoGrupo} no existente!");

            return ret;
        }

        private static int Get_ID_PRESTACION(string codigoPrestacion)
        {
            int ret = 0;

            using (var connection = new SqlConnection(Program.configuration.GetConnectionString("DataConnection")))
            {
                string sql = $"SELECT TOP 1 PRTC_ID_PRESTACION FROM PRESTACION WHERE PRTC_COD_PRESTACION = '{codigoPrestacion}'";
                ret = connection.QueryFirstOrDefault<int>(sql);
            }

            if (ret == 0) throw new Exception($"PRESTACION {codigoPrestacion} no existente!");

            return ret;
        }

        private static int Get_ID_TIP_REEMBOLSO(string tipoReembolso)
        {
            int ret = 0;

            using (var connection = new SqlConnection(Program.configuration.GetConnectionString("DataConnection")))
            {
                string sql = $"SELECT TOP 1 TPRB_ID_TIP_REEMBOLSO FROM TIPO_REEMBOLSO WHERE TPRB_DES_TIP_REEMBOLSO = '{tipoReembolso}'";
                ret = connection.QueryFirstOrDefault<int>(sql);
            }

            if (ret == 0) throw new Exception($"TIPO_REEMBOLSO {tipoReembolso} no existente!");

            return ret;
        }

        private static int Get_ID_PLAN_INFORMADO(string codigoPlan)
        {
            return 0;
        }

        private static int Get_ID_GRUPO_ING_INFORMADO(string codigoGrupo)
        {
            int ret = 0;

            using (var connection = new SqlConnection(Program.configuration.GetConnectionString("DataConnection")))
            {
                string sql = $"SELECT TOP 1 GRIN_ID_GRUPO_ING_INFORMADO FROM GRUPO_ING_INFORMADO WHERE GRIN_COD_GRUPO_ING_INFORMADO = '{codigoGrupo}'";
                ret = connection.QueryFirstOrDefault<int>(sql);
            }

            if (ret == 0) throw new Exception($"GRUPO_ING_INFORMADO {codigoGrupo} no existente!");

            return ret;
        }

        private static int Get_ID_EST_REMESA(string estadoRemesa)
        {
            int ret = 0;

            using (var connection = new SqlConnection(Program.configuration.GetConnectionString("DataConnection")))
            {
                string sql = $"SELECT TOP 1 ESRE_ID_EST_REMESA FROM ESTADO_REMESA WHERE ESRE_DES_EST_REMESA = '{estadoRemesa}'";
                ret = connection.QueryFirstOrDefault<int>(sql);
            }

            if (ret == 0) throw new Exception($"ESTADO_REMESA {estadoRemesa} no existente!");

            return ret;
        }

        private static int Get_ID_ESTADO(string estado)
        {
            int ret = 0;

            using (var connection = new SqlConnection(Program.configuration.GetConnectionString("DataConnection")))
            {
                string sql = $"SELECT TOP 1 ESDS_ID_ESTADO FROM ESTADO_DETALLE_SOLICITUD WHERE ESDS_COD_ESTADO = '{estado}'";
                ret = connection.QueryFirstOrDefault<int>(sql);
            }

            if (ret == 0) throw new Exception($"ESTADO_DETALLE_SOLICITUD {estado} no existente!");

            return ret;
        }

        private static int Get_ID_BANCO(string codigoBanco)
        {
            return 0;
        }

        private static int Get_ID_FORMA_PAGO(string formaPagoLiquidacion)
        {
            int ret = 0;

            using (var connection = new SqlConnection(Program.configuration.GetConnectionString("DataConnection")))
            {
                string sql = $"SELECT TOP 1 FMPR_ID_FORMA_PAGO FROM FORMA_PAGO_REEMBOLSO WHERE FMPR_COD_FORMA_PAGO = '{formaPagoLiquidacion}'";
                ret = connection.QueryFirstOrDefault<int>(sql);
            }

            if (ret == 0) throw new Exception($"FORMA_PAGO_REEMBOLSO {formaPagoLiquidacion} no existente!");

            return ret;
        }

        private static int Get_ID_SUCURSAL(string sucursal)
        {
            return 0;
        }

        private static int Get_ID_PROVEEDOR(string proveedor)
        {
            int ret = 0;

            using (var connection = new SqlConnection(Program.configuration.GetConnectionString("DataConnection")))
            {
                string sql = $"SELECT TOP 1 PRVD_ID_PROVEEDOR FROM PROVEEDOR WHERE PRVD_COD_PROVEEDOR = '{proveedor}'";
                ret = connection.QueryFirstOrDefault<int>(sql);
            }

            if (ret == 0) throw new Exception($"PROVEEDOR {proveedor} no existente!");

            return ret;
        }

        private static int Get_ID_LIQUIDADOR(string liquidador)
        {
            int ret = 0;

            using (var connection = new SqlConnection(Program.configuration.GetConnectionString("DataConnection")))
            {
                string sql = $"SELECT TOP 1 LIQU_ID_LIQUIDADOR FROM LIQUIDADOR WHERE LIQU_COD_LIQUIDADOR = '{liquidador}'";
                ret = connection.QueryFirstOrDefault<int>(sql);
            }

            if (ret == 0) throw new Exception($"LIQUIDADOR {liquidador} no existente!");

            return ret;
        }

        private static int Get_ID_ISAPRE(string codigoIsapre)
        {
            return 0;
        }

        private static int Get_ID_POLIZA(string poliza)
        {
            int ret = 0;

            using (var connection = new SqlConnection(Program.configuration.GetConnectionString("DataConnection")))
            {
                string sql = $"SELECT TOP 1 PLZA_ID_POLIZA FROM POLIZA WHERE PLZA_COD_POLIZA = '{poliza}'";
                ret = connection.QueryFirstOrDefault<int>(sql);
            }

            if (ret == 0) throw new Exception($"POLIZA {poliza} no existente!");

            return ret;
        }
    }
}