using CargaArchivoLiquidadores.Interfaces;
using Dapper;
using Microsoft.Extensions.Configuration;
using Serilog;
using System;
using System.Data.SqlClient;
using System.Linq;

namespace CargaArchivoLiquidadores.Activities
{
    public class LoadMaestroSolicitud : ILoadMaestroSolicitud
    {
        private readonly IConfiguration _configuration;
        private readonly SqlConnection _sqlConnection;

        public LoadMaestroSolicitud(IConfiguration configuration)
        {
            _configuration = configuration;
            _sqlConnection = new SqlConnection(_configuration.GetConnectionString("DataConnection"));
        }

        public int GetIdCobertura(int currentRow, string[] campos)
        {
            var selectTipoCobertura = @"SELECT TOP(1) ISNULL([TPCB_ID_COBERTURA], 0) FROM [dbo].[TIPO_COBERTURA] WHERE [TPCB_DES_COBERTURA]= @tipoCobertura ";
            var coberturaID = _sqlConnection.Query<Int32>(selectTipoCobertura, new
            {
                tipoCobertura = campos[1]
            });

            if (coberturaID.FirstOrDefault() == 0)
            {
                Log.Information($"Falta la Cobertura {campos[1]}: en la línea  {currentRow}");
                return 0;
            }

            int idCobt = Convert.ToInt32(coberturaID.FirstOrDefault());
            return idCobt;
        }

        public int GetIdPoliza(int currentRow, string[] campos)
        {
            var selectQueryPoliza = $"SELECT TOP(1) ISNULL([PLZA_ID_POLIZA], 0) FROM [dbo].[POLIZA] WHERE [PLZA_COD_POLIZA]= @poliza ORDER BY PLZA_ID_POLIZA DESC";
            var polizaID = _sqlConnection.Query<Int32>(selectQueryPoliza, new
            {
                poliza = campos[0]
            });

            if (polizaID.FirstOrDefault() == 0)
            {
                Log.Information($"Falta la Poliza {campos[0]}: en la línea  {currentRow}");
                return 0;
            }

            int idPol = Convert.ToInt32(polizaID.FirstOrDefault());
            return idPol;
        }

        public int GetTipoCarga(int currentRow, string[] campos)
        {
            var selectQueryCarga = $"SELECT TOP(1) ISNULL(TPCG_ID_TIP_CARGA, 0) TPCG_ID_TIP_CARGA FROM dbo.TIPO_CARGA WHERE TPCG_DES_TIP_CARGA = @tipoCarga";
            var tipoCargaID = _sqlConnection.Query<Int32>(selectQueryCarga, new
            {
                tipoCarga = campos[24]
            });

            if (tipoCargaID.FirstOrDefault() == 0)
            {
                Log.Information($"Falta el Tipo de Carga  {campos[1]}: en la línea  {currentRow}");
                return 0;
            }

            int idTipoCarga = Convert.ToInt32(tipoCargaID.FirstOrDefault());
            return idTipoCarga;
        }

        public int GetFormaPago(int currentRow, string[] campos)
        {
            var selectFormPago = @"SELECT TOP(1) FMPR_ID_FORMA_PAGO FROM dbo.FORMA_PAGO_REEMBOLSO WHERE FMPR_COD_FORMA_PAGO = @formaPagoRembolso";
            var formPagoID = _sqlConnection.Query<Int32>(selectFormPago, new
            {
                formaPagoRembolso = campos[65]
            });

            if (formPagoID.AsList().Count == 0)
            {
                //var insertFormaPago = @"INSERT INTO FORMA_PAGO_REEMBOLSO " +
                //    " ([FMPR_COD_FORMA_PAGO], [FMPR_DES_FORMA_PAGO], [FMPR_FEC_CREACION], [FMPR_FEC_ULT_ACT], [FMPR_USU_CREACION], [FMPR_USU_ULT_ACT], [FMPR_ES_VIGENTE], [FMPR_ORIGEN_DATO])" +
                //    " VALUES (@FMPR_COD_FORMA_PAGO, @FMPR_DES_FORMA_PAGO, @FMPR_FEC_CREACION, @FMPR_FEC_ULT_ACT, @FMPR_USU_CREACION, @FMPR_USU_ULT_ACT, @FMPR_ES_VIGENTE, @FMPR_ORIGEN_DATO)" +
                //    " SELECT SCOPE_IDENTITY() AS FMPR_ID_FORMA_PAGO";

                //var resultForm = _sqlConnection.Execute(insertFormaPago, new
                //{
                //    FMPR_COD_FORMA_PAGO = campos[65],
                //    FMPR_DES_FORMA_PAGO = campos[65],
                //    FMPR_FEC_CREACION = DateTime.Now,
                //    FMPR_FEC_ULT_ACT = DateTime.Now,
                //    FMPR_USU_CREACION = "BATCH",
                //    FMPR_USU_ULT_ACT = "BATCH",
                //    FMPR_ES_VIGENTE = true,
                //    FMPR_ORIGEN_DATO = "P"
                //});
                Log.Information($"Falta la Forma de pago {campos[0]}: en la línea  {currentRow}");
                return 0;

                //Devolver Scope identity
                //int resultId = Convert.ToInt32(resultForm);
                //return resultId;
            }

            int idFormaPago = Convert.ToInt32(formPagoID.FirstOrDefault());
            return idFormaPago;
        }
        
    }
}
