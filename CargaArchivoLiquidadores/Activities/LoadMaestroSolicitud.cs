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
                Log.Information($"Falta la Cobertura {campos[1]}: en la línea  {currentRow}, se debe agregar en la tabla maestra");
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
                Log.Information($"Falta la Poliza {campos[0]}: en la línea  {currentRow}, se debe agregar en la tabla maestra");
                return 0;
            }

            int idPol = Convert.ToInt32(polizaID.FirstOrDefault());
            return idPol;
        }

        public int GetTipoCarga(int currentRow, string[] campos)
        {
            var selectQueryCarga = $"SELECT TOP(1) ISNULL(TPCG_ID_TIP_CARGA, 0) TPCG_ID_TIP_CARGA FROM dbo.TIPO_CARGA WHERE TPCG_DES_TIP_CARGA = @tipoCarga";
            if (campos[24] == "CÓNYUGE" || campos[24] == "CONYÚGE")
            {
                campos[24] = "CONYUGE";
            }

            var tipoCargaID = _sqlConnection.Query<Int32>(selectQueryCarga, new
            {
                tipoCarga = campos[24]
            });

            if (tipoCargaID.FirstOrDefault() == 0)
            {
                Log.Information($"Falta el Tipo de Carga  {campos[24]}: en la línea  {currentRow}, se debe agregar en la tabla maestra");
                return 0;
            }

            int idTipoCarga = Convert.ToInt32(tipoCargaID.FirstOrDefault());
            return idTipoCarga;
        }

        public int GetFormaPago(int currentRow, string[] campos)
        {
            var selectFormPago = $"SELECT TOP(1) FMPR_ID_FORMA_PAGO FROM dbo.FORMA_PAGO_REEMBOLSO WHERE FMPR_COD_FORMA_PAGO = '{campos[65]}'";

            var formPagoID = _sqlConnection.Query<Int32>(selectFormPago);

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
                Log.Information($"Falta la Forma de pago {campos[65]}: en la línea  {currentRow}, se debe agregar en la tabla maestra");
                return 0;

                //Devolver Scope identity
                //int resultId = Convert.ToInt32(resultForm);
                //return resultId;
            }

            int idFormaPago = Convert.ToInt32(formPagoID.FirstOrDefault());
            return idFormaPago;
        }

        public int GetLiquidador(int currentRow, string[] campos)
        {
            var selectLiqui = $"SELECT TOP(1) LIQU_ID_LIQUIDADOR FROM dbo.LIQUIDADOR WHERE LIQU_COD_LIQUIDADOR = @liquidador";
            var liquidadorID = _sqlConnection.Query<Int32>(selectLiqui, new
            {
                liquidador = campos[87]
            });

            if (liquidadorID.FirstOrDefault() == 0)
            {
                Log.Information($"Falta el Liquidador  {campos[87]}: en la línea  {currentRow}, se debe agregar en la tabla maestra");
                return 0;
            }

            int idLiquidador = Convert.ToInt32(liquidadorID.FirstOrDefault());
            return idLiquidador;
        }

        public int GetProveedor(int currentRow, string[] campos)
        {
            var query = $"SELECT TOP(1) PRVD_ID_PROVEEDOR FROM dbo.PROVEEDOR WHERE PRVD_COD_PROVEEDOR = @proveedor";
            var proveedorID = _sqlConnection.Query<Int32>(query, new
            {
                proveedor = campos[83]
            });

            if (proveedorID.FirstOrDefault() == 0)
            {
                Log.Information($"Falta el Proveedor {campos[83]}: en la línea  {currentRow}, se debe agregar en la tabla maestra");
                return 0;
            }

            int idProveedor = Convert.ToInt32(proveedorID.FirstOrDefault());
            return idProveedor;
        }

        public int GetSucursal(int currentRow, string[] campos)
        {
            var query = $"SELECT TOP(1) SCSL_ID_SUCURSAL FROM dbo.SUCURSAL WHERE SCSL_COD_SUCURSAL = @sucursal";
            var sucursalID = _sqlConnection.Query<Int32>(query, new
            {
                sucursal = campos[12]
            });

            if (sucursalID.FirstOrDefault() == 0)
            {
                Log.Information($"Falta la Sucursal {campos[12]} - {campos[13]}: en la línea  {currentRow}, se debe agregar en la tabla maestra");
                return 0;
            }

            int idSucursal = Convert.ToInt32(sucursalID.FirstOrDefault());
            return idSucursal;
        }

        public int GetBanco(int currentRow, string[] campos)
        {
            var query = $"SELECT TOP(1) BNCO_ID_BANCO FROM dbo.BANCO WHERE BNCO_COD_BANCO = @banco";
            var resultID = _sqlConnection.Query<Int32>(query, new
            {
                banco = campos[66]
            });

            if (resultID.FirstOrDefault() == 0)
            {
                Log.Information($"Falta el Banco {campos[66]} - {campos[67]}: en la línea  {currentRow}, se debe agregar en la tabla maestra");
                return 0;
            }

            int primaryKey = Convert.ToInt32(resultID.FirstOrDefault());
            return primaryKey;
        }

        public int GetTipoAdministracion(int currentRow, string[] campos)
        {
            var query = $"SELECT TOP(1) TPAD_ID_TIP_ADMINISTRACION FROM dbo.TIPO_ADMINISTRACION  WHERE TPAD_COD_TIP_ADMINISTRACION = @tipoAdministracion";
            var resultID = _sqlConnection.Query<Int32>(query, new
            {
                tipoAdministracion = campos[85]
            });

            if (resultID.FirstOrDefault() == 0)
            {
                Log.Information($"Falta el Tipo Administración {campos[85]}: en la línea  {currentRow}, se debe agregar en la tabla maestra");
                return 0;
            }

            int primaryKey = Convert.ToInt32(resultID.FirstOrDefault());
            return primaryKey;
        }

        public int GetEstadoSolicitudReembolso(int currentRow, string[] campos)
        {
            var query = $"SELECT TOP(1) ESDS_ID_ESTADO FROM dbo.ESTADO_DETALLE_SOLICITUD WHERE ESDS_COD_ESTADO = @estado";
            var resultID = _sqlConnection.Query<Int32>(query, new
            {
                estado = campos[56]
            });

            if (resultID.FirstOrDefault() == 0)
            {
                Log.Information($"Falta el Estado Detalle Solicitud {campos[56]}: en la línea  {currentRow}, se debe agregar en la tabla maestra");
                return 0;
            }

            int primaryKey = Convert.ToInt32(resultID.FirstOrDefault());
            return primaryKey;
        }
    }
}
