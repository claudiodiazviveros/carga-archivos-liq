using CargaArchivoLiquidadores.Interfaces;
using Dapper;
using Microsoft.Extensions.Configuration;
using Serilog;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;

namespace CargaArchivoLiquidadores.Activities
{
    public class LoadMaestroDetalleSolicitud : ILoadMaestroDetalleSolicitud
    {
        private readonly IConfiguration _configuration;
        private readonly SqlConnection _sqlConnection;

        public LoadMaestroDetalleSolicitud(IConfiguration configuration)
        {
            _configuration = configuration;
            _sqlConnection = new SqlConnection(_configuration.GetConnectionString("DataConnection"));
        }

        public int GetPrestacion(int currentRow, string[] campos)
        {
            var query = $"SELECT TOP(1) PRTC_ID_PRESTACION FROM PRESTACION WHERE PRTC_COD_PRESTACION = @prestacion ";
            var resultId = _sqlConnection.Query<Int32>(query, new
            {
                prestacion = campos[61].Trim()
            });

            if (resultId.FirstOrDefault() == 0)
            {
                Log.Information($"Falta la Prestación {campos[61]}: en la línea  {currentRow}, se debe agregar en la tabla maestra");
                return 0;
            }

            int idCobt = Convert.ToInt32(resultId.FirstOrDefault());
            return idCobt;
        }


        public int GetTipoReembolso(int currentRow, string[] campos)
        {
            var query = $"SELECT TOP(1) TPRB_ID_TIP_REEMBOLSO FROM dbo.TIPO_REEMBOLSO WHERE TPRB_DES_TIP_REEMBOLSO = @tipoReembolso ";
            var resultId = _sqlConnection.Query<Int32>(query, new
            {
                tipoReembolso = campos[81].Trim()
            });

            if (resultId.FirstOrDefault() == 0)
            {
                Log.Information($"Falta el Tipo Reembolso {campos[81]}: en la línea  {currentRow}, se debe agregar en la tabla maestra");
                return 0;
            }

            int result = Convert.ToInt32(resultId.FirstOrDefault());
            return result;
        }


        public int GetGrupoPrestacion(int currentRow, string[] campos)
        {
            var query = $"SELECT TOP(1) GRPR_ID_GRUPO FROM dbo.GRUPO_PRESTACION WHERE GRPR_COD_GRUPO = @grupo ";
            var resultId = _sqlConnection.Query<Int32>(query, new
            {
                grupo = campos[41].Trim()
            });

            if (resultId.FirstOrDefault() == 0)
            {
                Log.Information($"Falta el Grupo Prestación {campos[41]}: en la línea  {currentRow}, se debe agregar en la tabla maestra");
                return 0;
            }

            int result = Convert.ToInt32(resultId.FirstOrDefault());
            return result;
        }

        public int GetMedicamento(int currentRow, string[] campos)
        {
            var query = $"SELECT TOP(1) MDTO_ID_MEDICAMENTO FROM dbo.MEDICAMENTO WHERE MDTO_COD_MEDICAMENTO = @medicamento ";
            var resultId = _sqlConnection.Query<Int32>(query, new
            {
                medicamento = campos[88].Trim()
            });

            if (resultId.FirstOrDefault() == 0)
            {
                Log.Information($"Falta el Medicamento {campos[88]}: en la línea  {currentRow}, se debe agregar en la tabla maestra");
                return 0;
            }

            int result = Convert.ToInt32(resultId.FirstOrDefault());
            return result;
        }

        public int GetDiagnostico(int currentRow, string[] campos)
        {
            var query = $"SELECT TOP(1) DIAG_ID_DIAGNOSTICO FROM dbo.DIAGNOSTICO WHERE DIAG_COD_DIAGNOSTICO = @diagnostico ";
            var resultId = _sqlConnection.Query<Int32>(query, new
            {
                diagnostico = campos[29].Trim()
            });

            if (resultId.FirstOrDefault() == 0)
            {
                Log.Information($"Falta el Diagnostico {campos[29]}: en la línea  {currentRow}, se debe agregar en la tabla maestra");
                return 0;
            }

            int result = Convert.ToInt32(resultId.FirstOrDefault());
            return result;
        }


        public int GetTipoDocumento(int currentRow, string[] campos)
        {
            var query = $"SELECT TOP(1) TPDC_ID_TIP_DOCUMENTO FROM dbo.TIPO_DOCUMENTO WHERE TPDC_COD_TIP_DOCUMENTO = @tipoDocumento ";
            var resultId = _sqlConnection.Query<Int32>(query, new
            {
                tipoDocumento = campos[33].Trim()
            });

            if (resultId.FirstOrDefault() == 0)
            {
                Log.Information($"Falta el Tipo Documento {campos[33]}: en la línea  {currentRow}, se debe agregar en la tabla maestra");
                return 0;
            }

            int result = Convert.ToInt32(resultId.FirstOrDefault());
            return result;
        }

        public int GetTipoAtencion(int currentRow, string[] campos)
        {
            var query = $"SELECT TOP(1) TPAT_ID_TIP_ATENCION FROM dbo.TIPO_ATENCION WHERE TPAT_COD_TIP_ATENCION = @tipoAtencion ";
            var resultId = _sqlConnection.Query<Int32>(query, new
            {
                tipoAtencion = campos[35].Trim()
            });

            if (resultId.FirstOrDefault() == 0)
            {
                Log.Information($"Falta el Tipo Atención {campos[35]}: en la línea  {currentRow}, se debe agregar en la tabla maestra");
                return 0;
            }

            int result = Convert.ToInt32(resultId.FirstOrDefault());
            return result;
        }

        public int GetPlanes(int currentRow, string[] campos)
        {
            var query = $"SELECT TOP(1) PLAN_ID FROM PLANES WHERE PLAN_CODIGO_PLAN = @plan ";
            var resultId = _sqlConnection.Query<Int32>(query, new
            {
                plan = campos[76].Trim()
            });

            if (resultId.FirstOrDefault() == 0)
            {
                Log.Information($"Falta el Plan {campos[76]}: en la línea  {currentRow}, se debe agregar en la tabla maestra");
                return 0;
            }

            int result = Convert.ToInt32(resultId.FirstOrDefault());
            return result;
        }

        public int GetClasificacionBiomedica(int currentRow, string[] campos)
        {
            var query = $"SELECT TOP(1) CLBI_ID_CLASIFICACION FROM CLASIFICACION_BIOMEDICA WHERE CLBI_COD_CLASIFICACION = @biomedico ";
            var biomed = campos[89].Trim();

            if (biomed == "")
                biomed = "0";

            var resultId = _sqlConnection.Query<Int32>(query, new
            {
                biomedico = biomed
            });

            if (resultId.FirstOrDefault() == -1)
            {
                Log.Information($"Falta la Clasificación Biomédica {campos[89]}: en la línea  {currentRow}, se debe agregar en la tabla maestra");
                return 0;
            }

            int result = Convert.ToInt32(resultId.FirstOrDefault());
            return result;
        }

        public int GetTipoDeducible(int currentRow, string[] campos)
        {
            var query = $"SELECT TOP(1) TDED_ID_TIPO_DEDUCIBLE FROM TIPO_DEDUCIBLE WHERE TDED_COD_TIPO_DEDUCIBLE = @tipoDeducible ";
            var resultId = _sqlConnection.Query<Int32>(query, new
            {
                tipoDeducible = campos[94].Trim()
            });

            if (resultId.FirstOrDefault() == -1)
            {
                Log.Information($"Falta el Tipo Deducible  {campos[94]}: en la línea  {currentRow}, se debe agregar en la tabla maestra");
                return 0;
            }

            int result = Convert.ToInt32(resultId.FirstOrDefault());
            return result;
        }
    }
}
