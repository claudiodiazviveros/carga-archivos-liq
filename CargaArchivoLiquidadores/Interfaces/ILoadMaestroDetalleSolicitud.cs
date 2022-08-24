using System;
using System.Collections.Generic;
using System.Text;

namespace CargaArchivoLiquidadores.Interfaces
{
    public interface ILoadMaestroDetalleSolicitud
    {
        int GetPrestacion(int currentRow, string[] campos);
        int GetTipoReembolso(int currentRow, string[] campos);
        int GetGrupoPrestacion(int currentRow, string[] campos);
        int GetMedicamento(int currentRow, string[] campos);
        int GetDiagnostico(int currentRow, string[] campos);
        int GetTipoDocumento(int currentRow, string[] campos);
        int GetTipoAtencion(int currentRow, string[] campos);
        int GetPlanes(int currentRow, string[] campos);
        int GetClasificacionBiomedica(int currentRow, string[] campos);
        int GetTipoDeducible(int currentRow, string[] campos);
    }
}
