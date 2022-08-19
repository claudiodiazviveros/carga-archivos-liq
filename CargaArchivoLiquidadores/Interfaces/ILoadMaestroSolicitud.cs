namespace CargaArchivoLiquidadores.Interfaces
{
    public interface ILoadMaestroSolicitud
    {
        int GetIdPoliza(int currentRow, string[] campos);
        int GetIdCobertura(int currentRow, string[] campos);
        int GetFormaPago(int currentRow, string[] campos);
        int GetTipoCarga(int currentRow, string[] campos);
        int GetLiquidador(int currentRow, string[] campos);
        int GetProveedor(int currentRow, string[] campos);
        int GetSucursal(int currentRow, string[] campos);
        int GetBanco(int currentRow, string[] campos);
        int GetTipoAdministracion(int currentRow, string[] campos);
        int GetEstadoSolicitudReembolso(int currentRow, string[] campos);
        int GetGrupoIngInformado(int currentRow, string[] campos);
        int GetPoolInformado(int currentRow, string[] campos);
        int GetNombrePlanInformado(int currentRow, string[] campos);
        int GetRemesa(int currentRow, string[] campos);
    }
}
