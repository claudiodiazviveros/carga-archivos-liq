namespace CargaArchivoLiquidadores.Interfaces
{
    public interface ILoadMaestroSolicitud
    {
        int GetIdPoliza(int currentRow, string[] campos);
        int GetIdCobertura(int currentRow, string[] campos);
        int GetFormaPago(int currentRow, string[] campos);
        int GetTipoCarga(int currentRow, string[] campos);
    }
}
