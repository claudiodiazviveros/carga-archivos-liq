using Serilog;
using System.Threading.Tasks;
using CargaArchivoLiquidadores.Interfaces;

namespace CargaArchivoLiquidadores
{
    public class App
    {
        private readonly ILoadFileClasificacionBiomedica _loadFileClasificacionBiomedica;
        private readonly ILoadFileMedicamentos _loadFileMedicamentos;
        private readonly ILoadFileSolicitud _loadFileSolicitud;
        private readonly ILoadFileDeduPlan _loadFileDeduPlan;
        private readonly IBlobManager _blobManager;

        public App(ILoadFileClasificacionBiomedica loadFileClasificacionBiomedica,
            ILoadFileMedicamentos loadFileMedicamentos,
            ILoadFileSolicitud loadFileSolicitud,
            ILoadFileDeduPlan loadFileDeduPlan,
            IBlobManager blobManager)
        {
            _loadFileClasificacionBiomedica = loadFileClasificacionBiomedica;
            _loadFileMedicamentos = loadFileMedicamentos;
            _loadFileSolicitud = loadFileSolicitud;
            _loadFileDeduPlan = loadFileDeduPlan;
            _blobManager = blobManager;
        }

        public Task Run()
        {
            Log.Information("Inicia servicio");

            //1. load file 'Clasificacion Biomedica'
            _loadFileClasificacionBiomedica.LoadData();

            //2. load file 'Medicamentos'
            _loadFileMedicamentos.LoadData();

            //3. load file 'Solicitud'
            _loadFileSolicitud.LoadData();

            //?. load file 'Deducible Plan'
            _loadFileDeduPlan.LoadData();





            var manager = _blobManager.GetBlobs("");


            return Task.CompletedTask;  
        }
    }
}
