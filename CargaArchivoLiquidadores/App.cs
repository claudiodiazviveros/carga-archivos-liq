using System.Threading.Tasks;
using Serilog;
using CargaArchivoLiquidadores.Interfaces;

namespace CargaArchivoLiquidadores
{
    public class App
    {
        private readonly ILoadFileClasificacionBiomedica _loadFileClasificacionBiomedica;
        private readonly ILoadFileMedicamentos _loadFileMedicamentos;
        private readonly ILoadFileSolicitud _loadFileSolicitud;
        private readonly ILoadFileDeduPlan _loadFileDeduPlan;
        private readonly ILoadFileDeduCobDet _loadFileDeduCobDet;
        private readonly ILoadFileDeduFamiliar _loadFileDeduFamiliar;

        public App(ILoadFileClasificacionBiomedica loadFileClasificacionBiomedica,
            ILoadFileMedicamentos loadFileMedicamentos,
            ILoadFileSolicitud loadFileSolicitud,
            ILoadFileDeduPlan loadFileDeduPlan,
            ILoadFileDeduCobDet loadFileDeduCobDet,
            ILoadFileDeduFamiliar loadFileDeduFamiliar)
        {
            _loadFileClasificacionBiomedica = loadFileClasificacionBiomedica;
            _loadFileMedicamentos = loadFileMedicamentos;
            _loadFileSolicitud = loadFileSolicitud;
            _loadFileDeduPlan = loadFileDeduPlan;
            _loadFileDeduCobDet = loadFileDeduCobDet;
            _loadFileDeduFamiliar = loadFileDeduFamiliar;
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
            _loadFileDeduPlan.SaveScript();

            //?. load file 'Deducible Cobertura Detalle'
            _loadFileDeduCobDet.LoadData();

            //?. load file 'Deducible Familiar'
            _loadFileDeduFamiliar.LoadData();

            return Task.CompletedTask;  
        }
    }
}
