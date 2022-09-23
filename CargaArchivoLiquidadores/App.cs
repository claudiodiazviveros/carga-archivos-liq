using CargaArchivoLiquidadores.Activities;
using CargaArchivoLiquidadores.Interfaces;
using System.Threading.Tasks;

namespace CargaArchivoLiquidadores
{
    public class App
    {
        private readonly IFileClasificacionBiomedica _fileClasificacionBiomedica;
        private readonly IFileMedicamentos _fileMedicamentos;
        private readonly IFileCarta _fileCarta;
        private readonly IFileLiquidacion _fileLiquidacion;
        private readonly IFileDeduPlan _fileDeduPlan;
        private readonly IFileDeduCobDet _fileDeduCobDet;
        private readonly IFileDeduCobertura _fileDeduCobertura;
        private readonly IFileDeduFamiliar _fileDeduFamiliar;

        public App(IFileClasificacionBiomedica fileClasificacionBiomedica,
            IFileMedicamentos fileMedicamentos,
            IFileCarta fileCarta,
            IFileLiquidacion fileLiquidacion,
            IFileDeduPlan fileDeduPlan,
            IFileDeduCobDet fileDeduCobDet,
            IFileDeduCobertura fileDeduCobertura,
            IFileDeduFamiliar fileDeduFamiliar)
        {
            _fileClasificacionBiomedica = fileClasificacionBiomedica;
            _fileMedicamentos = fileMedicamentos;
            _fileCarta = fileCarta;
            _fileLiquidacion = fileLiquidacion;
            _fileDeduPlan = fileDeduPlan;
            _fileDeduCobDet = fileDeduCobDet;
            _fileDeduCobertura = fileDeduCobertura;
            _fileDeduFamiliar = fileDeduFamiliar;
        }

        public Task Run()
        {
            //Log.Information("Inicia servicio");

            //1. create script 'Clasificacion Biomedica'
            _fileClasificacionBiomedica.CreateScript();

            //2. load file 'Medicamentos'
            _fileMedicamentos.CreateScript();

            //2. load file 'Liquidacion'
            _fileLiquidacion.CreateScript();

            //?. create script 'Carta Rechazo'
            _fileCarta.CreateScript();

            //?. create script 'Deducible Plan'
            _fileDeduPlan.CreateScript();

            //?. create script 'Deducible Cobertura Detalle'
            _fileDeduCobDet.CreateScript();

            //?. create script 'Deducible Familiar'
            _fileDeduFamiliar.CreateScript();

            //?. create script 'Deducible Cobertura'
            _fileDeduCobertura.CreateScript();

            return Task.CompletedTask;
        }
    }
}
