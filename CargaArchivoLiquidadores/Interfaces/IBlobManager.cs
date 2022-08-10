using System.Collections.Generic;
using System.Threading.Tasks;

namespace CargaArchivoLiquidadores.Interfaces
{
    public interface IBlobManager
    {
        Task FindNewFiles();
        List<string> GetBlobs(string sContainer);
    }
}
