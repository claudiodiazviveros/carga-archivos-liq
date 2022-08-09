using CargaArchivoLiquidadores.Activities;
using System;

namespace CargaArchivoLiquidadores
{
    class Program
    {
        private static string connectionString = "Data Source=10.140.171.7;Initial catalog=OficinaVirtual;User ID=SPQPR1COFVCOFV;Password=Sura2019;";

        static void Main(string[] args)
        {
            //var task = LoadFileClasificacionBiomedica.LoadData(connectionString);
            var taskMedicamento = LoadFileMedicamentos.LoadData(connectionString);

            Console.ReadKey();
            Console.WriteLine("Hello World!");
        }
    }
}
