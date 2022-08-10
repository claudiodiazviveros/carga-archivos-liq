using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using CargaArchivoLiquidadores.Interfaces;
using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace CargaArchivoLiquidadores.Services
{
    public class BlobManager : IBlobManager
    {
        private readonly IConfiguration _configuration;

        public BlobManager(IConfiguration configuration)
        {
            _configuration = configuration;
        }

        public async Task FindNewFiles()
        {
            // Create a BlobServiceClient that will authenticate through Active Directory
            Uri accountUri = new Uri("https://sasegurossurafilesdev.blob.core.windows.net/cargaliquidadoresvida");
            BlobServiceClient client = new BlobServiceClient(accountUri);

            var folders = GetBlobs();


            await Task.FromResult(0);
        }

        public List<string> GetBlobs(string sContainer = "cargaliquidadoresvida")
        {
            var folders = new List<string>();

            string connectionString = _configuration.GetConnectionString("StorageConnectionString");
            string containerName = _configuration["Storage:ContainerName"];

            BlobContainerClient container = new BlobContainerClient(connectionString, containerName);
            foreach (BlobItem blob in container.GetBlobs())
            {
                folders.Add(blob.Name);
            }
            return folders;
        }
    }
}
