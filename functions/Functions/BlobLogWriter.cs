using System;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Azure.Storage.Blobs;

namespace Functions
{
    public static class BlobLogWriter
    {
        public static async Task WriteTextAsync(
            string connStr,
            string containerName,
            string blobPath,
            string content
        )
        {
            var container = new BlobContainerClient(connStr, containerName);
            await container.CreateIfNotExistsAsync();
            var blob = container.GetBlobClient(blobPath);

            using var ms = new MemoryStream(Encoding.UTF8.GetBytes(content));
            await blob.UploadAsync(ms, overwrite: true);
        }
    }
}