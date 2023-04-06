using System;
using System.IO;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Core.Exceptions;

class Program
{
    static void Main(string[] args)
    {
        // Set the connection string and create a BlobServiceClient object
        string connectionString = "<storage connection string>";
        BlobServiceClient blobServiceClient = new BlobServiceClient(connectionString);

        // Create a container
        string containerName = "<container id>";
        BlobContainerClient containerClient;
        try
        {
            containerClient = blobServiceClient.CreateBlobContainer(containerName);
        }
        catch (RequestFailedException ex) when (ex.ErrorCode == BlobErrorCode.ContainerAlreadyExists)
        {
            containerClient = blobServiceClient.GetBlobContainerClient(containerName);
        }
        Console.WriteLine(containerClient.Uri);

        // List containers
        foreach (BlobContainerItem container in blobServiceClient.GetBlobContainers())
        {
            Console.WriteLine(container.Name);
            Console.WriteLine(container.Properties.Metadata);
        }

        // Upload a blob to a container
        string blobRootDirectory = "dbt";
        string workingDir = Directory.GetCurrentDirectory();
        foreach (string folder in Directory.GetDirectories(Path.Combine(workingDir, "dbt"), "*", SearchOption.AllDirectories))
        {
            foreach (string file in Directory.GetFiles(folder))
            {
                try
                {
                    string filePath = Path.Combine(folder, file);
                    string blobPath = $"{blobRootDirectory}{filePath.Replace(@"C:\Users\Ready_(Azure Storage) Blob Management\dbt", string.Empty)}";
                    BlobClient blobClient = containerClient.GetBlobClient(blobPath);
                    using (FileStream fileStream = new FileStream(filePath, FileMode.Open))
                    {
                        blobClient.Upload(fileStream, true);
                    }
                }
                catch (RequestFailedException ex) when (ex.ErrorCode == BlobErrorCode.BlobAlreadyExists)
                {
                    Console.WriteLine($"Blob (file object) {file} already exists.");
                }
                catch (Exception ex)
                {
                    throw new Exception(ex.Message);
                }
            }
        }

        // List blobs (file objects) in a given container
        foreach (BlobItem blob in containerClient.GetBlobs())
        {
            BlobProperties properties = blob.Properties;
            Console.WriteLine(blob.Name);
            Console.WriteLine(blob.ContainerName);
            Console.WriteLine(properties.Snapshot);
            Console.WriteLine(properties.VersionId);
            Console.WriteLine(properties.IsCurrentVersion);
            Console.WriteLine(properties.BlobType);
            Console.WriteLine(properties.AccessTier);
            Console.WriteLine(properties.Metadata);
            Console.WriteLine(properties.CreatedOn);
            Console.WriteLine(properties.LastModified);
            Console.WriteLine(properties.LastAccessed);
            Console.WriteLine(properties.ContentLength);
            Console.WriteLine(properties.DeletedTime);
            Console.WriteLine(properties.Tags);
        }

        // Download a blob
        string fileObjectPath = "dbt/2. Build dbt projects/1. Build your DAG/Exposures dbt Developer Hub.pdf";
        string fileDownloaded = Path.Combine(workingDir, "Exposures dbt Developer Hub.pdf");
        BlobDownloadInfo blobDownloadInfo = containerClient.GetBlobClient(fileObjectPath).Download();
        using (FileStream downloadFileStream = new FileStream(fileDownloaded, FileMode.Create))
        {
            blobDownloadInfo.Content.CopyTo(downloadFileStream);
        }
    }
}
