using System;
using System.Linq;
using System.Net;
using System.Threading;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Blob;

namespace Elastacloud.AzureManagement.Storage
{

    ///<summary> 
    /// Used to define the properties of a blob which should be copied to or from
    /// </summary>
    public class BlobEndpoint
    {
        ///<summary> 
        /// The storage account name
        /// </summary>
        private readonly string _storageAccountName = null;
        ///<summary> 
        /// The container name 
        /// </summary>
        private readonly string _containerName = null;
        ///<summary> 
        /// The storage key which is used to
        /// </summary>
        private readonly string _storageKey = null;
        /// <summary> 
        /// Used to construct a blob endpoint
        /// </summary>
        public BlobEndpoint(string storageAccountName, string containerName = null, string storageKey = null)
        {
            _storageAccountName = storageAccountName;
            _containerName = containerName;
            _storageKey = storageKey;
        }

        ///<summary> 
        /// Used to a copy a blob to a particular blob destination endpoint - this is a blocking call
        /// </summary>
        public int CopyBlobTo(string blobName, BlobEndpoint destinationEndpoint, bool async = false)
        {
            var now = DateTime.Now;
            // get all of the details for the source blob
            var sourceBlob = GetCloudBlob(blobName, this);
            var signature = sourceBlob.GetSharedAccessSignature(new SharedAccessBlobPolicy()
            {
                Permissions = SharedAccessBlobPermissions.Read,
                SharedAccessExpiryTime = DateTime.UtcNow + TimeSpan.FromMinutes(10)
            });
            // get all of the details for the destination blob
            var destinationBlob = GetCloudBlob(blobName, destinationEndpoint);
            // copy from the destination blob pulling the blob
            try
            {
                destinationBlob.StartCopyFromBlob(new Uri(sourceBlob.Uri.AbsoluteUri + signature));
            }
            catch (Exception ex)
            {
                var we = ex.InnerException as WebException;
                if (we != null && we.Status == WebExceptionStatus.ProtocolError)
                {
                    // TODO: replace this with a tracelistener
                    Console.WriteLine("conflict with blob copy for blob {0} - you currently have a pending blob already waiting to be copied", sourceBlob.Uri.AbsoluteUri);
                    return 0;
                }
            }
            // make this call block so that we can check the time it takes to pull back the blob
            // this is a regional copy should be very quick even though it's queued but still make this defensive
            const int seconds = 1800;
            int count = 0;

            if (async)
                return 0;

            while ((count * 10) < seconds)
            {
                // if we succeed we want to drop out this straight away
                if (destinationBlob.CopyState.Status == CopyStatus.Success)
                    break;
                Thread.Sleep(100);
                count++;
            }
            //calculate the time taken and return
            return (int)DateTime.Now.Subtract(now).TotalSeconds;
        }

        /// <summary>
        /// All the blobs are being copied from endpoint to another
        /// </summary>
        /// <param name="destinationEndpoint">the destination endpoint to copy to</param>
        /// <param name="async">Doing this asynchronously</param>
        /// <returns>A total of the number of seconds taken</returns>
        public int CopyAllBlobsTo(BlobEndpoint destinationEndpoint, bool async = false)
        {
            var container = GetCloudBlobContainer(this);
            var items = container.ListBlobs(useFlatBlobListing: true);
            return items.Sum(item => CopyBlobTo(((CloudBlockBlob)item).Name, destinationEndpoint, async));
        }

        ///<summary> 
        /// Used to determine whether the blob exists or not
        /// </summary>
        public bool BlobExists(string blobName)
        {
            // get the cloud blob
            var cloudBlob = GetCloudBlob(blobName, this);
            try
            {
                // this is the only way to test
                cloudBlob.FetchAttributes();
            }
            catch (Exception)
            {
                // we should check for a variant of this exception but chances are it will be okay otherwise - that's defensive programming for you!
                return false;
            }
            return true;
        }

        ///<summary> 
        /// The storage account name
        /// </summary>
        public string StorageAccountName
        {
            get { return _storageAccountName; }
        }

        ///<summary> 
        /// The name of the container the blob is in
        /// </summary>
        public string ContainerName
        {
            get { return _containerName; }
        }

        ///<summary> 
        /// The key used to access the storage account
        /// </summary>

        public string StorageKey
        {
            get { return _storageKey; }
        }

        ///<summary> 
        /// Used to pull back the cloud blob that should be copied from or to
        /// </summary>
        private ICloudBlob GetCloudBlob(string blobName, BlobEndpoint endpoint)
        {
            var containerRef = GetCloudBlobContainer(endpoint);
            return containerRef.GetBlockBlobReference(blobName);
        }

        /// <summary>
        /// Used to get the blob client 
        /// </summary>
        /// <param name="endpoint">the blob endpoint</param>
        /// <returns>A CloudBlobClient instance</returns>
        private CloudBlobContainer GetCloudBlobContainer(BlobEndpoint endpoint)
        {
            string blobClientConnectString = String.Format("http://{0}.blob.core.windows.net", endpoint.StorageAccountName);
            CloudBlobClient blobClient = null;
            if (endpoint.StorageKey == null)
                blobClient = new CloudBlobClient(new Uri(blobClientConnectString));
            else
            {
                var account = new CloudStorageAccount(new StorageCredentials(endpoint.StorageAccountName, endpoint.StorageKey), false);
                blobClient = account.CreateCloudBlobClient();
            }
            var containerRef = blobClient.GetContainerReference(endpoint.ContainerName);
            containerRef.CreateIfNotExists();
            return containerRef;
        }
    }

}
