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
        /// <summary>
        /// Used toconstruct a blob endpoint
        /// </summary>
        /// <param name="state">Contains the details to derive the blob endpoint</param>
        public BlobEndpoint(BlobEndpointState state)
        {
            EndpointState = state;
        }

        /// <summary>
        /// The default blob state for the endpoint
        /// </summary>
        protected BlobEndpointState EndpointState { get; set; }

        #region Blob Copy Operations
        ///<summary> 
        /// Used to a copy a blob to a particular blob destination endpoint - this is a blocking call
        /// </summary>
        public int CopyBlobTo(BlobEndpoint destinationEndpoint)
        {
            var now = DateTime.Now;
            // get all of the details for the source blob
            var sourceBlob = GetCloudBlob(EndpointState.BlobName, this);
            var signature = sourceBlob.GetSharedAccessSignature(new SharedAccessBlobPolicy()
            {
                Permissions = SharedAccessBlobPermissions.Read,
                SharedAccessExpiryTime = DateTime.UtcNow + TimeSpan.FromMinutes(10)
            });
            // get all of the details for the destination blob
            var destinationBlob = GetCloudBlob(destinationEndpoint.EndpointState.BlobName, destinationEndpoint);
            // check whether the blob should be copied or not - if it has changed then copy
            if (!EndpointState.Force)
            {
                if (AreBlobsIdentical(EndpointState.BlobName, this, destinationEndpoint))
                    return 0;
            }
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

            if (EndpointState.Async)
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
        /// <returns>A total of the number of seconds taken</returns>
        public int CopyAllBlobsTo(BlobEndpoint destinationEndpoint)
        {
            var container = GetCloudBlobContainer(this);
            var items = container.ListBlobs(useFlatBlobListing: true);
            return items.Sum(item =>
                                 {
                                     destinationEndpoint.EndpointState.BlobName = ((CloudBlockBlob) item).Name;
                                     return CopyBlobTo(destinationEndpoint);
                                 });
        }

        #endregion

        # region Helper Methods

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
        /// Used to pull back the cloud blob that should be copied from or to
        /// </summary>
        private ICloudBlob GetCloudBlob(string blobName, BlobEndpoint endpoint)
        {
            var containerRef = GetCloudBlobContainer(endpoint);
            return containerRef.GetBlockBlobReference(blobName);
        }

        /// <summary>
        /// Used to determine whether the blobs are the same or not before copying
        /// </summary>
        /// <param name="blobName">The name of the blob to check in both container</param>
        /// <param name="sourceEndpoint">the endpoint of the source blob</param>
        /// <param name="destinationEndpoint">the endpoint of the destination blob</param>
        /// <returns>Checks whether the blobs are identical</returns>
        private bool AreBlobsIdentical(string blobName, BlobEndpoint sourceEndpoint, BlobEndpoint destinationEndpoint)
        {
            bool exists = sourceEndpoint.BlobExists(blobName) && destinationEndpoint.BlobExists(blobName);
            if (!exists)
                return false;
            var sourceBlob = GetCloudBlob(blobName, sourceEndpoint);
            var destinationBlob = GetCloudBlob(blobName, destinationEndpoint);
            sourceBlob.FetchAttributes();
            destinationBlob.FetchAttributes();

            if (sourceBlob.Properties.ContentMD5 != destinationBlob.Properties.ContentMD5)
            {
                return false;
            }
            return true;
        }

        /// <summary>
        /// Used to get the blob client 
        /// </summary>
        /// <param name="endpoint">the blob endpoint</param>
        /// <returns>A CloudBlobClient instance</returns>
        private CloudBlobContainer GetCloudBlobContainer(BlobEndpoint endpoint)
        {
            string blobClientConnectString = String.Format("http://{0}.blob.core.windows.net", endpoint.EndpointState.AccountName);
            CloudBlobClient blobClient = null;
            if (endpoint.EndpointState.AccountKey == null)
                blobClient = new CloudBlobClient(new Uri(blobClientConnectString));
            else
            {
                var account = new CloudStorageAccount(new StorageCredentials(endpoint.EndpointState.AccountName, endpoint.EndpointState.AccountKey), false);
                blobClient = account.CreateCloudBlobClient();
            }
            var containerRef = blobClient.GetContainerReference(endpoint.EndpointState.ContainerName);
            containerRef.CreateIfNotExists();
            return containerRef;
        }
        #endregion
    }

}
