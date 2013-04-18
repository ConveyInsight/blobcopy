using System;
using System.Diagnostics;
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
        private string _endpoint;

        /// <summary>
        /// Used toconstruct a blob endpoint
        /// </summary>
        /// <param name="state">Contains the details to derive the blob endpoint</param>
        public BlobEndpoint(BlobEndpointState state)
        {
            EndpointState = state;
        }

        public BlobEndpoint(string httpEndpoint)
        {
            _endpoint = httpEndpoint;
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
            if (_endpoint == null)
            {
                var sourceBlob = GetCloudBlob(this);
                var signature = sourceBlob.GetSharedAccessSignature(new SharedAccessBlobPolicy()
                                                                        {
                                                                            Permissions =
                                                                                SharedAccessBlobPermissions.Read,
                                                                            SharedAccessExpiryTime =
                                                                                DateTime.UtcNow +
                                                                                TimeSpan.FromMinutes(10)
                                                                        });
                _endpoint = sourceBlob.Uri.AbsoluteUri + signature;
            }
            // get all of the details for the destination blob
            var destinationBlob = GetCloudBlob(destinationEndpoint);
            // check whether the blob should be copied or not - if it has changed then copy
            if (_endpoint == null && !EndpointState.Force)
            {
                if (AreBlobsIdentical(destinationEndpoint))
                {
                    System.Diagnostics.Trace.TraceWarning("Skipping: {0}", _endpoint);
                    return 0;
                }
            }
            // copy from the destination blob pulling the blob
            try
            {
                destinationBlob.StartCopyFromBlob(new Uri(_endpoint));
                System.Diagnostics.Trace.TraceInformation("Copying: {0}", destinationEndpoint.EndpointState.BlobName);
            }
            catch (Exception ex)
            {
                var we = ex.InnerException as WebException;
                if (we != null && we.Status == WebExceptionStatus.ProtocolError)
                {
                    // TODO: replace this with a tracelistener
                    System.Diagnostics.Trace.TraceError("conflict with blob copy for blob {0} - you currently have a pending blob already waiting to be copied", _endpoint);
                    return 0;
                }
            }
            // make this call block so that we can check the time it takes to pull back the blob
            // this is a regional copy should be very quick even though it's queued but still make this defensive
            const int seconds = 18000;
            int count = 0;

            if (EndpointState != null && EndpointState.Async)
                return 0;

            while ((count * 10) < seconds)
            {
                var properties = destinationBlob.Properties;
                destinationBlob.FetchAttributes();
                // if we succeed we want to drop out this straight away
                if (destinationBlob.CopyState.Status == CopyStatus.Success)
                    break;
                // if this is pending we would want to log and continue waiting
                if (destinationBlob.CopyState.Status == CopyStatus.Pending)
                {
                    Trace.TraceInformation("Copy of {0} is pending", destinationBlob.Name);
                    Thread.Sleep(500);
                }
                // otherwise let's log how many bytes we've copied across in percent
                float percentComplete = ((float)destinationBlob.CopyState.BytesCopied/(float)destinationBlob.CopyState.TotalBytes);
                Trace.TraceInformation("{0:P1} copied of {1}", percentComplete, destinationBlob.Name);
                count++;
            }
            //calculate the time taken and return
            //return (int)DateTime.Now.Subtract(now).TotalSeconds;
            return (int)destinationBlob.CopyState.CompletionTime.Value.DateTime.Subtract(now).TotalSeconds;
        }

        /// <summary>
        /// All the blobs are being copied from endpoint to another
        /// </summary>
        /// <param name="destinationEndpoint">the destination endpoint to copy to</param>
        /// <returns>A total of the number of seconds taken</returns>
        public int CopyAllBlobsTo(BlobEndpoint destinationEndpoint)
        {
            var container = GetCloudBlobContainer(this);
            if (container == null)
                return CopyBlobTo(destinationEndpoint);

            var items = container.ListBlobs(useFlatBlobListing: true);
            return items.Sum(item =>
                                 {
                                     EndpointState.BlobName = destinationEndpoint.EndpointState.BlobName = ((CloudBlockBlob) item).Name;
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
            var cloudBlob = GetCloudBlob(this);
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
        private ICloudBlob GetCloudBlob(BlobEndpoint endpoint)
        {
            if (endpoint.EndpointState.BlobName == null)
            {
                throw new ApplicationException("unknown blob name - please ensure this value is set");    
            }

            var containerRef = GetCloudBlobContainer(endpoint);
            return containerRef.GetBlockBlobReference(endpoint.EndpointState.BlobName);
        }

        /// <summary>
        /// Used to determine whether the blobs are the same or not before copying
        /// </summary>
        /// <param name="sourceEndpoint">the endpoint of the source blob</param>
        /// <param name="destinationEndpoint">the endpoint of the destination blob</param>
        /// <returns>Checks whether the blobs are identical</returns>
        private bool AreBlobsIdentical(BlobEndpoint sourceEndpoint, BlobEndpoint destinationEndpoint)
        {
            bool exists = sourceEndpoint.BlobExists(sourceEndpoint.EndpointState.BlobName) 
                && destinationEndpoint.BlobExists(destinationEndpoint.EndpointState.BlobName);
            if (!exists)
                return false;
            var sourceBlob = GetCloudBlob(sourceEndpoint);
            var destinationBlob = GetCloudBlob(destinationEndpoint);
            //need to fetch the attributes to get the md5 content tags
            sourceBlob.FetchAttributes();
            destinationBlob.FetchAttributes();
            // check to see whether the md5 content tags are the same
            return sourceBlob.Properties.ContentMD5 == destinationBlob.Properties.ContentMD5  &&
                sourceBlob.Properties.Length == destinationBlob.Properties.Length;
        }

        /// <summary>
        /// Shortcut for the source blob to check whether the blob can copy 
        /// </summary>
        /// <param name="destinationBlobEndpoint">the destination endpoint</param>
        /// <returns>True if the blobs are identical</returns>
        private bool AreBlobsIdentical(BlobEndpoint destinationBlobEndpoint)
        {
            return AreBlobsIdentical(this, destinationBlobEndpoint);
        }

        /// <summary>
        /// Used to get the blob client 
        /// </summary>
        /// <param name="endpoint">the blob endpoint</param>
        /// <returns>A CloudBlobClient instance</returns>
        private CloudBlobContainer GetCloudBlobContainer(BlobEndpoint endpoint)
        {
            if (endpoint._endpoint != null)
                return null;
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
