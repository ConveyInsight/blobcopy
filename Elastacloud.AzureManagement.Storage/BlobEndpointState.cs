using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Elastacloud.AzureManagement.Storage
{
    /// <summary>
    /// Used to contain the state of the BlobEndpoint
    /// </summary>
    public class BlobEndpointState
    {
        /// <summary>
        /// The storage account name of the blob endpoint
        /// </summary>
        public string AccountName { get; set; }
        /// <summary>
        /// The storage account key of the blob endpoint
        /// </summary>
        public string AccountKey { get; set; }
        /// <summary>
        /// The container name of the blob endpoint
        /// </summary>
        public string ContainerName { get; set; }
        /// <summary>
        /// The name of the blob to copy if there is on
        /// </summary>
        public string BlobName { get; set; }
        /// <summary>
        /// Whether to do this asynchronously or not
        /// </summary>
        public bool Async { get; set; }
        /// <summary>
        /// To force a copy even if the blobs are the same on either endpoint
        /// </summary>
        public bool Force { get; set; }
    }
}
