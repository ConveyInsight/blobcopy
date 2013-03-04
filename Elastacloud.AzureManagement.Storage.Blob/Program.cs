using System;

namespace Elastacloud.AzureManagement.Storage
{
    internal class Program
    {
        private static void Main(string[] args)
        {
            var program = new Program();
            bool parse = program.ParseTokens(args);
            if (!parse)
                return;

            var sourceEndpointState = new BlobEndpointState()
            {
                AccountName = program.SourceCopyAccount,
                AccountKey = program.SourceAccountKey,
                ContainerName = program.SourceCopyContainer,
                BlobName = program.BlobName,
                Async = program.DoAsync,
                Force = program.Force
            };
            var destinationEndpointState = new BlobEndpointState()
            {
                AccountName = program.DestinationCopyAccount,
                AccountKey = program.DestinationAccountKey,
                ContainerName = program.DestinationCopyContainer,
                BlobName = program.BlobName,
                Async = program.DoAsync,
                Force = program.Force
            };

            var source = new BlobEndpoint(sourceEndpointState);
            var destination = new BlobEndpoint(destinationEndpointState);
            int timeTaken = program.BlobName != null ? source.CopyBlobTo(destination) : source.CopyAllBlobsTo(destination);

            if (program.DoAsync)
                Console.WriteLine("All blobs copied asynchronously");
            else
                Console.WriteLine("All blobs copied in {0} seconds", timeTaken);

            Console.WriteLine("Press [q] to exit");
            ConsoleKeyInfo info;
            do
            {
                info = Console.ReadKey();
            } while (info.Key != ConsoleKey.Q);
        }

        public string DestinationCopyContainer { get; set; }
        public string SourceCopyContainer { get; set; }
        public string SourceCopyAccount { get; set; }
        public string DestinationCopyAccount { get; set; }
        public string SourceAccountKey { get; set; }
        public string DestinationAccountKey { get; set; }
        public string BlobName { get; set; }
        public bool DoAsync { get; set; }
        public bool Force { get; set; }

        private bool ParseTokens(string[] args)
        {
            // TODO: this is not very defensive sanitise these arguments
            for (int i = 0; i < args.Length; i++)
            {
                switch (args[i])
                {
                    case "-SourceContainerName":
                        SourceCopyContainer = args[i + 1];
                        break;
                    case "-DestinationContainerName":
                        DestinationCopyContainer = args[i + 1];
                        break;
                    case "-SourceAccountName":
                        SourceCopyAccount = args[i + 1];
                        break;
                    case "-DestinationAccountName":
                        DestinationCopyAccount = args[i + 1];
                        break;
                    case "-SourceAccountKey":
                        SourceAccountKey = args[i + 1];
                        break;
                    case "-DestinationAccountKey":
                        DestinationAccountKey = args[i + 1];
                        break;
                    case "-BlobName":
                        BlobName = args[i + 1];
                        break;
                    case "-Async":
                        DoAsync = true;
                        break;
                }

            }

            if (SourceCopyContainer == null || DestinationCopyContainer == null || SourceCopyAccount == null ||
                DestinationCopyAccount == null || SourceAccountKey == null || DestinationAccountKey == null)
            {
                Console.WriteLine(
                    "usage: blobcopy -SourceCopyContainer SourceCopyContainer -DestinationCopyContainer DestinationCopyContainer " +
                    "-SourceCopyAccount SourceCopyAccount -DestinationCopyAccount DestinationCopyAccount -SourceAccountKey SourceAccountKey -DestinationAccountKey DestinationAccountKey " + 
                    "[-BlobName blobName] [-Async] [-Force]");
                return false;
            }
            return true;
        }

    }
}
