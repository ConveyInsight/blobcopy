blobcopy
========

An simple console app which will copy blobs or containers from one azure subscription to another:

Usage: blobcopy -SourceAccountName sourceAccountName -DestinationAccountName destinationAccountName -SourceAccountKey sourceAccountKey -DestinationAccountKey destinationAccountKey -SourceContainerName sourceContainerName -DestinationContainerName destinationContainerName [-BlobName blobName] [-Async] [-Force]

blobcopy allows you to define two accounts with credentials and allows you to copy either a single blob or a container from one account to another.

If the -Force switch is used then the copier will always copy the blobs. If it left off the default behaviour is to check the MD5 hash to determin whether the blob is the same.
