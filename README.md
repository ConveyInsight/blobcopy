blobcopy
========

An simple console app which will copy blobs or containers from one azure subscription to another:

Usage: blobcopy -SourceAccountName sourceAccountName -DestinationAccountName destinationAccountName -SourceAccountKey sourceAccountKey -DestinationAccountKey destinationAccountKey -SourceContainerName sourceContainerName -DestinationContainerName destinationContainerName [-Async]

blobcopy allows you to define two accounts with credentials and allows you to copy either a single blob or a container from one account to another.
