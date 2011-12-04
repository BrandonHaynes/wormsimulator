from sys import argv
import tarfile
import os
import tempfile
from itertools import repeat
from Network.Node import TabSeparatedNodeSerializer

class Partitions:
    """
    Utility class that accepts a (potentially large) input file and emits
    n partitions that contain the sorted results based upon a given partition
    function.
    """
    @staticmethod 
    def create(filename, partitions, partitioner, 
                          fail_silently=False, tar_filename=None):
        """
        Create a new partition.
        filename: input filename
        partitions: the number of partitions to create
        partitioner: a function from key to partition number
        fail_silently: when set, will swallow any exception encountered when 
                       opening the input file
        tar_filename: The filename used to create the partition archive
        """
        make_filename = lambda partition: 'part-%05d' % partition
        make_path = lambda partition: '%s/%s' % (directory, 
                                                  make_filename(partition))
        nodes = map(lambda _: [], xrange(partitions))
        stream = None

        # Open the file, swallow if flag set
        try:
            stream = open(filename)
        except IOError:
            if not fail_silently: raise
            else: return None
            
        # For each line in the file, save it to its proper partition
        try:
            for line in stream:
                node = TabSeparatedNodeSerializer.deserialize(line)
                partition = partitioner(node.address)
                nodes[partition].append(node)
        finally:
            if stream: stream.close()

        # Create a temporary file
        directory = tempfile.mkdtemp()

        # For each partition, write its dataset
        for partition, partition_nodes in enumerate(nodes):
            stream = open(make_path(partition), "w")
            try:
                for node in sorted(partition_nodes, key=lambda n: n.address):
                    stream.write(\
                        TabSeparatedNodeSerializer.serialize(node) + '\n')
            finally:
                if stream: stream.close()
        
        # Compress the resulting partitions
        tar_filename = tar_filename or tempfile.mktemp(prefix='partition', 
                                                       suffix='.tar.gz')
        tar = tarfile.open(tar_filename, "w:gz")
        try: map(lambda p: tar.add(make_path(p), make_filename(p)), 
                  xrange(partitions))
        finally: tar.close()

        # Remove the scratch files
        map(lambda p: os.remove(make_path(p)), xrange(partitions))

        return tar_filename

if __name__ == '__main__':
    CreatePartitions.create(argv[1], int(argv[2]))
