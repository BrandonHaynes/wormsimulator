from sys import argv
import tarfile
import os
import tempfile
from itertools import repeat
import Network.Network
from Network.Node import Node, TabSeparatedNodeSerializer

class Partitions:
    @staticmethod 
    def create(filename, network, partitions, partitioner, fail_silently=False, tar_filename=None):
        make_filename = lambda partition: 'part-%05d' % partition
        make_path = lambda partition: '%s/%s' % (directory, make_filename(partition))
        nodes = map(lambda _: [], xrange(partitions))
        stream = None

        #print 'Reading...'
        try:
            stream = open(filename)
        except IOError:
            if not fail_silently: raise
            else: return None
            
        try:
            for line in stream:
                node = TabSeparatedNodeSerializer.deserialize(line)
                partition = partitioner(node.address)
                nodes[partition].append(node)
        finally:
            if stream: stream.close()

        #print 'Writing...'
        directory = tempfile.mkdtemp()

        for partition, partition_nodes in enumerate(nodes):
            #print 'Partition #%d' % partition
            stream = open(make_path(partition), "w")
            try:
                for node in sorted(partition_nodes, key=lambda n: n.address):
                    stream.write(TabSeparatedNodeSerializer.serialize(node) + '\n')
            finally:
                if stream: stream.close()
        
        #print 'Compressing...'
        tar_filename = tar_filename or tempfile.mktemp(prefix='partition', suffix='.tar.gz')
        tar = tarfile.open(tar_filename, "w:gz")
        try: map(lambda p: tar.add(make_path(p), make_filename(p)), xrange(partitions))
        finally: tar.close()

        #print 'Cleaning up...'
        map(lambda p: os.remove(make_path(p)), xrange(partitions))

        return tar_filename

if __name__ == '__main__':
    CreatePartitions.create(argv[1], getattr(Network, argv[2]), int(argv[3]))
