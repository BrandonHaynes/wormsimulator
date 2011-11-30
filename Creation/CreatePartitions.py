from sys import argv
import tarfile
import os
from itertools import repeat
import Network.Network
from Network.Node import Node, TabSeparatedNodeSerializer

class CreatePartitions:
    @classmethod
    def partition(cls, node, network, partitions):
        return int((float(node.address) / network.address_space) * partitions)

    @classmethod 
    def run(cls, filename, network, partitions):
        nodes = map(lambda _: [], xrange(partitions))

        #print 'Reading...'
        with open(filename) as file:
            for line in file:
                node = TabSeparatedNodeSerializer.deserialize(line)
                partition = cls.partition(node, network, partitions)
                nodes[partition].append(node) # = nodes[partition] + [node]

        #print 'Writing...'
        for partition, partition_nodes in enumerate(nodes):
            #print 'Partition #%d' % partition
            with open('part-%05d' % (partition), "w") as file:
                for node in sorted(partition_nodes, key=lambda n: n.address):
                    file.write(TabSeparatedNodeSerializer.serialize(node) + '\n')
        
        #print 'Compressing...'
        tar = tarfile.open("partitions.tar.gz", "w:gz")
        map(lambda p: tar.add('part-%05d' % (p)), xrange(partitions))
        tar.close()

        #print 'Cleaning up...'
        map(lambda p: os.remove('part-%05d' % (p)), xrange(partitions))

if __name__ == '__main__':
    CreatePartitions.run(argv[1], getattr(Network, argv[2]), int(argv[3]))
