from sys import argv
import tempfile
import os
from functools import partial
from Creation.CreateHitLists import CreateHitLists
from Creation.CreateVulnerableHosts import CreateVulnerableHosts
from Utilities.Partitions import Partitions
import Network.Network 
from Network.Node import TabSeparatedNodeSerializer

class Create:
    @staticmethod
    def _partition(address, network, partitions):
        return int((float(address) / network.address_space) * partitions)

    @staticmethod
    def execute(filename, network, nodes_to_infect, hit_list_size, partitions):
        try:
            with tempfile.NamedTemporaryFile('w', delete=False) as file:
                for host in CreateVulnerableHosts.execute(network, nodes_to_infect):
                    file.write(TabSeparatedNodeSerializer.serialize(host) + '\n')

            with CreateHitLists(args=['--size', str(hit_list_size), file.name]).make_runner() as runner:
                runner.run()
                with open(filename, 'w') as output:
                    map(output.write, runner.stream_output())
        finally:
            os.remove(file.name)

        Partitions.create(filename, network, partitions, 
                          partial(Create._partition, 
                                  network=network, 
                                  partitions=partitions))
            
if __name__ == '__main__':
    if len(argv) < 4:
        print 'Usage: python Create.py output_filename network_class num_partitions [num_infected] [hit_list_size]'
    else:
        filename = argv[1]
        network =  getattr(Network.Network, argv[2])
        partitions = int(argv[3])
        nodes_to_infect = int(argv[4]) if len(argv) > 4 else 1
        hit_list_size = int(argv[5]) if len(argv) > 5 else 1
        
        Create.execute(filename, network, nodes_to_infect, 
                       hit_list_size, partitions)

