from sys import argv
import tempfile
import os
from Creation.CreateHitLists import CreateHitLists
from Creation.CreateVulnerableHosts import CreateVulnerableHosts
from Creation.CreatePartitions import CreatePartitions
import Network.Network 
from Network.Node import TabSeparatedNodeSerializer

class Create:
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

            CreatePartitions.run(filename, network, partitions)
            
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

