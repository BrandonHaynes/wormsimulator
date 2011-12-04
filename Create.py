from sys import argv
import tempfile
import os
from functools import partial
from Creation.CreateHitLists import CreateHitLists
from Creation.CreateVulnerableHosts import CreateVulnerableHosts
import Network.Network 
from Network.Node import TabSeparatedNodeSerializer

class Create:
    """
    Creates a new network for propagation.

    First, creates a temporary file and emits n vulnerable nodes
    (where n is approximately the network address space * the probability of
    vulnerability)

    Then, a given number of those nodes are marked as initially-infected.
    """

    @staticmethod
    def execute(filename, network, nodes_to_infect, hit_list_size):
        """
        Create a new network with the given filename.
        filename: output filename
        network: the network address space under consideration
        nodes_to_infect: the number of nodes to mark initially-infected
        hit_list_size: the initial hit-list size for infected nodes
        """
        try:
            # Create our list of vulnerable nodes
            with tempfile.NamedTemporaryFile('w', delete=False) as file:
                for host in CreateVulnerableHosts.execute(network, 
                                                           nodes_to_infect):
                    file.write(TabSeparatedNodeSerializer.serialize(host)+'\n')

            # Then run a map/reduce job that marks some nodes as infected
            with CreateHitLists(args=['--size',str(hit_list_size), file.name])\
                    .make_runner() as runner:
                runner.run()
                with open(filename, 'w') as output:
                    map(output.write, runner.stream_output())
        finally:
            os.remove(file.name)

if __name__ == '__main__':
    if len(argv) < 3:
        print 'Usage: python Create.py output_filename network_class ' +\
                       '[num_infected] [hit_list_size]'
    else:
        filename = argv[1]
        network =  getattr(Network.Network, argv[2])
        nodes_to_infect = int(argv[3]) if len(argv) > 3 else 1
        hit_list_size = int(argv[4]) if len(argv) > 4 else 1
        
        Create.execute(filename, network, nodes_to_infect, 
                       hit_list_size)

