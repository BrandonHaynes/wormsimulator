from sys import argv
import random
from itertools import imap, chain
from functools import partial
import Network.Network
from Network.Node import Node
from Network.Node import TabSeparatedNodeSerializer
from Network.InfectionStatus import InfectionStatus


class CreateVulnerableHosts():
    """
    Class used to generate an initial set of vulnerable addresses
    Expected to be initialized with a network (to identify address space
    and expose a node factory).  Additionally, zero or more nodes will be 
    marked as being initially infected.  
    """
    @staticmethod
    def execute(network, nodes_to_infect=1):
        """ 
        Generate infected and vulnerable nodes for the given network.
        nodes_to_infect indicates the number of initially-infected nodes.
        """
        create_vulnerable_host = partial(network.create_host, 
                                         status=InfectionStatus.VULNERABLE)
        create_infected_host = partial(network.create_host, 
                                       status=InfectionStatus.INFECTED)

        # Create iterations of infected and vulnerable addresses
        # These may overlap, we'll have to deal with that during propagation
        infected_addresses = imap(
            lambda _: random.randint(0, network.address_space - 1), 
            CreateVulnerableHosts.__longrange(nodes_to_infect))
        vulnerable_addresses = imap(
            lambda _: random.randint(0, network.address_space - 1), 
            CreateVulnerableHosts.__longrange(long(
                network.address_space * network.probability_vulnerable) - 
                    nodes_to_infect))

        return chain(imap(create_infected_host, infected_addresses), 
                      imap(create_vulnerable_host, vulnerable_addresses))

    @staticmethod
    def __longrange(count):
        """ Lazy range implementation that iterates over a long value """
        while count > 0:
            yield count
            count -= 1

if __name__ == '__main__':
    """ Use a tab-separated serializer since we're not in a map/reduce job """
    Node.serializer = TabSeparatedNodeSerializer

    # Argv[1] specifies the network to use in generation (256-node, IPv4, etc)
    # Argv[2] specifies the number of initial nodes to infect
    for node in CreateVulnerableHosts.execute(
            getattr(Network, argv[1]), 
            int(argv[2]) if len(argv) > 2 else 1):
        print Node.serializer.serialize(node)

