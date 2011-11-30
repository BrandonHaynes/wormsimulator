import threading
from sys import argv
from itertools import imap
from itertools import islice
from functools import partial
import Network
from Network import IPv4, IPv6, Network256, NetworkGraphable
from Node import Node
from Node import TabSeparatedNodeSerializer
from InfectionStatus import InfectionStatus

def foreach(f, iterable):
    any(imap(f, iterable))

class CreateVulnerableHosts():
    """
    Class used to generate an initial set of vulnerable addresses
    Expected to be initialized with a network (to identify address space
    and expose a node factory).  Additionally, zero or more nodes will be 
    marked as being initially infected.  

    By default the class operates on a single thread, but optionally multiple
    threads may be used to decrease processing time.

    Note that since this approachis O(#addresses), it is not practical
    for a very large address space (e.g. IPv6).
    """
    # Lock output so that threads don't step on each other
    lock = threading.Lock()

    def __init__(self, network=IPv4):
        self.network = network

    def execute(self, threads=1, nodes_to_infect=1):
        """ 
        Execute network generation over the network address space.
        This search will be partitioned across a configurable number of threads.
        Additionally, zero or more nodes will be marked as infected.
        These nodes are at the beginning of the address space, which is 
        a little lame, but probably okay.
        """
        # Total number of addresses this thread is responsible for
        search_space = self.network.address_space / threads
        # The entry point for a given thread.
        entry = partial(self._execute_thread, nodes_to_infect=nodes_to_infect, search_space=search_space)

        for index in xrange(0, threads):
            threading.Thread(target=partial(entry, thread_index=index)).start()

    def _execute_thread(self, thread_index, nodes_to_infect, search_space):
        """
        Entry point for a given thread in vulnerable host generation.
        """
        # Not much to do here.
        # _infect_nodes is a decorator that splits off one or more nodes
        # in thread zero (and marks those as infected).  Either way, the 
        # result is an iteration of vulnerable addresses.  We pass this
        # iterator to _emit_hosts, which outputs them.
        self._emit_nodes(self._infect_nodes(
                              nodes_to_infect if thread_index == 0 else 0, 
                              self.network.vulnerable_addresses(
                                  start=thread_index * search_space, 
                                  size=search_space)))

    def _infect_nodes(self, nodes_to_infect, addresses):
        """
        Decorates an iteration of vulnerable addresses.
        Its purpose is, for thread zero, to pull one or more values
        out of that iteration and emit them as infected.
        The remaining addresses are returned (and presumably emitted as 
        vulnerable)
        """
        # Partially-applied function for emitting an infected host
        create_infected_host = partial(self.network.create_host, status=InfectionStatus.INFECTED)
        # Pull n values out of the iteration
        infected_addresses = islice(addresses, nodes_to_infect)
        # For each value pulled, emit it as infected
        foreach(self._emit_node, imap(create_infected_host, infected_addresses))
        # Return the remaining addresses 
        return addresses

    def _emit_nodes(self, addresses):
        """ Emits one vulnerable node for each address in the iteration """
        create_vulnerable_host = partial(self.network.create_host, status=InfectionStatus.VULNERABLE)
        foreach(self._emit_node, imap(create_vulnerable_host, addresses))

    def _emit_node(self, node):
        """ Emits a single node """
        with self.lock:
            print Node.serializer.serialize(node)

if __name__ == '__main__':
    """ Use a tab-separated serializer since we're not in a map/reduce job """
    Node.serializer = TabSeparatedNodeSerializer
    # Argv[1] specifies the network to use in generation (256-node, IPv4, etc)
    # Argv[2] specifies the number of threads over which to operate
    CreateVulnerableHosts(getattr(Network, argv[1])).execute(int(argv[2]) if len(argv) > 2 else 1)

