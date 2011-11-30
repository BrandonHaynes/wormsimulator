CreateHistLists.py                                                                                  0000664 0057463 0057463 00000004205 11663245020 014400  0                                                                                                    ustar   bhaynes                         bhaynes                                                                                                                                                                                                                from mrjob.job import MRJob
import random
from InfectionStatus import InfectionStatus 
from Node import Node

class CreateHitLists(MRJob):
    """
    Given a file of infected and vulnerable hosts (presumably created
    via CreateVulnerableHosts), adds a hit list to each infected host
    that consists of n vulnerable hosts.

    Hit lists are used to increase the initial speed of propogation.

    Uses a map/reduce job to effectuate this process; requires O(m)
    space to create hit lists, where m is the number of vulnerable
    hosts assigned to a particular map job.  
    
    This is a bit suboptimal, since an infected node will only receive
    nodes within its local mapping scope, but is probably sufficient.
    """

    def __init__(self, **kwargs):
        super(CreateHitLists, self).__init__(**kwargs)
        self.infected_nodes = []
        self.vulnerable_nodes = []

    def mapper(self, key, value):
        # If the node is infected, save it for emission during mapper_final
        # Otherwise, emit it
        # Note that in both cases the node is appended it its appropriate list
        # (either infected or vulnerable)
        node = Node.serializer.deserialize((key, value))
        if(node.status == InfectionStatus.INFECTED):
            self.infected_nodes.append(node)
        else:
            self.vulnerable_nodes.append(node)
            yield key, value

    def mapper_final(self):
        # Here we handle infected nodes (which were not emitted during the mapper)
        # For each such node, create a hit list, attach it, and then emit the result.
        for infected_node in self.infected_nodes:
            infected_node.hit_list = map(lambda n: n.address, random.sample(self.vulnerable_nodes, self.size))
            yield Node.serializer.serialize(infected_node)

    def reducer(self, key, values):
        # Identity reducer.  Expecting one value per key.
        yield key, values.next()

    @classmethod
    def create(cls, size):
        """ Creates hit lists for each infected node using a list of given size. """
        cls.size = size
        cls.run()

if __name__ == '__main__':
    CreateHitLists.create(size=300)

                                                                                                                                                                                                                                                                                                                                                                                           CreateVulnerableHosts.py                                                                            0000664 0057463 0057463 00000010221 11663245020 015565  0                                                                                                    ustar   bhaynes                         bhaynes                                                                                                                                                                                                                import threading
from sys import argv
from itertools import imap
from itertools import islice
from functools import partial
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
    # Argv[1] specifies the number of threads over which to operate
    CreateVulnerableHosts(NetworkGraphable).execute(int(argv[1]) if len(argv) > 1 else 1)

                                                                                                                                                                                                                                                                                                                                                                               InfectionStatus.py                                                                                  0000664 0057463 0057463 00000004352 11663245020 014453  0                                                                                                    ustar   bhaynes                         bhaynes                                                                                                                                                                                                                class InfectionStatus:
    """
    An enumeration representing the infection status of a network node.

    UNKNOWN = Unused, indicates an error state
    IMMUNE = Currently unused, indicates that a node is not vulnerable to infection
    VULNERABLE = Node is vulnerable to infection, but not currently infected
    INFECTING = Node is in the process of being infected (this may not be successful)
                This is generally a transition state between vulnerable and infected,
                or immune and immune.
    INFECTED = Node is infected and will transmit infecting messages.

    State diagram:

    IMMUNE --- any ---> IMMUNE
    UKNOWN --- any ---> UKNOWN
    INFECTED --- { INFECTING } ---> INFECTED
    VULNERABLE --- { INFECTING, INFECTED } ---> INFECTED   
    VULNERABLE --- { VULNERABLE } ---> VULNERABLE
    INFECTING --- { VULNERABLE } ---> INFECTED   

    Note that if we ignore IMMUNE and UNKNOWN, we have a total order of the form:
        VULNERABLE <= INFECTING <= INFECTED
    """

    UNKNOWN = -1
    IMMUNE = 0
    VULNERABLE = 1
    INFECTING = 2
    INFECTED = 3

    @staticmethod
    def compare(left, right):
        """
        Implements the state diagram above.  Since we have few states,
        we just use conditionals.

        To more readily allow comparision composition during a reduce,
        we coalesce on None such that compare(x,None)=x and compare(None,x)=x.
        """
        if left is None:
            return right
        elif right is None:
            return left
        elif left == InfectionStatus.IMMUNE or right == InfectionStatus.IMMUNE:
            return InfectionStatus.IMMUNE
        elif left == InfectionStatus.INFECTED or right == InfectionStatus.INFECTED:
            return InfectionStatus.INFECTED
        elif ((left == InfectionStatus.VULNERABLE and right == InfectionStatus.INFECTING) or 
              (left == InfectionStatus.INFECTING and right == InfectionStatus.VULNERABLE)):
            return InfectionStatus.INFECTED
        elif left == InfectionStatus.VULNERABLE or right == InfectionStatus.VULNERABLE:
            return InfectionStatus.VULNERABLE
        elif left == InfectionStatus.UNKNOWN and right == InfectionStatus.UNKNOWN:
            return InfectionStatus.UNKNOWN
        
                                                                                                                                                                                                                                                                                      Network.py                                                                                          0000664 0057463 0057463 00000005263 11663245020 012764  0                                                                                                    ustar   bhaynes                         bhaynes                                                                                                                                                                                                                from Node import Node
from InfectionStatus import InfectionStatus
from itertools import ifilter
import random

class Network:
    """ 
    Exposes methods useful to dealing with address in a network.

    The network is assumed to be sparse, and individual nodes are
    not specifically tracked herein.  Instead, this class exposes
    methods used to sample vulnerable addresses probabilistically,
    create lists of such vulnerable addresses, and create hosts
    (given an address and status).  
    """

    @classmethod
    def random_node(cls, with_status=InfectionStatus.UNKNOWN):
        """ Select a random node across the full address space """
        return Node(random.randrange(0, cls.address_space), with_status)

    @classmethod
    def create_host(cls, address, status=InfectionStatus.UNKNOWN):
        """ Creates a host with the given address and optional status """
        return Node(address, status)

    @classmethod
    def vulnerable_addresses(cls, start=0, size=None, probability=None):
        """ 
        Returns an iteration of vulnerable addresses 
        By default this is across the entire address space, but 
        optional parameters can be used to control the addresses examined.
        """
        return ifilter(lambda address: cls._sample_vulnerable(), 
                        xrange(start, (size if not(size is None) else cls.address_space) + start))

    @classmethod
    def _sample_vulnerable(cls):
        """ Sample using the probability that a host is vulnerable """
        return Network._sample(cls.probability_vulnerable)

    @staticmethod
    def _sample(probability):
        return random.random() < probability
        

class Network256(Network):
    """
    Create a network of 2^8 nodes, with a default 10% vulnerable probability.
    Both of these values are absurd, but useful for testing.
    """
    address_space = 2**8
    probability_vulnerable = 0.10

class NetworkGraphable(Network):
    """
    Create a network of 2^8 nodes, with a default 10% vulnerable probability.
    Both of these values are absurd, but useful for testing.
    """
    address_space = 2**16
    probability_vulnerable = 1.0/6

class IPv4(Network):
    """
    Represents the IPv4 network.
    We assume 3M /4 vulnerable hosts.
    This is a bit high (Code Red had 350k hosts and Slammer had 100k)
    Should probably throttle it back.
    """
    address_space = 2**32

    expected_hosts = 3000000
    expected_vulnerable = expected_hosts / 4
    probability_vulnerable = float(expected_vulnerable) / address_space

class IPv6(IPv4):
    """ 
    Represents an IPv6 address space.
    Vulnerability cardnality and probability remains the same as IPv4
    """
    address_space = 2**128

                                                                                                                                                                                                                                                                                                                                             Node.py                                                                                             0000664 0057463 0057463 00000003406 11664515460 012226  0                                                                                                    ustar   bhaynes                         bhaynes                                                                                                                                                                                                                from InfectionStatus import InfectionStatus 


class KeyValuePairNodeSerializer:
    """
    Serializer for Node instances.
    Serializes by producing a key-value pair of node metadata
    Keys are addresses, values are node metadata.
    """
    @staticmethod
    def serialize(node):
        """ Converts a node into a key-value pair """
        return node.address, (node.status, node.hit_list)

    @staticmethod
    def deserialize((address, metadata)):
        status, hit_list = metadata if not metadata is None else (InfectionStatus.UNKNOWN, [])
        """ Converts a key-value pair into a node instance """
        return Node(address, status, hit_list)

class TabSeparatedNodeSerializer(KeyValuePairNodeSerializer):
    """
    Alternate serializer for node instances.
    Serializes by producing a string representing a node instance,
    string is a tab-separated key-value pair.
    Keys are node addresses, values are node metadata.
    """
    @staticmethod
    def deserialize(text):
        address, metadata = map(eval, text.split('\t'))
        status, hit_list = metadata if not metadata is None else (InfectionStatus.UNKNOWN, [])
        return Node(address, status, hit_list)

    @staticmethod
    def serialize(node):
        return "%s\t%s,%s" % (node.address, node.status, node.hit_list)

class Node:
    """
    Represents a node in the network.

    All nodes have an address and an infection status. 
    Infected nodes may maintain a hit list of known-vulnerable addresses.   
    """

    # Maintain a class-bound serializer for nodes
    serializer = KeyValuePairNodeSerializer

    def __init__(self, address, status, hit_list=[]):
        self.address = int(address) if not address is None else None
        self.status = status
        self.hit_list = hit_list
                                                                                                                                                                                                                                                          Package.py                                                                                          0000664 0057463 0057463 00000000730 11665300456 012667  0                                                                                                    ustar   bhaynes                         bhaynes                                                                                                                                                                                                                import os
import tempfile
import tarfile
from fnmatch import fnmatch

class Package:
    @classmethod 
    def create(cls, archive_name=None, directory='.', filetype='*.py'):
        files = filter(lambda f: fnmatch(f, filetype), os.listdir(directory))
        if archive_name is None: archive_name = tempfile.mktemp() + '.tar.gz'

        tar = tarfile.open(archive_name, "w:gz")
        map(lambda f: tar.add(f), files)
        tar.close()

        return archive_name

                                        Propagate.py                                                                                        0000664 0057463 0057463 00000007113 11665314402 013254  0                                                                                                    ustar   bhaynes                         bhaynes                                                                                                                                                                                                                from sys import argv
from itertools import repeat
from itertools import izip
from itertools import imap
from mrjob.job import MRJob
from InfectionStatus import InfectionStatus 
from Node import Node
from Network import IPv4, IPv6, Network256, NetworkGraphable
from Package import Package

class Propagate(MRJob):
    def __init__(self, **kwargs):
        super(Propagate, self).__init__(**kwargs)
        # This needs to go when Schimmy is applied
        self.total_reducers = self.options.jobconf.get('mapred.map.tasks', 1)

    @staticmethod
    def is_emitted(status):
        """ 
        Identifies if a node should be emitted by the reducer 
        Some states (such as INFECTING) don't need to be carried forward.
        """
        return (status == InfectionStatus.VULNERABLE or
                 status == InfectionStatus.INFECTED or
                 status == InfectionStatus.IMMUNE)

    def steps(self):
        return map(lambda _: MRJob.mr(self.mapper, self.reducer), 
                    xrange(0, self.iterations))

    def partition(key):
        # This needs to go when Schimmy is applied
        node = Node.serializer.deserialize((key, value))
        return super.partition(node.address % self.total_reducers)

    def mapper(self, key, value):
        # If a node is infected, check its hit list for a target (otherwise choose randomly).
        #   Then, mark that node as INFECTING.
        #   Then, emit the infected node (this should be removed with Schimmy)
        # Otherwise, do nothing and emit.
        node = Node.serializer.deserialize((key, value))
        if(node.status == InfectionStatus.INFECTED):
            target = Node(node.hit_list.pop(), InfectionStatus.INFECTING) if any(node.hit_list)  \
                     else self.network.random_node(InfectionStatus.INFECTING)
            yield Node.serializer.serialize(node)
            yield Node.serializer.serialize(target)
        else:
            yield key, value

    def reducer(self, key, values):
        # Each address (key) will have a set of infection statuses associated therewith.
        # For each such status, call InfectionStatus.compare and aggregate the response.
        # Since we have a total order on these status values, reduce order does not matter.
        # Emit the final status value (and other node metadata)
        candidate_nodes = map(Node.serializer.deserialize, izip(repeat(key), values))
        candidate_statuses = imap(lambda n: n.status, candidate_nodes)
        result_status = reduce(InfectionStatus.compare, candidate_statuses, None)

        # Only emit if it's an interesting status, otherwise ignore
        if(Propagate.is_emitted(result_status)):
            # Package the node into its current status and other metadata
            result_node = candidate_nodes[0]
            result_node.status = result_status
            yield Node.serializer.serialize(result_node)

    @classmethod
    def forward(cls, network=Network256, iterations=1, arguments=[]):
        """ Propogate an input network in time for a given number of iterations. """
        #cls.iterations = iterations
        #cls.network = network
        #cls.run()

        #archive_name = Package.create()
        job = Propagate(args=['-r', 'emr', '--input-protocol', 'repr', '--python-archive', 'network.tar.gz'] + arguments)
        job.DEFAULT_INPUT_PROTOCOL = 'repr'
        job.options.input_protocol = 'repr'
        job.options.protocol = 'repr'
        job.iterations = iterations
        job.network = network
        job.execute()

if __name__ == '__main__':
    Propagate.forward(network=NetworkGraphable, iterations=1, arguments=argv)

                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     