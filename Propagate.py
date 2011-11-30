from sys import argv
from itertools import repeat
from itertools import izip
from itertools import imap
from mrjob.job import MRJob
from InfectionStatus import InfectionStatus 
from Node import Node
from Package import Package
from Network import IPv4, IPv6, Network256, NetworkGraphable
import Network

class Propagate(MRJob):
    def __init__(self, **kwargs):
        self.network = kwargs.pop('network')
        self.iterations = kwargs.pop('iterations', 1)
        self.emit_volatile = kwargs.pop('emit_volatile', False)
	kwargs['args'] = ['--input-protocol', 'repr', '--python-archive', Package.create()] + kwargs.get('args', [])

        super(Propagate, self).__init__(**kwargs)
        # This needs to go when Schimmy is applied
        self.total_reducers = self.options.jobconf.get('mapred.map.tasks', 1)

    def is_volatile(self, status):
        return (self.emit_volatile and status == InfectionStatus.INFECTING)

    def is_stable(self, status):
        """ 
        Identifies if a node should be emitted by the mapper
        Some states (such as INFECTING) MAY NOT need to be carried forward.
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
        if node.status == InfectionStatus.INFECTED:
            target = Node(node.hit_list.pop(), InfectionStatus.INFECTING, [node.address]) if any(node.hit_list)  \
                     else self.network.random_node(node.address, InfectionStatus.INFECTING)
            yield Node.serializer.serialize(node)
            yield Node.serializer.serialize(target)
        elif self.is_stable(node.status):
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
        if(self.is_stable(result_status)):
            # Package the node into its current status and other metadata
            result_node = candidate_nodes[0]
            result_node.status = result_status
            yield Node.serializer.serialize(result_node)

        if self.emit_volatile:
            for volatile_node in filter(lambda n: self.is_volatile(n.status), candidate_nodes):
                yield Node.serializer.serialize(volatile_node)

if __name__ == '__main__':
    """ Propogate an input network in time for a given number of iterations. """
    Propagate(network=Network256, iterations=8, args=argv[1:], emit_volatile=False).execute()

