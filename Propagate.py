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
        cls.iterations = iterations
        cls.network = network
        job = cls(args=['--input-protocol', 'repr', '--python-archive', Package.create()] + arguments)
        job.execute()

if __name__ == '__main__':
    Propagate.forward(network=Network256, iterations=1, arguments=argv[1:])

