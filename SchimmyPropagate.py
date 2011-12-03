from sys import argv
from itertools import repeat
from itertools import izip
from itertools import imap
from mrjob.job import MRJob
from Network.InfectionStatus import InfectionStatus 
from Network.Node import Node
import Network.Network
from Utilities.Package import Package
from Utilities.Schimmy import SchimmyMRJob
from Utilities.Partitions import Partitions

from Network.Network import Network256

class Propagate(SchimmyMRJob):
    def __init__(self, **kwargs):
        kwargs['args'] = ['--input-protocol', 'repr', 
                          '-r', 'emr',
                          '--hadoop-arg', '-partitioner',
                          '--hadoop-arg', 'org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner',
                          '--jobconf', 'mapred.text.key.partitioner.options=-k1,1', 
                          '--jobconf', 'map.output.key.field.separator=,',
                          '--python-archive', Package.create()] + \
                          kwargs.get('args', [])
        super(Propagate, self).__init__(**kwargs)

        if len(self.args) > 1: 
            raise NotImplemented('Only one input file may be leveraged with Shimmy')

        self.network = getattr(Network.Network, self.options.network)
        self.options.jobconf['mapred.reduce.tasks'] = self.options.partitions
        self.total_reducers = self.options.partitions
        self.total_partitions = self.options.partitions
        self.options.upload_archives.append('%s#partitions' % \
            Partitions.create(self.args[0] if any(self.args) else '', 
                              self.network, self.total_partitions, 
                              self.partition, True))

    def configure_options(self):
        super(Propagate, self).configure_options()
        self.add_passthrough_option(
            '--partitions', type='int', 
            help='Indicate the number of partitions associated with this propagation (an equal number of reducers are created).')
        self.add_passthrough_option(
            '--network', type='string', 
            help='Indicate the class name of the network associated with this propagation, one of { IPv4, IPv6, Network256, NetworkGraphable }.')
        self.add_passthrough_option(
            '--iterations', type='int', default=1, 
            help='Indicate the number of iterations to execute.')
        self.add_passthrough_option(
            '--propagation-delay', type='int', default=0, 
            help='Indicate the propagation delay for new infections.')
        self.add_passthrough_option(
            '--emit-volatile', type='int', default=0, 
            help='Indicate whether to emit infecting edges from the last iteration.')

    def is_volatile(self, status):
        return (self.options.emit_volatile and status == InfectionStatus.INFECTING)

    def is_stable(self, status):
        """ 
        Identifies if a node should be emitted by the mapper
        Some states (such as INFECTING) MAY NOT need to be carried forward.
        """
        return (status == InfectionStatus.VULNERABLE or
                 status == InfectionStatus.INFECTED or
                 status == InfectionStatus.IMMUNE)

    @staticmethod
    def is_new_infection(result_status, input_statuses):
        return result_status == InfectionStatus.INFECTED and \
                all(map(lambda status: status != InfectionStatus.INFECTED, input_statuses)) and \
                any(map(lambda status: status == InfectionStatus.INFECTING, input_statuses))

    def steps(self):
        return map(lambda _: MRJob.mr(self.mapper, self.reducer, self.mapper_final), 
                    xrange(0, self.options.iterations))

    def partition(self, key):
        node = Node.serializer.deserialize((key, None))
        return int((float(node.address) / self.network.address_space) * self.total_partitions)

    def mapper_schimmy(self, key, value):
        # If a node is infected, check its hit list for a target (otherwise choose randomly).
        #   Then, mark that node as INFECTING.
        # Otherwise, do nothing.
        node = Node.serializer.deserialize((key, value))
        if node.status == InfectionStatus.INFECTED:
            if node.propagation_delay == 0:
                target = Node(node.hit_list.pop(), InfectionStatus.INFECTING, [], self.options.propagation_delay, node.address) if any(node.hit_list)  \
                         else self.network.random_node(node.address, InfectionStatus.INFECTING, self.options.propagation_delay)
		target.hit_list = node.hit_list[:len(node.hit_list)/2]
                yield Node.serializer.serialize(target)
            else:
                node.propagation_delay -= 1
            #yield Node.serializer.serialize(node)
        #elif self.is_stable(node.status) or node.status == InfectionStatus.SUCCESSFUL:
        elif node.status == InfectionStatus.SUCCESSFUL:
            yield key, value

    @staticmethod
    def resolve_hit_list(statuses, nodes):
        #if(Propagate.is_new_infection(result_status, candidate_statuses)):
        #    return reduce(lambda a,n: a + n.hit_list, nodes, [])
        if(any(map(lambda status: status == InfectionStatus.SUCCESSFUL, statuses))):
            full_list = reduce(lambda a,n: a + n.hit_list, nodes, [])
            return full_list[:len(full_list) / 2]
        else:
            return reduce(lambda a,n: a + n.hit_list, nodes, [])       

    def resolve_delay(self, statuses, nodes):
        if(any(map(lambda status: status == InfectionStatus.SUCCESSFUL, statuses))):
            return max(map(lambda n: n.propagation_delay, nodes) + [self.options.propagation_delay])
        else:
            return max(map(lambda n: n.propagation_delay, nodes))      

    def reducer_schimmy(self, key, values):
        # Each address (key) will have a set of infection statuses associated therewith.
        # For each such status, call InfectionStatus.compare and aggregate the response.
        # Since we have a total order on these status values, reduce order does not matter.
        # Emit the final status value (and other node metadata)
        candidate_nodes = map(Node.serializer.deserialize, izip(repeat(key), values))
        candidate_statuses = map(lambda n: n.status, candidate_nodes)
        result_status = reduce(InfectionStatus.compare, candidate_statuses, None)

        # Only emit if it's an interesting status, otherwise ignore
        if(self.is_stable(result_status)):
            # Package the node into its current status and other metadata
            result_node = candidate_nodes[0]
            result_node.status = result_status
            result_node.propagation_delay = self.resolve_delay(candidate_statuses, candidate_nodes)
            result_node.hit_list = Propagate.resolve_hit_list(candidate_statuses, candidate_nodes)
            result_node.source = max(map(lambda n: n.source, candidate_nodes))
            yield Node.serializer.serialize(result_node)

            if(Propagate.is_new_infection(result_status, candidate_statuses)):
                yield Node.serializer.serialize(Node(result_node.source, InfectionStatus.SUCCESSFUL, source=result_node.address))
            
        if self.options.emit_volatile:
            for volatile_node in filter(lambda n: self.is_volatile(n.status), candidate_nodes):
                yield Node.serializer.serialize(volatile_node)

if __name__ == '__main__':
    """ Propogate an input network in time for a given number of iterations. """
    """ Volatile nodes represent attempted infections between nodes, and may be optimally emitted """
    """ Propagation delay is the delay (in iterations) that an infecting and infected node must wait before resuming scanning """
    if not any(map(lambda a: a == '--network', argv)):
        print '--network switch required.  Use --help to display all options.'
    elif not any(map(lambda a: a == '--partitions', argv)):
        print '--partitions switch required.  Use --help to display all options.'
    else:
        Propagate(args=argv[1:]).execute()

