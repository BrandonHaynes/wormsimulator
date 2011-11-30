from sys import argv
from mrjob.job import MRJob
import mrjob.util
import random
from Network.InfectionStatus import InfectionStatus 
from Network.Node import Node

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
        mrjob.util.log_to_stream(level=mrjob.util.logging.ERROR)
        kwargs['args'] = ['--input-protocol', 'repr'] + list(kwargs.get('args', []))

        super(CreateHitLists, self).__init__(**kwargs)
        self.infected_nodes = []
        self.vulnerable_nodes = []
    
    def configure_options(self):
        super(CreateHitLists, self).configure_options()
        self.add_passthrough_option(
            '--size', type='int', default=1, help='Indicate the desired hit-list size of for vulnerable nodes.')

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
            infected_node.hit_list = map(lambda n: n.address, random.sample(self.vulnerable_nodes, self.options.size))
            yield Node.serializer.serialize(infected_node)

    def reducer(self, key, values):
        # Identity reducer.  Expecting one value per key.
        yield key, values.next()

if __name__ == '__main__':
    CreateHitLists(args=argv[1:]).execute()

