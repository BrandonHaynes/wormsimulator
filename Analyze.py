from mrjob.job import MRJob
import random
from InfectionStatus import InfectionStatus 
from Node import Node

class Analyze(MRJob):
    """
    Given a file of infected and vulnerable hosts, reports the node count by status
    """

    def __init__(self, **kwargs):
        super(Analyze, self).__init__(**kwargs)
        self.statusmap = dict()

    def mapper(self, key, value):
        node = Node.serializer.deserialize((key, value))
        self.statusmap[node.status] = self.statusmap.get(node.status, 0) + 1
        return iter([])

    def mapper_final(self):
        return self.statusmap.items()

    def reducer(self, key, values):
        yield key, values.next()

if __name__ == '__main__':
    Analyze.run()

