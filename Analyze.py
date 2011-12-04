from sys import argv
from mrjob.job import MRJob
from Network.InfectionStatus import InfectionStatus 
from Network.Node import Node, KeyValuePairNodeSerializer

class Analyze(MRJob):
    """
    Given a file of infected and vulnerable hosts, reports the node 
    count by status
    """

    def __init__(self, **kwargs):
        kwargs['args'] = ['--input-protocol', 'repr'] + \
                         list(kwargs.get('args', []))
        super(Analyze, self).__init__(**kwargs)

        self.statusmap = dict()
        self.hit_list_size = 0

    def mapper(self, key, value):
        node = KeyValuePairNodeSerializer.deserialize((key, value))
        self.statusmap[node.status] = self.statusmap.get(node.status, 0) + 1
        self.statusmap['max.hitlist'] = max(len(node.hit_list), 
                                          self.statusmap.get('max.hitlist', 0))
        return iter([])

    def mapper_final(self):
        return self.statusmap.items()

    def reducer(self, key, values):
        yield key, values.next()

if __name__ == '__main__':
    Analyze(args=argv[1:]).execute()

