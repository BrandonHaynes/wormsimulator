from InfectionStatus import InfectionStatus 


class KeyValuePairNodeSerializer:
    """
    Serializer for Node instances.
    Serializes by producing a key-value pair of node metadata
    Keys are addresses, values are node metadata.
    """
    @staticmethod
    def serialize(node):
        """ Converts a node into a key-value pair """
        return node.address, (node.status, node.hit_list, node.propagation_delay, node.source)

    @staticmethod
    def deserialize((address, metadata)):
        status, hit_list, propagation_delay, source = metadata if not metadata is None else (InfectionStatus.UNKNOWN, [], 0, 0)
        """ Converts a key-value pair into a node instance """
        return Node(address, status, hit_list, propagation_delay, source)

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
        status, hit_list, propagation_delay, source = metadata if not metadata is None else (InfectionStatus.UNKNOWN, [], 0, 0)
        return Node(address, status, hit_list, propagation_delay, source)

    @staticmethod
    def serialize(node):
        return "%s\t%s,%s,%s,%s" % (node.address, node.status, node.hit_list, node.propagation_delay, node.source)

class Node:
    """
    Represents a node in the network.

    All nodes have an address and an infection status. 
    Infected nodes may maintain a hit list of known-vulnerable addresses.   
    """

    # Maintain a class-bound serializer for nodes
    serializer = KeyValuePairNodeSerializer

    def __init__(self, address, status, hit_list=[], propagation_delay=0, source=0):
        self.address = int(address) if not address is None else None
        self.status = status
        self.hit_list = hit_list
        self.propagation_delay = propagation_delay
        self.source = source
