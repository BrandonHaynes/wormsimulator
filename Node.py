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
    def deserialize((address, (status, hit_list))):
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
        self.address = address
        self.status = status
        self.hit_list = hit_list
