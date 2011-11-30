from Node import Node
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
    def random_node(cls, source_address=None, with_status=InfectionStatus.UNKNOWN):
        """ Select a random node across the full address space """
        return Node(random.randrange(0, cls.address_space), with_status, [source_address] if not source_address is None else [])

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

