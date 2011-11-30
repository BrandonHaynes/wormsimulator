class InfectionStatus:
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
    SUCCESSFUL --- any ---> any
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
    SUCCESSFUL = 4

    @staticmethod
    def compare(left, right):
        """
        Implements the state diagram above.  Since we have few states,
        we just use conditionals.

        To more readily allow comparision composition during a reduce,
        we coalesce on None such that compare(x,None)=x and compare(None,x)=x.
        """
        if left is None or left == InfectionStatus.SUCCESSFUL:
            return right
        elif right is None or right == InfectionStatus.SUCCESSFUL:
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
        
