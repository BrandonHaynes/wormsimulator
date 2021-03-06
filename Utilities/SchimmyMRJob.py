from itertools import chain
from mrjob.job import MRJob
from Utilities.RewindableFile import RewindableFile
from Utilities.PartitionUtilities import PartitionUtilities
from Network.Node import TabSeparatedNodeSerializer

class SchimmyException(Exception): pass

class SchimmyMRJob(MRJob):
    """
    Base class for map/reduce jobs that use the Schimmy pattern.

    Expects an initial set of partition files located on the local file system
    in the ./partition directory of the form part-0000x, where x is an integer
    in the range (0, #partitions-1).  Subsequent steps will automatically 
    utilize partitions generated from the prior step (which are themselves
    copied from the HDFS to the local file system).

    Expects the following definitions in a subclass (in addition to the MRJOB
    mapper and reducer):

        sel.options.partitions: integer yielding the total number of partitions
        partition(key): Given a key, indicates its partition

    By default, this class assumes that key "0" is the lowest that will be
    encountered for any partition; this may be overridden via get_sentinel.
    Regardless, one key must be reserved for Schimmy initialization, and it
    must always be the first such key encountered.
    """

    def __init__(self, **kwargs):
        super(SchimmyMRJob, self).__init__(**kwargs)
        self.count = None

        # Wire up our Schimmy interceptors
        self.mapper_schimmy = self.mapper
        self.reducer_schimmy = self.reducer
        self.reducer = self._reducer
        self.mapper = self._mapper
        if 'mapper_final' in self.__dict__:
            self.mapper_final_schimmy = self.mapper_final
        else:
            self.mapper_final_schimmy = lambda _: iter([])
        self.mapper_final = self._mapper_final

        if len(self.args) > 1: 
            raise SchimmyException('Only one input file may be leveraged with '+
                                    'Shimmy')

    def __del__(self):
        if 'options' in self.__dict__:
            map(self.close_partition, xrange(self.options.partitions))

    def configure_options(self):
        super(SchimmyMRJob, self).configure_options()
        self.add_passthrough_option(
            '--partitions', type='int', 
            help='Indicate the number of partitions associated with this '+\
                  'propagation (an equal number of reducers are created).')

    ########################################################    

    def partition(self, key):
        """ Given a key, indicates its partition """
        raise NotImplemented

    def get_sentinel(self, partition):
        """ 
        Retrieves a value that is a minimum for the given partition.
        Need not be the same for all partitions.
        """
        return 0

    @property
    def partition_counts(self):
        if "_partition_counts" not in self.__dict__ or \
                self._partition_counts is None:
            self._partition_counts = [0] * self.options.partitions
        return self._partition_counts

    ########################################################

    @property
    def partition_file(self):
        """ Reference to the partition file associated with this reducer """
        if "_partition_file" not in self.__dict__ or \
                self._partition_file is None:
            self._partition_file = \
                RewindableFile(open(self.partition_filename), 1024)
        return self._partition_file

    @property
    def partition_filename(self):
        """ Gets the partition filename associated with this reducer """

        if "_partition_filename" not in self.__dict__ or \
                self._partition_filename is None:
            self._partition_filename = \
                PartitionUtilities.get_partition_filename(
                    self.current_partition, self.options.partitions,
                    lambda key: self.partition(key), 
                    lambda line: TabSeparatedNodeSerializer.deserialize(line)\
                                  .address)
        
        return self._partition_filename

    def close_partition(self, partition):
        """ Closes the partition file associated with this reducer, if open """
        if "_partition_file" in self.__dict__ and \
                not self._partition_file is None:
            partition_file, self._partition_file = self._partition_file, None
            if partition_file != None: partition_file.close()
            self._partition_filename = None
            if '_partition_counts' in self.__dict__: 
                self._partition_counts[partition] = None
            self.count = None

    ########################################################

    def _next_kvp(self):
        """ Gets the next key/value pair from the partition file """
        if not self.partition_file: return None
        line = self.partition_file.readline().strip()
        # Fake "repr" format
        return map(eval, line.split('\t')) if line else None
    
    def _next_until(self, key):
        # Do while pairs left and the pair-key is less than the parameter-key
        pair = self._next_kvp()
        while pair is not None and (pair[0] < key or key is None): 
            yield pair
            pair = self._next_kvp()
        if pair is not None:
            yield pair
        
    ########################################################

    def _mapper(self, key, value): 
        # Delegate generation to the underlying mapper
        # And decorate with a partition
        for skey, svalue in self.mapper_schimmy(key, value):
            yield (self._update_partition(skey), skey), svalue

    def _mapper_final(self):
        # Emit all of our partition counts using the special
        # sentinel value (which ensures it is first in the shuffle)
        for partition, count in enumerate(self.partition_counts):
            yield (partition, self.get_sentinel(partition)), count
        map(self.close_partition, xrange(self.options.partitions))

    def _update_partition(self, key):
        """ Increments the appropriate partition's result count """
        current_partition = self.partition(key)
        self.partition_counts[current_partition] += 1
        return current_partition

    ########################################################

    def _reducer(self, (partition, key), values):
        # We expect that the sentinel value will be the first kvp encountered.
        # For that special line, we initialize our reducer.
        if key == self.get_sentinel(partition):
            # Initialize our reducer metadata
            self.current_partition = partition
            self.count = sum(values)
            results = iter([])
        else:
            # All other kvps are "real" and we need to process them accordingly
            values = list(values)
            # Keep track of the kvps we've encountered (we expect one value for
            # each mapper output, so use the length of values here).
            self.count -= len(values)

            # Process the kvp.
            results = self.__process(key, values)

        # When we've encountered all of the kvps that we're expecting, we can
        # blast everything left in the partition file to our output stream.        
        if self.count <= 0:
            results = chain(results, self.__process_to_end())
            self.close_partition(partition)

        return results

    def __process_to_end(self):
        """ Blast all remaining kvps in the partition file """
        return self.__process(None, [])

    def __process(self, target_key, values):
        """
        Utility function to sequentially handle lines in the partition file
        until the target_key is located.  If we (inadvertantly) iterate past
        the target_key, we rewind such that the incorrect key is not consumed.
        """
        key = None

        # Iterate across our partition file until we reach target_key
        for (key, value) in self._next_until(target_key):
            # Emit if we haven't reached the target key (or if its null)
            if key <= target_key or target_key is None:
                for pair in self.reducer_schimmy(key, ([value] + values) \
                                    if key == target_key else [value]):
                    yield pair

        # Have we accidently gone too far?  If so, rewind once.
        if key > target_key:
            self._partition_file.rewind()
        # We may not have actually output the current kvp above (perhaps 
        # because the kvp does not exist in the partition file).  This is 
        # ok, but we still need to emit it if that is the case.
        if target_key != key and not target_key is None:
            for pair in self.reducer_schimmy(key, values):
                yield pair

