from itertools import imap
import re
import tempfile
from HDFSUtilities import HDFSUtilities

class PartitionUtilities:
    """
    Utility class to identify the last-step partition directory,
    download those files from HDFS, and identify which file belongs to a 
    given partition.
    """

    # These assume current values used by Hadoop, but could change
    filename_pattern = ' /.+/step-output/\d+/part-\d+$'
    part_pattern = '/.+/step-output/(?P<step>\d+)/part-\d+'

    @staticmethod
    def get_partition_filename(current_partition, total_partitions, 
                               partitioner, transformer):
        partition_directory = \
            PartitionUtilities.get_last_step_directory(total_partitions)

        # If there is no last-step directory, we're on the first step and 
        # should use the initial partition files.
        if partition_directory is None:
            return 'partitions/part-%05d'% current_partition
        else:
            return PartitionUtilities.get_partition_file(
                        partition_directory, 
                        current_partition, total_partitions, 
                        partitioner, transformer)

    @staticmethod
    def get_partition_file(directory, current_partition, total_partitions, 
                            partitioner, transformer):
        """ 
        Given a directory and current partition, identifies the partition 
        associated therewith.

        directory: The directory in which the partition files reside
        current_partition: The desired partition
        total_partitions: The total number of partitions
        partitioner: A function from key to integer which idenifies a partition
        transformer: A function from line (of an arbitrary partition file) to 
                     key; used to examine partition files for appropriateness.
        """
        empty_file = None

        for filename in imap(lambda p: '%s/part-%05d' % (directory, p), 
                              xrange(total_partitions)):
            # Get the partition associated with this file
            partition = PartitionUtilities._get_file_partition( \
                filename, partitioner, transformer)

            # Empty file?  Store it just in case.
            if(partition is None):
                empty_file = filename
            # Return if a match is found
            elif(partition == current_partition):
                return filename

        # Return an arbitrary empty file if one was found; None otherwise
        return empty_file

    @staticmethod
    def get_last_step_directory(total_partitions, 
                                 local_directory=None):
        """ 
        Gets the directory associated with the most recent step executed in 
        this job, or None if none exists.
        """
        last_step, filenames = \
            PartitionUtilities.__get_last_step_filenames(total_partitions)
        local_directory = (local_directory or tempfile.mkdtemp(suffix='.%d')) % \
            (last_step if not last_step is None else -1)
        HDFSUtilities.download_files(filenames, local_directory)
        return local_directory if not last_step is None else None

    @staticmethod
    def get_step_dictionary(total_partitions):
        """
        Gets a dictionary of steps and the partition files associated with 
        each.
        """
        raw_output = HDFSUtilities.list_files('/')
        filenames = imap(lambda f: f.strip(), 
                         re.findall(PartitionUtilities.filename_pattern, 
                                    raw_output, re.MULTILINE))
        pairs = imap(lambda filename: \
                        (int(re.match(PartitionUtilities.part_pattern,
                                      filename).group(1)), 
                         filename), 
                     filenames)
        raw_dictionary = reduce(lambda d, p: \
                                    PartitionUtilities.__insert(d, *p), 
                                pairs, dict())
        return dict(filter(lambda (_, v): len(v) == total_partitions, 
                            raw_dictionary.items()))

    @staticmethod
    def _get_file_partition(filename, partitioner, transformer):
        """
        Given a file, examines it and determines which partition to which it
        belongs.  This requires reading the first line from the given file.
        """
        f = open(filename) 
        try:
            line = f.readline().strip()
            return partitioner(transformer(line)) \
                        if line else None
        finally:
            f.close()

    @staticmethod
    def __insert(dictionary, key, value):
        """ 
        Helper function to insert into a dictionary and return the dictionary.
        Makes for convenient reduction.
        """
        dictionary.setdefault(key, []).append(value)
        return dictionary

    @staticmethod
    def __get_last_step_filenames(total_partitions):
        """ 
        Identifies all steps that exist on the HDFS.
        Notes which ones have fully completed.
        Returns a pair consisting of the last step and 
        the partitions assoicated therewith.
        """
        step_dictionary = \
            PartitionUtilities.get_step_dictionary(total_partitions)
        if(any(step_dictionary.keys())):
            last_step = max(step_dictionary.keys())
            return last_step - 1, step_dictionary[last_step]
        else:
            return None, []

