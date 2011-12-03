import os
import subprocess
import re

class HDFSUtilities:
    """
    Utility class exposing HDFS operations.  Assumes that the hadoop 
    executable is part of the current path.
    """

    @staticmethod
    def list_files(path):
        """ 
        Generates a list of files residing on the HDFS server at the given 
        path and recursively including all subdirectories. 
        """
        return HDFSUtilities.__exec(["hadoop", "dfs", "-lsr", path])

    @staticmethod
    def download_file(uri, local_directory):
        """ Moves a file from the HDFS to local storage """ 
        return HDFSUtilities.__exec(["hadoop", "dfs", "-copyToLocal", 
                                     uri, local_directory])

    @staticmethod
    def download_files(filenames, local_directory, create_directory=True):
        """ Moves a list of files from the HDFS to local storage """
        # Create the destination directory if requested
        if create_directory and not os.path.exists(local_directory): 
            os.mkdir(local_directory)
        # For each file, download it.
        map(lambda filename: HDFSUtilities.download_file(
                filename, 
                HDFSUtilities._make_local_filename(filename, local_directory)),
            filenames)

    @staticmethod
    def _make_local_filename(uri, local_directory):
        """ Given a HDFS URI, convert it into a local filename. """
        pattern = '.*/(?P<filename>[^/]+)$'
        local_filename = re.search(pattern, uri)
        separator = '/' if not local_directory.endswith('/') else ''
        return local_directory + separator + local_filename.group(1) \
                    if not local_filename is None else None

    @staticmethod
    def __exec(arguments):
        """ 
        A really dangerous function that accepts an arbitrary list of 
        arguments and executes them in a subprocess.  Doesn't even try to 
        filter those arguments for safety.  

        This function is ripe for an injection attack.  Please don't try this 
        at home.
        """
        proc = subprocess.Popen(arguments, stdin=subprocess.PIPE, 
                                           stdout=subprocess.PIPE)
        output = proc.stdout.read()
        proc.stdin.close()
        return output


