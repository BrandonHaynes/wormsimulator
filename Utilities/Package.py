import os
import tempfile
import tarfile
from fnmatch import fnmatch

class Package:
    """
    Utility class used to package one or more files into a tar.gz for
    uploading to Amazon EMR
    """

    @classmethod 
    def create(cls, archive_name=None, 
                     directories=['.', 'Network', 'Utilities'], 
                    filetype='*.py'):
        """ Creates a new archive that includes the given files and 
            directories """
        files = []

        try:
            # For each directory, list files and find the matching ones therein
            # Then, filter by desired type
            files = filter(lambda f: fnmatch(f, filetype), 
                           reduce(lambda a, d: a + map(lambda f: d + '/' + f, 
                                                        os.listdir(d)), 
                                  directories, []))
        except OSError:
            # Packaging (obviously) fails on EMR.  Swallow.
            pass

        # Identify our archive name
        if archive_name is None: archive_name = tempfile.mktemp() + '.tar.gz'

        # Add our files to the archive
        tar = tarfile.open(archive_name, "w:gz")
        try:
            map(lambda f: tar.add(f), files)
        finally:
            tar.close()

        return archive_name

