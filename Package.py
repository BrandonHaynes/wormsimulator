import os
import tempfile
import tarfile
from fnmatch import fnmatch

class Package:
    @classmethod 
    def create(cls, archive_name=None, directory='.', filetype='*.py'):
        files = filter(lambda f: fnmatch(f, filetype), os.listdir(directory))
        if archive_name is None: archive_name = tempfile.mktemp() + '.tar.gz'

        tar = tarfile.open(archive_name, "w:gz")
        map(lambda f: tar.add(f), files)
        tar.close()

        return archive_name

