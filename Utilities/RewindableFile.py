from collections import deque

class RewindableFile:
    """
    File decorator that supports rewinding.
    """

    def __init__(self, file, limit=1):
        self.file = file
        self.limit = limit
        self.buffer = deque([])
        self.prefix = deque([])
        self.__enter__ = file.__enter__
        self.__exit__ = file.__exit__
        self.close = file.close

    def readline(self):
        # Draw from buffer before performing additional reads
        if(any(self.prefix)):
            self.buffer.append(self.prefix.pop())
        else:
            line = self.file.readline()
            self.buffer.append(line)
            if(len(self.buffer) > self.limit):
                self.buffer.popleft()
        return self.buffer[-1]

    def rewind(self):
        """ Rewinds the decorated file by one line """
        if(not any(self.buffer)):
            raise IndexError('Buffer underflow; too many rewinds')
        self.prefix.append(self.buffer.pop())
        return self.prefix[-1]

    def is_rewindable(self):
        """ True if the rewind buffer is non-empty """
        return any(self.buffer)

    def rewind_if_rewindable(self):
        """ If the rewind buffer is non-empty, rewind.  Otherwise no-op. """
        return rewind() if self.is_rewindable() else None

