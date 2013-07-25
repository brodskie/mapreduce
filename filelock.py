import os
import time
import errno
 
class FileLock(object):
    """ A file locking mechanism that has context-manager support 
        can be used as 'with' statement:

        with FileLock("test.txt", timeout=2) as lock:
            lock.write()
    """
    def __init__(self, filename, status='r', timeout=10, delay=.10):
        """ Prepare the file locker. Specify the file to lock and optionally
            the maximum timeout and the delay between each attempt to lock.

        """
        self.locked = False
        self.lockfile = os.path.join( os.getcwd(), '{0:s}.lock'.format(filename) )
        self.filename = filename
        self.status = status
        self.timeout = timeout
        self.delay = delay
 
    def acquire(self):
        """ Acquire the lock, if possible. If the lock is in use, it check again
            every 'delay' seconds. It does this until it either gets the lock or
            exceeds 'timeout' number of seconds, in which case it throws 
            an exception.
        """
        inittime = time.time()
        while True:
            try:
                self.flock = os.open(self.lockfile, os.O_CREAT|os.O_EXCL|os.O_RDWR)
                break;
            except OSError as e:
                if e.errno != errno.EEXIST:
                    raise 
                if (time.time() - inittime) >= self.timeout:
                    raise RuntimeError('FileLock timeout occured.')
                time.sleep(self.delay)
        self.locked = True
 
    def release(self):
        """ Get rid of the lock by deleting the lockfile. 
            When working in a `with` statement, this gets automatically 
            called at the end.
        """
        if self.locked:
            os.close(self.flock)
            os.unlink(self.lockfile)
            self.locked = False
 
    def __enter__(self):
        """ Activated when used in conjunction with 'with'. 
        """
        if not self.locked:
            self.acquire()
        self.file = open(self.filename, self.status)
        return self.file
 
    def __exit__(self, type, value, traceback):
        """ Activated at the end of the with statement.
        """
        self.file.close()
        self.release()
 
    def __del__(self):
        """ Make sure that the FileLock instance doesn't leave a lockfile
            lying around.
        """
        self.file.close()
        self.release()
