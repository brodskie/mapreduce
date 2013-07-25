# Parallel timing class for profiling purposes
from time import time
import mpi4py.MPI as MPI
import numpy as np

comm = MPI.COMM_WORLD

funclst = []

counter = []
timelst = []
inittime = time()

def add(time, name):
    if name in funclst:
        idx = funclst.index(name)
        counter[idx] += 1
        timelst[idx] += time
    else:
        funclst.append(name)
        counter.append(1)
        timelst.append(time)

def finalize():
    counterlst = comm.reduce(counter, op=MPI.SUM, root=0)
    redtimelst = comm.reduce(timelst, op=MPI.SUM, root=0)
    # Output all time usage to stdout
    if comm.rank == 0:
        print('\nSubroutine   : No. of Calls:    Total(s): Per Proc(s)')
        for ii in range(len(funclst)):
            print('{0:13s}:   {1:10d}:{2:12d}:{3:12d}'.\
                format(funclst[ii], counterlst[ii], \
                int(redtimelst[ii]), int(redtimelst[ii]/comm.size) ) )

class Timer(object):
    def __init__(self, name):
        self.name = name
        self.inittime = time()

    def __del__(self):
        add(self.elapsed(), self.name)

    def elapsed(self):
        return time() - self.inittime
