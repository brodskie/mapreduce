#!/bin/env python

import heapq, timer
import mpi4py.MPI as MPI
import os, os.path, sys, glob
if sys.version_info[0] <= 2:
    import cPickle as pickle
else:
    import pickle

from mpi4py.MPI import COMM_WORLD as world
from operator import itemgetter
from time import sleep, time
from math import ceil
#from filelock import FileLock
from timer import Timer

# constants for MPI communication
MAP_START = 0
MAP_FINISH = 1
INIT_START = 2
REDUCE_START = 10
REDUCE_FINISH = 11
UPDATE_MAP = 100
UPDATE_REDUCE = 101
UPDATE_CONFIG = 102
TERMINATE = 9999

# Worker state object
class State(object):
    """Worker's running state object
    """
    def __init__(self, id):
        self.id = id
        self._state = -1

    def isFree(self): return self._state == -1
    def isFaulty(self): return self._state == 999999
    def setBusy(self): self._state = self.id
    def setFree(self): self._state = -1
    def setFaulty(self): self._state = 999999

    # implement the comparator based on state
    def __eq__(self, other):
        return self._state == other._state
    def __lt__(self, other):
        return self._state < other._state

# Master object
class Master(object):
    """MapReduce Master Node

    Master node read configurations, datafiles
    and mapfn, reducefn, readfn and finalfn 
    from the user written configure module.
    Note that each datafile has to be able to
    fit into single machine's memory.
    Data file is either in ascii mode or pickle format.
    """
    def __init__(self, config):
        """Read in user created config module and initialize the master node
        """
        # set default values and update according to user input (config)
        # NOTE: input files should be prepared in the user module
        # (e.g. split the BIG file into smaller chunks using 'split')
        # each file is fed into a mapper, supposing it can fit into mapper's memory

        assert hasattr(config, 'mapfn') and hasattr(config, 'reducefn')
        self.config = {'nReduce':1, 'nMap':1, 'maxLoop':1, 'appendReduce':True,\
                'scratchFolder':'./', 'readPickle':False, 'writePickle':False,\
                'verbosity':6, 'timeout':60, 'delay':0.2, 'jobwait':1,\
                'mapfn':config.mapfn, 'reducefn':config.reducefn, 'ctrlfn':None,\
                'finalfn':None, 'readfn':None, 'hashfn':hash }

        if world.size == 1:
            raise AttributeError('Parallel mode only! At least one worker node is required.')

        # number of mapping tasks by default equals number of initial files
        # it can be overidden by user input
        assert isinstance(config.initFiles, list)
        self.config['nMap'] = len(config.initFiles)
        self.initFiles = config.initFiles

        # read in user defined configurations
        for key, val in self.config.items():
            if hasattr(config, key): self.config[key] = getattr(config, key)

        # sync config with all nodes
        self.config = world.bcast(self.config, root=0)

        # setup workers into a priority queue
        self.workers = [ State(ii) for ii in range(1, world.size) ]
        heapq.heapify(self.workers)
        self.nActive =  world.size - 1

        # assign map / reduce / finalize file list
        tmpList = [ config.__name__+'_'+str(ii).zfill(len(str(self.config['nMap'])))\
                +'.map' for ii in range(1, self.config['nMap']+1) ]
        self.mapIn = [ os.path.join(self.config['scratchFolder'], file) for file in tmpList ]

        tmpList = [ config.__name__+'_'+str(ii).zfill(len(str(self.config['nReduce'])))\
                +'.int' for ii in range(1, self.config['nReduce']+1) ]
        self.reduceIn = [ os.path.join(self.config['scratchFolder'], file) for file in tmpList ]
        self.reduceOut = [ os.path.splitext(file)[0]+'.red' for file in self.reduceIn ]

        # Currently only support single output file
        self.finalOut = [ config.__name__+'.out' ]

        # count number of iterations
        self.nLoop = 0; self.init = True

    def run(self):
        """Running sequence of the master node
        """
        atimer = Timer('Master')

        while self.iterCtrl():
            # map phase
            if self.init:
                self.execTask('Init')
                self.init = False
            else:
                self.execTask('Map')
            # reduce phase
            self.execTask('Reduce')

        # terminate all workers
        for ii in range(1, len(self.workers)+1):
            world.send(True, dest=ii, tag=TERMINATE)

        # final output, serial execution only
        self.finalize()

    def iterCtrl(self):
        """Generate map/reduce file lists and do loop control
        Return True to continue loop
        """
        # remove all temporary files used by mapper
        for file in glob.glob( os.path.join(self.config['scratchFolder'], '*.map') ):
            os.unlink(file)
        for file in glob.glob( os.path.join(self.config['scratchFolder'], '*-tmp*') ):
            os.unlink(file)

        self.nLoop += 1
        if self.nLoop > self.config['maxLoop']: return False

        converged = False
        if self.config['ctrlfn']:
            converged = self.config['ctrlfn'](self)
        elif self.nLoop == 1:
            # synchronize configuration file and file lists with worker nodes
            # for the 1st time execution only
            self.mapIn = self.initFiles
            self.sync('Map'); self.sync('Reduce')
        # if ctrlfn is specified, hand over all power
        else:
            raise ValueError('The ctrlfn is needed for correct looping behavior. Exiting...')

        return not converged

    def sync(self, key):
        """Synchronize dataType with all worker nodes.
        """
        tagDict = { 'Map': (UPDATE_MAP, self.mapIn),\
                'Reduce': (UPDATE_REDUCE, self.reduceIn),\
                'Config': (UPDATE_CONFIG, self.config) }

        for ii in range(1, len(self.workers)+1):
            world.send(tagDict[key][1], dest=ii, tag=tagDict[key][0])


    def finalize(self):
        """Combine all reducing files if necessary
        """
        if self.config['finalfn']:
            self.config['finalfn'](self.reduceOut, self.finalOut)
        else: # By default, output in ASCII format
            with open(self.finalOut[0], 'w') as fout:
                for filename in self.reduceOut:
                    with open(filename, 'rb') as fin:
                        data = pickle.load(fin)
                        for key, val in data.items():
                            fout.write('{0} ## {1}\n'.format(str(key), str(val)))

    def execTask(self, task):
        """Wrapper function calling mapping/reducing/finalizing phase tasks,
        dispatch tasks to workers until all finished and collect feedback. 
        Faulty workers are removed from active duty work list.
        """
        atimer = Timer(task)
        print( 'Entering {0:s} phase...'.format(task) )

        taskDict = { 'Map':(self.mapIn, MAP_START, MAP_FINISH), \
                'Init':(self.mapIn, INIT_START, MAP_FINISH), \
                'Reduce':(self.reduceIn, REDUCE_START, REDUCE_FINISH) }

        # line up jobs and workers into priority queues
        jobs = taskDict[task][0][:]
        heapq.heapify(jobs); running = {}
        heapq.heapify(self.workers)

        while (jobs or running) and self.nActive > 0:
            # dispatch all jobs to all free workers
            while jobs and self.workers[0].isFree():
                job = heapq.heappop(jobs)
                worker = heapq.heappop(self.workers)
                world.send(job, dest=worker.id, tag=taskDict[task][1])
                worker.setBusy(); heapq.heappush(self.workers, worker)
                running[job] = (time(), worker)
                if self.config['verbosity'] >= 6:
                    print('Dispatching file '+os.path.basename(job)+' to worker '+str(worker.id))
                # if no more free workers, break
                if not self.workers[0].isFree(): break

            # wait for finishing workers as well as do cleaning
            self.wait(running, taskDict[task][2])
            self.clean(running, jobs)

        print( '{0:s} phase completed'.format(task) )

    def wait(self, running, tag):
        """Test if any worker has finished its job.
        If so, decrease its key and make it available
        """
        atimer = Timer('Wait')

        inittime = time()
        status = MPI.Status()
        while time() - inittime < self.config['jobwait']:
            if world.Iprobe(source=MPI.ANY_SOURCE,tag=tag,status=status):
                jobf = world.recv(source=status.source, tag=tag)
                idx = 0
                for ii, worker in enumerate(self.workers):
                    if worker.id == status.source: idx = ii; break
                if self.config['verbosity'] >= 8:
                    print('Freeing worker '+str(self.workers[idx].id))
                worker = self.workers[idx]

                # faulty worker's job has already been cleaned
                if not worker.isFaulty():
                    del running[jobf]
                else:
                    self.nActive += 1
                worker.setFree()
                heapq._siftup(self.workers, idx)

    def clean(self, running, queue):
        """if the running time is too long, reassign the job
        and mark the worker permanently failed
        """
        atimer = Timer('Clean')

        if len(running) == 0: return
        for job, val in running.items():
            if time() - val[0] > self.config['timeout']:
                if self.config['verbosity'] >= 6:
                    print('Time out, marking worker '+str(val[1].id)+' faulty')
                heapq.heappush(queue, job)
                del running[job]
                idx = self.workers.index(val[1])
                self.workers[idx].setFaulty()
                heapq._siftdown(self.workers, 0, idx)
                self.nActive -= 1


class Worker(object):
    """Worker functions both as mapper and reducer,
    depending on different phases it is in.
    """
    def __init__(self):
        """Obtain configurations and all filelists from the master node
        """
        assert world.rank >= 1

        # synchronize configuration file and file lists with the master node
        self.config = {}; self.mapIn = []; self.reduceIn = []; self.reduceOut = []
        self.config = world.bcast(self.config, root=0)

    def run(self):
        """Receiving job instructions from the master node until
        TERMINATE signal received. Allowed tasks are defined in taskDict
        """
        atimer = Timer('Worker')

        # tasks define signal-behavior in the run function
        taskDict = { MAP_START: self.map, REDUCE_START: self.reduce,\
                INIT_START: self.map,\
                UPDATE_MAP: self.update, UPDATE_REDUCE: self.update,\
                UPDATE_CONFIG: self.update }

        status = MPI.Status()
        while True:
            # ping input
            if not world.Iprobe(source=0, tag=MPI.ANY_TAG, status=status):
                sleep(self.config['delay']);

            # entire calculation finished
            elif status.tag == TERMINATE:
                term = world.recv(source=0, tag=TERMINATE); break

            # check allowed tasks
            elif status.tag in taskDict:
                taskDict[status.tag](status.tag);

            # no instruction found, looping
            else:
                sleep(self.config['delay'])

    def update(self, tag):
        """Update file list and global configurations
        """
        atimer = Timer('Worker_Update')

        if tag == UPDATE_MAP:
            self.mapIn = world.recv(source=0, tag=tag)
        elif tag == UPDATE_REDUCE:
            self.reduceIn = world.recv(source=0, tag=tag)
            self.reduceOut = [ os.path.splitext(file)[0]+'.red' for file in self.reduceIn ]
        elif tag == UPDATE_CONFIG:
            self.config = world.recv(source=0, tag=tag)
        else:
            raise ValueError('Wrong tag specified.')

    def read(self, filename, tag):
        """Read initial data for mapper. 
        By default key and value pairs are in string format.
        The read function will be overloaded if config.readfn is supplied.
        """
        atimer = Timer('Worker_Read')

        # Pickle format
        if tag != INIT_START or self.config['readPickle']:
            with open(filename, 'rb') as fin:
                return pickle.load(fin)
        # ASCII mode
        with open(filename, 'r') as fin:
            if self.config['readfn']: return self.config['readfn'](fin)
            # default reader
            data = {}
            for line in fin:
                line = line.split(' ')
                data[line[0]] = ' '.join(line[1:])
            return data

    def map(self, tag):
        """
        Execute supplied mapfn on each key-value pair read from file
        assigned by the master node
        """
        atimer = Timer('Worker_Map')

        # load key-value pairs from filename
        filename = world.recv(source=0, tag=tag)
        data = self.read(filename, tag)

        buffer = [ [] for ii in range(self.config['nReduce']) ]
        for key, val in data.items():
            for newKey, newVal in self.config['mapfn'](key, val):
                idx = self.config['hashfn'](newKey) % self.config['nReduce']
                buffer[idx].append( (newKey, newVal) )

        # write out new key-value pairs in scattered files
        for ii in range(self.config['nReduce']):
            tmpfile = self.reduceIn[ii]+'-tmp'+str(world.rank)
            # dump in append mode
            with open(tmpfile, 'a+b') as fout:
                pickle.dump(buffer[ii], fout, pickle.HIGHEST_PROTOCOL)

        # report back as successful completion of task
        world.send(filename, dest=0, tag=MAP_FINISH)

    def reduce(self, tag):
        """Use supplied reducefn to operate on
        a list of values from a given key, generated by self.map()
        """
        atimer = Timer('Worker_Reduce')

        filename = world.recv(source=0, tag=tag)
        files = glob.glob(filename+'-tmp*')
        dataList = []
        for file in files:
            with open(file, 'rb') as fin:
                try:
                    while True: dataList.extend( pickle.load(fin) )
                except EOFError: # read in every instance of pickle dump
                    pass
        data = {}
        for key, val in dataList:
            if key in data:
                data[key].append(val)
            else:
                data[key] = [val]
        results = []
        for key, values in data.items():
            results.append( ( key, self.config['reducefn'](key, values) ) )
        results.sort(key=itemgetter(0))

        # write out in dictionary format
        idx = self.reduceIn.index(filename)
        if self.config['appendReduce']:
            with open(self.reduceOut[idx], 'a+') as fout:
                pickle.dump(dict(results), fout, pickle.HIGHEST_PROTOCOL)
        else:
            with open(self.reduceOut[idx], 'w+') as fout:
                pickle.dump(dict(results), fout, pickle.HIGHEST_PROTOCOL)

        world.send(filename, dest=0, tag=REDUCE_FINISH)

def main(filename):
    """Main routine of MR, can be called as module function
    """
    if world.rank == 0: # master node
        config = __import__(filename)
        master = Master(config)
        master.run()
    else: # worker node
        worker = Worker()
        worker.run()

    timer.finalize()

if __name__ == '__main__':
    if len(sys.argv) <= 1 or not os.path.isfile(sys.argv[1]):
        if world.rank == 0:
            print('Usage: \n     '+os.path.basename(sys.argv[0])+' config.in')
        sys.exit(1)
    filename = os.path.splitext(os.path.basename(sys.argv[1]))[0]
    main(filename)
