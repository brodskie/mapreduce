import os, sys
from random import randint
from sys import maxsize
if sys.version_info[0] <= 2:
    import cPickle as pickle
else:
    import pickle

scratchFolder = './scratch'
dataFolder = './data'
nReduce = 1
timeout = 999
jobwait = 10
nFinal = 1
writePickle = True

#create temporary input files for initial mapper
totalSize = 20000000
nFile = 10
base = 'data_'; initFiles = []
for ii in range(nFile):
    filename = os.path.join( scratchFolder, 'mapinput'+str(ii)+'.in' )
    initFiles.append(filename)
    with open(filename, 'w') as fout:
        outname = os.path.join( dataFolder, base+str(ii).zfill(len(str(nFile)))+'.dat')
        fout.write('{0:s} {1:d}\n'.format(outname, totalSize//nFile))
inlst = initFiles

def mapfn(filename, size):
    data = {}
    for ii in range(size):
        key = randint(0, maxsize-1) 
        if key in data:
            data[key] += 1
        else:
            data[key] = 1
    with open(filename, 'wb') as fout:
        pickle.dump(data, fout, pickle.HIGHEST_PROTOCOL)
    yield filename, True

def reducefn(key, values):
    return all(values)

def readfn(fin):
    """File format: filename, size
    """
    for line in fin:
        data = {}
        inlst = line.split()
        data[inlst[0]] = int(inlst[1])
        return data
    
