import unittest, random, sys, glob, os
if sys.version_info[0] <= 2:
    import cPickle as pickle
else:
    import pickle

nFinal = 1
dataFolder = './data'
scratchFolder = './scratch'
timeout = 600
readPickle = True
writePickle = True

initFiles = glob.glob(os.path.join(dataFolder, 'data_*.dat'))
inlst = initFiles

def hashfn(key):
    return int( float(key) / sys.maxsize * nReduce )

def mapfn(key, value):
    yield key, value

def reducefn(key, values):
    return sum(values)

