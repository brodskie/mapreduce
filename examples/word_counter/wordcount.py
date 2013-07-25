### triangular.py
#server
import pickle
import random
import os
from time import sleep

#system param
scratchFolder = './scratch'
nReduce = 5
timeout = 15
nFinal = 1

splitsize = '5000'
datafile = 'lordoftherings.txt'
#splitsize = '1'
#datafile = 'test.txt'

os.system('cd '+scratchFolder+'; rm -f *; split -l '+splitsize+' ../'+datafile)
initFiles = [scratchFolder+'/'+file for file in os.listdir(scratchFolder)]
inlst = initFiles

# client
def mapfn(key, value):
    yield key, value

def readfn(fin):
    data = {}
    for line in fin:
        for word in line.split():
            if word in data:
                data[word] += 1
            else:
                data[word] = 1
    return data

def reducefn(key, values):
    return sum(values)
