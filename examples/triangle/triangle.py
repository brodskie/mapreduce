### triangular.py
#server
import pickle
import random
from time import sleep

source = dict(zip(range(100), range(100)))
inlst = []
for ii in range(10):
    fname = 'tri'+str(ii)+'.dat'
    inlst.append(fname)
    pickle.dump(source, open(fname,'wb'))
scratch = './scratch'
verb = 8
pickle = False
nred = 1
sleeptime=0.5
timeout=15

# client
def mapfn(key, value):
    for i in range(value + 1):
        yield key, i

def reducefn(key, value):
    return sum(value)
