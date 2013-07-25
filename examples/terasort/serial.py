import unittest, random, sys
if sys.version_info[0] <= 2:
    import cPickle as pickle
else:
    import pickle

def serialAssert(filelst):
    """Assert the numbers in filelst are in non-decending order
    """
    assert isinstance(filelst, list)
    nfile = len(filelst)
    cmin = -1
    for ii in range(nfile):
        with open(filelst[ii], 'rb') as fin:
            data = pickle.load(fin)
        if data[0] < cmin: return False
        jj = 1
        while jj < len(data):
            if data[jj] < data[jj-1]: return False
            jj += 1
        cmin = data[-1]
    return True

def serialSort(filelst):
    assert isinstance(filelst, list)
    nfile = len(filelst)
    for ii in range(nfile):
        with open(filelst[ii], 'rb') as fin:
            data = pickle.load(fin)
        data.sort()
        with open(filelst[ii], 'wb') as fout:
            pickle.dump(data, fout)

def serialCreate(filelst, ntot, nmax=10**9):
    """Generate a large set of random integers in a list of files
    """
    assert isinstance(filelst, list)
    nfile = len(filelst)
    for ii in range(nfile):
        data = [ random.randint(0, nmax) for jj in range(ntot//nfile) ]
        with open(filelst[ii], 'wb') as fout:
            pickle.dump(data, fout, pickle.HIGHEST_PROTOCOL)

class TestGenAssert(unittest.TestCase):
    def atestCreate(self):
        ntotal, nmax = 10**5, 10**12
        serialCreate(inlst, ntotal, nmax)
    def testSort(self):
        outlst = serialSort(inlst)
    def testAssert(self):
        res = serialAssert(outlst)
        self.assertTrue(res)

if __name__=='__main__':
    unittest.main()
