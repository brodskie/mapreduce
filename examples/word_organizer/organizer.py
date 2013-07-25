"""Compute how many times every term occurs across titles, for each author
in a set of given files"""

# Parallel version in map-reduce
import os.path, os, sys

def readfn(fin):
    data = {}
    for line in fin:
        line = line.split(':::')
        assert len(line) == 3
        data[line[0]] = (line[1], line[2]) # key - title; value - authors
    return data

def mapfn(key, val):
    authors = val[0].split('::')
    title = val[1]
    for auth in authors:
        yield auth, title

def reducefn(auth, titlelst):
    from stopwords import allStopWords as sw
    import re
    kword = {}
    tmp = ' '.join(titlelst)
    tmp = tmp.lower().strip()
    tmp = re.sub('(\.|\s[a-z]\s|,)', ' ', tmp)
    for key in tmp.split():
        if key in sw: continue
        key = key.replace('-',' ')
        if key in kword:
            kword[key] += 1
        else:
            kword[key] = 1
    return kword

# import data and represent it as key-val pairs
p = os.path.abspath('./dataset')
inlst = [os.path.join(p,ifile) for ifile in os.listdir(p) if os.path.isfile(os.path.join(p,ifile))]
initFiles = inlst
verbosity = 6
nReduce = 1
scratchFolder = './scratch'
