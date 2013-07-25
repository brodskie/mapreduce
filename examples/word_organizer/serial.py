"""Compute how many times every term occurs across titles, for each author
in a set of given files"""

import os.path, os, sys
# Serial version

def reader(filelst):
    if not isinstance(filelst, list):
        filelst = [filelst]
    data = {}
    for ifile in filelst:
        f = open(ifile, 'r')
        for line in f:
            line = line.split(':::')
            assert len(line) == 3
            data[line[0]] = (line[1], line[2]) # key - title; value - authors
    return data

def mapper(data):
    assert isinstance(data, dict)
    authdict = {}
    for key, (authors, title) in data.items():
        authors = authors.split('::')
        for auth in authors:
            if auth in authdict:
                authdict[auth].append(title)
            else:
                authdict[auth] = [title]
    return authdict

def reducer(authdict):
    from stopwords import allStopWords as sw
    import re
    assert isinstance(authdict, dict)
    outdict = {}
    for auth, titlelst in authdict.items():
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
        outdict[auth] = kword
    return outdict

def main(folder):
    p = os.path.abspath(folder)
    filelst = [os.path.join(p,ifile) for ifile in os.listdir(p) if os.path.isfile(os.path.join(p,ifile))]
    data = reader(filelst)
    authdict = mapper(data)
    outdict = reducer(authdict)
    results = []
    for key, value in outdict.items():
        from operator import itemgetter
        val = [(term, number) for term, number in value.items()]
        val.sort(key=itemgetter(1),reverse=True)
        results.append((key, val))
    results.sort()
    for ii in results:
        print(ii)

if __name__=='__main__':
    import timeit
    main(sys.argv[1])
