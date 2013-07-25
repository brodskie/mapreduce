fin = open('lordoftherings.txt', 'r')
data = {}
for line in fin:
    for word in line.split():
        if word in data:
            data[word] += 1
        else:
            data[word] = 1
fout = open('compare.out', 'w+')
for key, val in data.items():
    fout.write('{0} ## {1}\n'.format(key, val))
fout.close()

