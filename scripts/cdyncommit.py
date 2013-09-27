
import random
import sys

import numpy as np
import scipy as sp
import matplotlib
matplotlib.use('pdf')
import matplotlib.pylab as plt

from model.execute import calcNDetmnExec, calcDetmnExec

def readparams(rfile):
    fh = open(rfile, 'r')
    params = []
    while True:
        line = fh.readline()
        if line == '':
            break
        line = line.strip()
        if line == '':
            continue
        if line.startswith('#'):
            continue
        key, val = line.split('=')
        val = val.strip()
        params.append(eval(val))
    fh.close()
    return params

class DataPoints(object):
    def __init__(self):
        self.x = []
        self.y = []

def findIndex(n, l):
    index = len(l)
    mindiff = n
    for i, v in enumerate(l):
        if abs(v - n) < mindiff:
            index = i
            mindiff = abs(v - n)
    return index

def compare(params):
    data = {}
    data['model'] = DataPoints()
    data['sim'] = DataPoints()
    cshift = 5 * 5 * 5 * 5
    nshift = 5 * 5 * 5
    mshift = 5 * 5
    kshift = 5
    sshift = 1
    cindex = [0.1, 10, 20, 30, 50, 70, 90]
    nindex = [1024, 4096, 16384]
    mindex = [12, 16, 20]
    kindex = [8, 12, 16]
    sindex = [10, 20, 30]
    for i, param in enumerate(params):
        config, result = param
        if config['system.impl'] != 'impl.cdylock.CentralDyLockSystem':
            continue
        m = config['max.num.txns.in.system']
        k = config['nwrites']
        n = config['dataset.groups'][1]
        s = config['intvl']
        c = config['commit.time']
        g = float(c) / s
        index = findIndex(c, cindex) * cshift + \
                findIndex(s, sindex) * sshift + \
                findIndex(k, kindex) * kshift + \
                findIndex(m, mindex) * mshift + \
                findIndex(n, nindex) * nshift
        #model
        pcM, pdM, wM, resM, beta = calcNDetmnExec(n, m, k, s, c)
        #real
        resR = result['res.mean']
        #data
        data['model'].x.append(g)
        data['model'].y.append(resM)
        data['sim'].x.append(g)
        data['sim'].y.append(resR)
    fig = plt.figure()
    axes = fig.add_subplot(111)
    axes.plot(data['sim'].x, data['sim'].y, '.r')
    axes.plot(data['model'].x, data['model'].y, '+b')
    fig.savefig('tmp/ndetc_compare.pdf')

def approx(params):
    data = {}
    dataKeys = ['pc', 'pd', 'w', 'res', 'load']
    for k in dataKeys:
        data[k] = DataPoints()
    nshift = 10 * 5 * 5 * 5
    mshift = 10 * 5 * 5
    kshift = 10 * 5
    sshift = 10
    cshift = 1
    nindex = [1024, 4096, 16384]
    mindex = [12, 16, 20]
    kindex = [8, 12, 16]
    sindex = [10, 20, 30]
    cindex = [0.1, 10, 20, 30, 50, 70, 90]
    for i, param in enumerate(params):
        config, result = param
        m = config['max.num.txns.in.system']
        k = config['nwrites']
        n = config['dataset.groups'][1]
        s = config['intvl']
        c = config['commit.time']
        gamma = float(c) / s
        index = findIndex(c, cindex) * cshift + \
                findIndex(s, sindex) * sshift + \
                findIndex(k, kindex) * kshift + \
                findIndex(m, mindex) * mshift + \
                findIndex(n, nindex) * nshift
        if config['system.impl'] == 'impl.cdylock.CentralDyLockSystem':
            #model
            pcM, pdM, wM, resM, beta = calcNDetmnExec(n, m, k, s, c)
            #real
            try:
                pcR = result['lock.block.prob']
                pdR = result['abort.deadlock.prob']
                wR = result['lock.block.time.mean']
                resR = result['res.mean']
                lR = result['load.mean']
            except:
                print i
                raise
            #data
            data['pc'].x.append(beta)
            data['pc'].y.append(pcR / pcM)
            data['pd'].x.append(beta)
            data['pd'].y.append(pdR / pdM)
            data['w'].x.append(beta)
            data['w'].y.append(wR / wM)
            data['res'].x.append(beta)
            data['res'].y.append(resR / resM)
            data['load'].x.append(beta)
            data['load'].y.append(lR / m)
        else:
            continue
    for key, val in data.iteritems():
        fig = plt.figure()
        axes = fig.add_subplot(111)
        axes.plot(val.x, val.y, '.r')
        fig.savefig('tmp/%s_%s.pdf'%('ndetc', key))

def main():
    if len(sys.argv) != 2:
        print 'ndetmncommit <result file>'
        sys.exit()
    params = readparams(sys.argv[1])
    compare(params)
    approx(params)

if __name__ == '__main__':
    main()
