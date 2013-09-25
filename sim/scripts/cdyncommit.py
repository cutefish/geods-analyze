
import random
import sys

import numpy as np
import scipy as sp
import matplotlib
matplotlib.use('pdf')
import matplotlib.pylab as plt

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

def approx(params):
    data = {}
    dataKeys = ['pc', 'pd', 'w', 'res', 'load']
    for k in dataKeys:
        data[k] = DataPoints()
    for i, param in enumerate(params):
        config, result = param
        m = config['max.num.txns.in.system']
        k = config['nwrites']
        n = config['dataset.groups'][1]
        s = config['intvl']
        c = config['commit.time']
        gamma = float(c) / s
        if config['system.impl'] == 'impl.cdylock.CentralDyLockSystem':
            #model
            pcM = float(m - 1) / n * k / 2 * (k - 1 + 2 * gamma) / (k + gamma)
            pwM = 1 - (1 - pcM)**k
            pdM = float(m - 1) * k**4 / 12 / n**2
            w1M = float(k - 1) / 3 * s + s + gamma * s
            nc = k * pcM
            A = w1M / s / k
            alpha = nc * A
            wM = w1M + 0.5 * alpha * w1M + 1.5 * alpha**2 * w1M
            resM = (k * s + gamma * s + k * pcM * wM) * (1 + 2 * pdM)
            #real
            pcR = result['lock.block.prob']
            pdR = result['abort.deadlock.prob']
            wR = result['lock.block.time.mean']
            resR = result['res.mean']
            lR = result['load.mean']
            #data
            data['pc'].x.append(pcR)
            data['pc'].y.append(pcR / pcM)
            data['pd'].x.append(pcR)
            data['pd'].y.append(pdR / pdM)
            data['w'].x.append(pcR)
            data['w'].y.append(wR / wM)
            data['res'].x.append(pcR)
            data['res'].y.append(resR / resM)
            data['load'].x.append(pcR)
            data['load'].y.append(lR / m)
        else:
            continue
    for key, val in data.iteritems():
        fig = plt.figure()
        axes = fig.add_subplot(111)
        axes.plot(val.x, val.y, '.r')
        fig.savefig('%s_%s.pdf'%('ndetc', key))

def main():
    if len(sys.argv) != 2:
        print 'ndetmncommit <result file>'
        sys.exit()
    params = readparams(sys.argv[1])
    approx(params)

if __name__ == '__main__':
    main()
