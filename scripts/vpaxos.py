import random
import sys

import numpy as np
import scipy as sp
import matplotlib
matplotlib.use('pdf')
import matplotlib.pylab as plt

from model.ddist import DDist
from model.protocol import getSLPLatencyDist

DDists = { }

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

def getDDist(config):
    key, mean, cfg = config
    strings = []
    strings.append(str(key))
    strings.append(str(mean))
    for key, val in cfg.iteritems():
        strings.append('%s=%s'%(key, val))
    ddistkey = ' '.join(strings)
    if ddistkey not in DDists:
        DDists[ddistkey] = DDist.sample(config, h=1)
    return DDists[ddistkey]

keys = [
    'paxos.propose.total.time',
    'paxos.propose.fail.time',
    'paxos.propose.succ.time',
    'order.consensus.time',
    'res',
]

class DataPoints(object):
    def __init__(self):
        self.x = []
        self.y = []

def dowork(params):
    splData = {}
    splKeys = ['rtrip', 'odelay', 'res']
    for k in splKeys:
        splData[k] = DataPoints()
    for i, param in enumerate(params):
        config, result = param
        networkcfg = config['nw.latency.cross.zone']
        netkey, netmean, netcfg = networkcfg
        ddist = getDDist(networkcfg)
        n = config['num.zones']
        arrdist = config['txn.arrive.interval.dist']
        key, mean = arrdist
        lambd = 1.0 / mean / n
        if 'slpdetmn' in config['system.impl']:
            #model
            resM, odelayM, rtripM = getSLPLatencyDist(n, ddist, lambd)
            #sim
            resS = result['res.mean']
            odelayS = result['order.consensus.time.mean']
            rtripS = result['paxos.propose.total.time.mean']
            #data
            splData['rtrip'].x.append(netcfg['sigma'])
            splData['rtrip'].y.append(rtripM.mean / rtripS)
            splData['odelay'].x.append(netcfg['sigma'])
            splData['odelay'].y.append(odelayM.mean / odelayS)
            splData['res'].x.append(netcfg['sigma'])
            splData['res'].y.append(resM.mean / resS)
            print resM.mean /resS
        else:
            continue
    for key, val in splData.iteritems():
        fig = plt.figure()
        axes = fig.add_subplot(111)
        axes.plot(val.x, val.y, '.r')
        fig.savefig('tmp/%s_%s.pdf'%('vpaxos', key))

def main():
    if len(sys.argv) != 2:
        print 'vpaxos <result file>'
        sys.exit()
    params = readparams(sys.argv[1])
    dowork(params)

if __name__ == '__main__':
    main()
