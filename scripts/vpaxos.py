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
        DDists[ddistkey] = DDist.sample(config, h=0.5)
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

def validate(params):
    splData = {}
    splKeys = ['rtrip', 'odelay', 'res']
    epData = {}
    epKeys = ['rtrip', 'odelay', 'res']
    for k in splKeys:
        splData[k] = DataPoints()
    for k in epKeys:
        epData[k] = DataPoints()
    for i, param in enumerate(params):
        config, result = param
        networkcfg = config['nw.latency.cross.zone']
        ddist = getDDist(networkcfg)
        n = config['num.zones']
        arrdist = config['txn.arrive.interval.dist']
        arrkey, arrmean = arrdist
        lambd = 1.0 / arrmean * n
        if 'slpdetmn' in config['system.impl']:
            #model
            resM, odelayM, rtripM = getSLPLatencyDist(n, ddist, lambd)
            #sim
            resS = result['res.mean']
            odelayS = result['order.consensus.time.mean']
            rtripS = result['paxos.propose.total.time.mean']
            #data
            splData['rtrip'].x.append(arrmean)
            splData['rtrip'].y.append(rtripM.mean / rtripS)
            splData['odelay'].x.append(arrmean)
            splData['odelay'].y.append(odelayM.mean / odelayS)
            splData['res'].x.append(arrmean)
            splData['res'].y.append(resM.mean / resS)
            print 'slp', resM.mean /resS
        elif 'epdetmn' in config['system.impl']:
            #model
            elen = config['epdetmn.epoch.length']
            skewcfg = config['epdetmn.epoch.skew.dist']
            skewdist = getDDist(skewcfg)
            resM, odelayM, rtripM = getEPLatencyDist(n, ddist, skewdist, elen)
            #sim
            resS = result['res.mean']
            odelayS = result['order.consensus.time.mean']
            rtripS = result['paxos.propose.total.time.mean']
            #data
            epData['rtrip'].x.append(arrmean)
            epData['rtrip'].y.append(rtripM.mean / rtripS)
            epData['odelay'].x.append(arrmean)
            epData['odelay'].y.append(odelayM.mean / odelayS)
            epData['res'].x.append(arrmean)
            epData['res'].y.append(resM.mean / resS)
            print 'ep', resM.mean /resS
        else:
            continue
    for key, val in splData.iteritems():
        fig = plt.figure()
        axes = fig.add_subplot(111)
        axes.plot(val.x, val.y, '.r')
        fig.savefig('tmp/%s_%s.pdf'%('vpaxos_vspl', key))
    for key, val in epData.iteritems():
        fig = plt.figure()
        axes = fig.add_subplot(111)
        axes.plot(val.x, val.y, '.r')
        fig.savefig('tmp/%s_%s.pdf'%('vpaxos_vep', key))

def compare(params):
    splData = {}
    splKeys = ['res']
    for k in splKeys:
        splData['%s.model'%k] = DataPoints()
        splData['%s.sim'%k] = DataPoints()
    for i ,param in enumerate(params):
        config, result = param
        networkcfg = config['nw.latency.cross.zone']
        netkey, netmean, netcfg = networkcfg
        ddist = getDDist(networkcfg)
        n = config['num.zones']
        arrdist = config['txn.arrive.interval.dist']
        arrkey, arrmean = arrdist
        lambd = 1.0 / arrmean * n
        if 'slpdetmn' in config['system.impl']:
            #model
            resM, odelayM, rtripM = getSLPLatencyDist(n, ddist, lambd)
            print n, networkcfg, arrmean, resM.mean
            #sim
            resS = result['res.mean']
            #data
            splData['res.model'].x.append(arrmean)
            splData['res.model'].y.append(resM.mean)
            splData['res.sim'].x.append(arrmean)
            splData['res.sim'].y.append(resS)
            print resM.mean /resS
        else:
            continue
    for key in splKeys:
        fig = plt.figure()
        axes = fig.add_subplot(111)
        val = splData['%s.model'%k]
        axes.plot(val.x, val.y, '.r')
        val = splData['%s.sim'%k]
        axes.plot(val.x, val.y, '+b')
        fig.savefig('tmp/%s_%s.pdf'%('vpaxos_compare', key))

def main():
    if len(sys.argv) != 2:
        print 'vpaxos <result file>'
        sys.exit()
    params = readparams(sys.argv[1])
    validate(params)
    #compare(params)

if __name__ == '__main__':
    main()
