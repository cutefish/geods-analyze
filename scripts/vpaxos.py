import random
import sys

import numpy as np
import scipy as sp
import matplotlib
matplotlib.use('pdf')
import matplotlib.pylab as plt

from model.ddist import DDist
from model.protocol import getSLPLatencyDist
from model.protocol import getEPLatencyDist
from model.protocol import getFPLatencyDist

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
    try:
        key, mean, cfg = config
    except:
        key, mean = config
        cfg = {}
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
    splKeys = ['rtrip', 'odelay', 'ratio']
    epData = {}
    epKeys = ['rtrip', 'odelay', 'ratio']
    fpData = {}
    fpKeys = ['eN', 'ratio']
    resData = {}
    resKeys = ['spl', 'ep', 'fp']
    for k in splKeys:
        splData[k] = DataPoints()
    for k in epKeys:
        epData[k] = DataPoints()
    for k in fpKeys:
        fpData[k] = DataPoints()
    for k in resKeys:
        resData[k] = {}
        resData[k]['m'] = DataPoints()
        resData[k]['s'] = DataPoints()
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
            splData['rtrip'].y.append(rtripS / rtripM.mean)
            splData['odelay'].x.append(arrmean)
            splData['odelay'].y.append(odelayS / odelayM.mean)
            splData['ratio'].x.append(arrmean)
            splData['ratio'].y.append(resS / resM.mean)
            print 'slp', resS / resM.mean
            resData['spl']['s'].x.append(arrmean)
            resData['spl']['s'].y.append(resS)
            resData['spl']['m'].x.append(arrmean)
            resData['spl']['m'].y.append(resM)
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
            epData['rtrip'].y.append(rtripS / rtripM.mean)
            epData['odelay'].x.append(arrmean)
            epData['odelay'].y.append(odelayS / odelayM.mean)
            epData['ratio'].x.append(arrmean)
            epData['ratio'].y.append(resS / resM.mean)
            print 'ep', resS / resM.mean, resS
            resData['ep']['s'].x.append(arrmean)
            resData['ep']['s'].y.append(resS)
            resData['ep']['m'].x.append(arrmean)
            resData['ep']['m'].y.append(resM)
        elif 'fpdetmn' in config['system.impl']:
            factor = config['arrfactor']
            print factor
            print ('networkcfg=%s, n=%s, arr.per.node.mean=%s'
                   %(networkcfg, n, arrmean))
            #model
            try:
                eNM, resM = getFPLatencyDist(n, ddist, lambd)
            except ValueError as e:
                print e
                continue
            #sim
            eNS = result['load.mean']
            frateS = result['paxos.propose.fail.ratio']
            ntriesS = result['paxos.ntries.time.mean']
            ftimeS = result['paxos.propose.fail.time.mean']
            crateS = result['paxos.collision.ratio']
            stimeS = result['paxos.propose.succ.time.mean']
            ttimeS = result['paxos.propose.total.time.mean']
            odelayS = result['order.consensus.time.mean']
            resS = result['res.mean']
            #print ('arrmean=%s, ncfg=%s, frate=%s, ntries=%s, ftime=%s, '
            #       'crate=%s, stime=%s, ttime=%s, odelay=%s, res=%s'
            #       %(arrmean, networkcfg, frateS, ntriesS, ftimeS,
            #         crateS, stimeS, ttimeS, odelayS, resS))
            #print

            #data
            fpData['eN'].x.append(factor)
            fpData['eN'].y.append(eNS / eNM)
            fpData['ratio'].x.append(factor)
            fpData['ratio'].y.append(resS / resM)
            print resS / resM
            resData['fp']['s'].x.append(factor)
            resData['fp']['s'].y.append(resS)
            resData['fp']['m'].x.append(factor)
            resData['fp']['m'].y.append(resM)
        else:
            continue
    for key, val in splData.iteritems():
        if len(val.x) == 0 and len(val.y) == 0:
            continue
        fig = plt.figure()
        axes = fig.add_subplot(111)
        #axes.plot(val.x, val.y, '.r')
        axes.plot(val.y, '.r')
        fig.savefig('tmp/%s_%s.pdf'%('vpaxos_vspl', key))
    for key, val in epData.iteritems():
        if len(val.x) == 0 and len(val.y) == 0:
            continue
        fig = plt.figure()
        axes = fig.add_subplot(111)
        #axes.plot(val.x, val.y, '.r')
        axes.plot(val.y, '.r')
        fig.savefig('tmp/%s_%s.pdf'%('vpaxos_vep', key))
    for key, val in fpData.iteritems():
        if len(val.x) == 0 and len(val.y) == 0:
            continue
        fig = plt.figure()
        axes = fig.add_subplot(111)
        axes.plot(val.x, val.y, '.r')
        fig.savefig('tmp/%s_%s.pdf'%('vpaxos_vfp', key))
    for key, val in resData.iteritems():
        s = val['s']
        m = val['m']
        if len(s.x) == 0 and len(s.y) == 0:
            continue
        fig = plt.figure()
        axes = fig.add_subplot(111)
        axes.plot(s.x, s.y, 'r.')
        axes.plot(m.x, m.y, 'b+')
        fig.savefig('tmp/%s_%s.pdf'%('vpaxos_res', key))

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
