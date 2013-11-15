import random
import sys

import numpy as np
import matplotlib
matplotlib.use('pdf')
import matplotlib.pylab as plt

from model.ddist import DDist
from model.execute import calcNDetmnExec
from model.execute import calcDetmnExec
from model.protocol import quorum
from model.protocol import getSLPLatencyDist
from model.protocol import getEPLatencyDist
from model.protocol import getFPLatencyDist
from model.system import calcNDetmnSystem
from model.system import calcDetmnSystem
from model.system import ExceedsCountMaxException
from model.system import NotConvergeException

matplotlib.rc('xtick', labelsize=16)
matplotlib.rc('ytick', labelsize=16)
matplotlib.rc('font', size=16)
matplotlib.rc('lines', markersize=10)

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

def getQuorum(n, f, ddist):
    if (n, f, ddist) not in DDists:
        DDists[(n, f, ddist)] = quorum(n, f, ddist)
    return DDists[(n, f, ddist)]

def getSLPL(z, ddist, lambd):
    if (z, ddist, lambd) not in DDists:
        DDists[(z, ddist, lambd)] = getSLPLatencyDist(z, ddist, lambd)[0].mean
    return DDists[z, ddist, lambd]

paxoskeys = [
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

    def add(self, x, y):
        self.x.append(x)
        self.y.append(y)

    def addY(self, y):
        self.x.append(len(self.x))
        self.y.append(y)

    def get(self, ymap=()):
        if len(ymap) == 0:
            return self.x, self.y
        else:
            retx = []
            rety = []
            index = {}
            #consolidate y values
            for i, x in enumerate(self.x):
                if x not in index:
                    index[x] = len(retx)
                    retx.append(x)
                    rety.append([])
                rety[index[x]].append(self.y[i])
            #apply y map
            retys = []
            for func in ymap:
                retys.append(map(func, rety))
            return retx, retys

def validate_nd(params):
    data = {}
    keys = ['ps', 'ws', 'res', 'load']
    for key in keys:
        data[key] = DataPoints()
    for i, param in enumerate(params):
        config, result = param
        if not 'cdylock' in config['system.impl']:
            continue
        m = config['max.num.txns.in.system']
        k = config['nwrites']
        n = config['dataset.groups'][1]
        s = config['intvl']
        kgratio = config['kgratio']
        g = k / kgratio
        #if n == 1024:
        #    continue
        #if n != 16384 or m != 12 or k != 8 or s != 10:
        #    continue
        #model
        psM, pdM, wsM, resM, beta = calcNDetmnExec(n, m, k, s, g)
        #sim
        psS = result['lock.block.prob']
        pdS = result['abort.deadlock.prob']
        wsS = result['lock.block.time.mean']
        resS = result['res.mean']
        lS = result['load.mean']
        #data
        def getError(m, s):
            if s == 0:
                return 0
            return (s - m) / s
        data['ps'].add(psS, getError(psM, psS))
        #data['pd'].add(beta, getError(pdM, pdS))
        data['ws'].add(psS, getError(wsM, wsS))
        data['res'].add(psS, getError(resM, resS))
        data['load'].add(psS, getError(m, lS))
    for key in keys:
        fig = plt.figure()
        axes = fig.add_subplot(111)
        x, y = data[key].get()
        axes.plot(x, y, '+')
        axes.set_xlabel('Probability of Blocking Per Step')
        if key == 'res':
            axes.set_ylabel('Error Rate')
            fig.savefig('tmp/validate_nd.pdf')
        else:
            fig.savefig('tmp/validate_nd_%s.pdf'%key)

def validate_de(params):
    data = {}
    keys = ['pt', 'a', 'h', 'wt', 'res', 'load']
    for key in keys:
        data[key] = DataPoints()
    for i, param in enumerate(params):
        config, result = param
        if not 'cdetmn' in config['system.impl']:
            continue
        m = config['max.num.txns.in.system']
        k = config['nwrites']
        n = config['dataset.groups'][1]
        s = config['intvl']
        #model
        ptM, aM, hM, wtM, resM, beta = calcDetmnExec(n, m, k, s)
        #sim
        ptS = result['lock.block.prob']
        aS = m - result['num.blocking.txns.mean']
        hS = result['block.height.cond.mean']
        wtS = result['lock.block.time.mean']
        lS = result.get('load.mean', m)
        resS = result['res.mean']
        #data
        def getError(m, s):
            if s == 0:
                return 0
            return (s - m) / s
        data['pt'].add(ptS, getError(ptM, ptS))
        data['a'].add(ptS, getError(aM, aS))
        data['h'].add(ptS, getError(hM, hS))
        data['wt'].add(ptS, getError(wtM, wtS))
        data['res'].add(ptS, getError(resM, resS))
        data['load'].add(ptS, getError(m, lS))
    for key in keys:
        fig = plt.figure()
        axes = fig.add_subplot(111)
        x, y = data[key].get()
        axes.plot(x, y, '+')
        axes.set_xlabel('Probability of Blocking Each Txn')
        if key == 'res':
            axes.set_ylabel('Error Rate')
            fig.savefig('tmp/validate_de.pdf')
        else:
            fig.savefig('tmp/validate_de_%s.pdf'%key)

def validate_sp(params):
    data = {}
    keys = ['rtrip', 'odelay', 'res']
    for key in keys:
        data[key] = DataPoints()
    for i, param in enumerate(params):
        config, result = param
        if not 'slpdetmn' in config['system.impl']:
            continue
        network = config['nw.latency.cross.zone']
        ddist = getDDist(network)
        n = config['num.zones']
        arrive = config['txn.arrive.interval.dist']
        arrkey, arrmean = arrive
        lambd = 1.0 / arrmean * n
        #model
        resM, odelayM, rtripM = getSLPLatencyDist(n, ddist, lambd)
        #sim
        rtripS = result['paxos.propose.total.time.mean']
        odelayS = result['order.consensus.time.mean']
        resS = result['res.mean']
        #data
        err = (rtripM.mean - rtripS)/rtripM.mean
        data['rtrip'].add(ddist.std, err)
        err = (odelayM.mean - odelayS)/odelayM.mean
        data['odelay'].add(ddist.std, err)
        err = (resM.mean - resS)/resM.mean
        data['res'].add(ddist.std, err)
        print i, err
    for key in keys:
        fig = plt.figure()
        axes = fig.add_subplot(111)
        x, y = data[key].get((np.average, min, max))
        yave, ymin, ymax = y
        axes.errorbar(x, yave, fmt='o',
                      yerr=[np.array(yave) - np.array(ymin),
                            np.array(ymax) - np.array(yave)])
        axes.set_ylabel('Error Rate')
        axes.set_xlabel('Network Latency Standard Deviation')
        axes.set_xlim([5, 50])
        if key == 'res':
            fig.savefig('tmp/validate_sp.pdf')
        else:
            fig.savefig('tmp/validate_sp_%s.pdf'%key)

def validate_ep(params):
    data = {}
    keys = ['rtrip', 'odelay', 'res']
    for key in keys:
        data[key] = DataPoints()
    for i, param in enumerate(params):
        config, result = param
        if not 'epdetmn' in config['system.impl']:
            continue
        network = config['nw.latency.cross.zone']
        ddist = getDDist(network)
        n = config['num.zones']
        arrive = config['txn.arrive.interval.dist']
        arrkey, arrmean = arrive
        lambd = 1.0 / arrmean * n
        elen = config['epdetmn.epoch.length']
        skewcfg = config['epdetmn.epoch.skew.dist']
        skewdist = getDDist(skewcfg)
        #model
        resM, odelayM, rtripM = getEPLatencyDist(n, ddist, skewdist, elen)
        #sim
        rtripS = result['paxos.propose.total.time.mean']
        odelayS = result['order.consensus.time.mean']
        resS = result['res.mean']
        #data
        err = (rtripM.mean - rtripS)/rtripM.mean
        data['rtrip'].add(ddist.std, err)
        err = (odelayM.mean - odelayS)/odelayM.mean
        data['odelay'].add(ddist.std, err)
        err = (resM.mean - resS)/resM.mean
        data['res'].add(ddist.std, err)
        print i, err
    for key in keys:
        fig = plt.figure()
        axes = fig.add_subplot(111)
        x, y = data[key].get((np.average, min, max))
        yave, ymin, ymax = y
        axes.errorbar(x, yave, fmt='o',
                      yerr=[np.array(yave) - np.array(ymin),
                            np.array(ymax) - np.array(yave)])
        axes.set_ylabel('Error Rate')
        axes.set_xlabel('Network Latency Standard Deviation')
        axes.set_xlim([5, 50])
        if key == 'res':
            fig.savefig('tmp/validate_ep.pdf')
        else:
            fig.savefig('tmp/validate_ep_%s.pdf'%key)


def validate_fp(params):
    data = {}
    data['res'] = DataPoints()
    for i, param in enumerate(params):
        config, result = param
        if not 'fpdetmn' in config['system.impl']:
            continue
        network = config['nw.latency.cross.zone']
        ddist = getDDist(network)
        n = config['num.zones']
        arrive = config['txn.arrive.interval.dist']
        arrkey, arrmean = arrive
        lambd = 1.0 / arrmean * n
        lambdaT = config['lambdaT']   #1 / \lambda T
        #model
        try:
            resM, eNM = getFPLatencyDist(n, ddist, lambd)
        except ValueError as e:
            print e
            continue
        #sim
        try:
            resS = result['res.mean']
        except:
            print i
            continue
        #data
        err = (resM - resS)/resM
        data['res'].add(lambdaT, err)
        print i, err
    fig = plt.figure()
    axes = fig.add_subplot(111)
    x, y = data['res'].get((np.average, min, max))
    yave, ymin, ymax = y
    axes.errorbar(x, yave, fmt='o',
                  yerr=[np.array(yave) - np.array(ymin),
                        np.array(ymax) - np.array(yave)])
    axes.set_xlim([0, 0.9])
    axes.set_ylabel('Error Rate')
    axes.set_xlabel(r'Arrival Rate $\times$ Average Round Trip Latency')
    fig.savefig('tmp/validate_fp.pdf')

def validate_ndsys(params):
    data = {}
    keys = ['res', 'm']
    data['res'] = DataPoints()
    data['m'] = DataPoints()
    for i, param in enumerate(params):
        config, result = param
        if not 'mstdylock' in config['system.impl']:
            continue
        network = config['nw.latency.cross.zone']
        ddist = getDDist(network)
        z = config['num.zones']
        arrive = config['txn.arrive.interval.dist']
        arrkey, arrmean = arrive
        lambd = 1.0 / arrmean * z
        txncfg = config['txn.classes'][0]
        n = config['dataset.groups'][1]
        k = txncfg['nwrites']
        s = txncfg['action.intvl.dist'][1]
        l = lambd
        f = int(np.ceil(z / 2.0) - 1)
        q = getQuorum(z, f, ddist).mean
        c = float(z - 1) / z * ddist.mean
        print n, k, s, l, ddist.mean
        #sim
        resS = result['res.mean']
        mS = result['load.mean']
        print resS, mS
        #model
        try:
            resM, mM, count, params = calcNDetmnSystem(n, k, s, l, q, c)
        except ExceedsCountMaxException as e:
            resM, mM, count = e.args
            print 'Exceeds COUNT_MAX, res=%s, m=%s, count=%s'%(resM, mM, count)
            continue
        except NotConvergeException as e:
            resM, mM, count = e.args
            print 'Not converge, res=%s, m=%s, count=%s'%(resM, mM, count)
            continue
        print resM, mM
        print k * s + q + c
        #data
        def getError(m, s):
            if s == 0:
                return 0
            return (s - m) / s
        data['res'].add(ddist.mean, resM)
        data['res'].add(ddist.mean, resS)
        #print getError(resM, resS)
        data['m'].add(ddist.mean, getError(mM, mS))
    def getM(array):
        return array[0]
    def getS(array):
        return array[1]
    for key in keys:
        fig = plt.figure()
        axes = fig.add_subplot(111)
        if key == 'res':
            x, y = data[key].get(ymap=(getM, getS))
            ym, ys = y
            axes.plot(x, ym, '+b-')
            axes.plot(x, ys, 'ro')
            axes.set_xlabel('Average Network Latency')
            axes.set_ylim([0, 350])
            axes.set_ylabel('Response Time')
            fig.savefig('tmp/validate_ndsys.pdf')

def validate_desys(params):
    data = {}
    data['res'] = DataPoints()
    data['m'] = DataPoints()
    keys = ['res', 'm']
    for i, param in enumerate(params):
        config, result = param
        if not 'slpdetmn' in config['system.impl']:
            continue
        network = config['nw.latency.cross.zone']
        ddist = getDDist(network)
        z = config['num.zones']
        arrive = config['txn.arrive.interval.dist']
        arrkey, arrmean = arrive
        lambd = 1.0 / arrmean * z
        txncfg = config['txn.classes'][0]
        n = config['dataset.groups'][1]
        k = txncfg['nwrites']
        s = txncfg['action.intvl.dist'][1]
        l = lambd
        p = getSLPL(z, ddist, lambd)
        print n, k, s, l, p
        #sim
        resS = result['res.mean']
        mS = result['load.mean']
        print 'de', 's', resS, mS
        #model
        try:
            resM, mM, count, params = calcDetmnSystem(n, k, s, l, p)
        except ExceedsCountMaxException as e:
            resM, mM, count = e.args
            print 'Exceeds COUNT_MAX, res=%s, m=%s, count=%s'%(resM, mM, count)
            continue
        except NotConvergeException as e:
            resM, mM, count = e.args
            print 'Not converge, res=%s, m=%s, count=%s'%(resM, mM, count)
            continue
        print 'de', 'm', resM, mM, count
        #data
        def getError(m, s):
            if s == 0:
                return 0
            return (s - m) / s
        data['res'].add(lambd, resM)
        data['res'].add(lambd, resS)
    def getM(array):
        return array[0]
    def getS(array):
        return array[1]
    for key in keys:
        fig = plt.figure()
        axes = fig.add_subplot(111)
        if key == 'res':
            x, y = data[key].get(ymap=(getM, getS))
            ym, ys = y
            axes.plot(x, ym, '+b-')
            axes.plot(x, ys, 'ro')
            axes.set_xlabel('Arrival Rate')
            axes.set_ylim([0, 250])
            axes.set_ylabel('Response Time')
            fig.savefig('tmp/validate_desys.pdf')

def main():
    if len(sys.argv) != 3:
        print 'validate <key> <result file>'
        print
        sys.exit()
    key = sys.argv[1]
    if key == 'test':
        data = DataPoints()
        for i in range(5):
            data.add(i, i-1)
            data.add(i, i)
            data.add(i, i+1)
        x, y = data.get((np.average, min, max))
        yave, ymin, ymax = y
        print x, yave, ymin, ymax
    else:
        params = readparams(sys.argv[2])
        if key == 'fp':
            validate_fp(params)
        elif key == 'sp':
            validate_sp(params)
        elif key == 'ep':
            validate_ep(params)
        elif key == 'nd':
            validate_nd(params)
        elif key == 'de':
            validate_de(params)
        elif key == 'ndsys':
            validate_ndsys(params)
        elif key == 'desys':
            validate_desys(params)
        else:
            print 'key error: %s'%key

if __name__ == '__main__':
    main()
