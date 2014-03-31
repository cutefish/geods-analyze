import random
import sys

import numpy as np
import matplotlib
matplotlib.use('pdf')
import matplotlib.pylab as plt
from matplotlib.ticker import MaxNLocator
from matplotlib.ticker import FuncFormatter

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
from scripts.collect import readconfig

#matplotlib.rcParams['text.usetex'] = True
matplotlib.rc('xtick', labelsize=36)
matplotlib.rc('ytick', labelsize=36)
matplotlib.rc('font', size=24)
matplotlib.rc('lines', markersize=10)

DDists = { }

#def toPercent(y, position):
#    return '%.0f%s'%(y, r'$^{\bf \%}$')

def toPercent(y, position):
    return '%d%%'%(int(y))

percentFormatter = FuncFormatter(toPercent)

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
    minerror = 100
    maxerror = -100
    for i, param in enumerate(params):
        config, result = param
        if not 'cdylock' in config['system.impl']:
            continue
        m = config['max.num.txns.in.system']
        k = config['nwrites']
        n = config['dataset.groups'][1]
        s = config['write.intvl.mean']
        c = config['commit.time.mean']
        dt = config['dist.type']
        if n < 4096:
            continue
        if dt == 'fixed':
            rs = 0.5 * s
            rc = 0.5 * c
        elif dt == 'expo':
            rs = s
            rc = c
        psM, pdM, wsM, resM, betaM = calcNDetmnExec(n, m, k, s, c, rs, rc)
        #sim
        try:
            psS = result['lock.block.prob']
            pdS = result['abort.deadlock.prob']
            wsS = result['lock.block.time.mean']
            resS = result['res.mean']
            lS = result['load.mean']
        except:
            print config['filegen.id']
            continue
        #data
        def getError(m, s):
            if s == 0:
                return 0
            return (m - s) / m * 100
        if abs(getError(resM, resS)) > 50:
            print ('id=%s, n=%s, m=%s, k=%s, s=%s, c=%s, rs=%s, rc=%s, '
                   'psM=%s, psS=%s, pdM=%s, pdS=%s, wsM=%s, wsS=%s, resM=%s, resS=%s'
                   % (config['filegen.id'], n, m, k, s, c, rs, rc,
                      psM, psS, pdM, pdS, wsM, wsS, resM, resS))
        #data['pd'].add(beta, getError(pdM, pdS))
        data['ps'].add(betaM, getError(psM, psS))
        data['ws'].add(psS, getError(wsM, wsS))
        data['load'].add(psS, getError(m, lS))
        data['res'].add(betaM, getError(resM, resS))
        error = getError(resM, resS)
        minerror = min(minerror, error)
        maxerror = max(maxerror, error)
    for key in keys:
        fig = plt.figure()
        axes = fig.add_subplot(111)
        x, y = data[key].get()
        axes.plot(x, y, '+')
        #axes.set_xlabel('Probability of Blocking Per Step')
        axes.xaxis.set_major_locator(MaxNLocator(nbins=4))
        axes.yaxis.set_major_locator(MaxNLocator(nbins=5))
        if key == 'res':
            axes.set_ylim([(minerror - 4) / 5 * 5, (maxerror + 4) / 5 * 5])
            axes.yaxis.set_major_formatter(percentFormatter)
            fig.subplots_adjust(left=0.2)
            fig.savefig('tmp/validate_nd.pdf')
        else:
            fig.savefig('tmp/validate_nd_%s.pdf'%key)
            pass

def validate_de(params):
    data = {}
    keys = ['pt', 'a', 'h', 'wt', 'res', 'load']
    for key in keys:
        data[key] = DataPoints()
    minerror = 100
    maxerror = -100
    for i, param in enumerate(params):
        config, result = param
        if not 'cdetmn' in config['system.impl']:
            continue
        m = config['max.num.txns.in.system']
        k = config['nwrites']
        n = config['dataset.groups'][1]
        s = config['write.intvl.mean']
        if n < 4096:
            continue
        #model
        ptM, aM, hM, wtM, resM, betaM = calcDetmnExec(n, m, k, s)
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
            return (m - s) / m * 100
        error = getError(resM, resS)
        data['res'].add(betaM, error)
        minerror = min(minerror, error)
        maxerror = max(maxerror, error)
    for key in keys:
        fig = plt.figure()
        axes = fig.add_subplot(111)
        x, y = data[key].get()
        axes.plot(x, y, '+')
        axes.xaxis.set_major_locator(MaxNLocator(nbins=5))
        axes.yaxis.set_major_locator(MaxNLocator(nbins=5))
        if key == 'res':
            axes.set_ylim([(minerror - 9) / 10 * 10, (maxerror + 9) / 10 * 10])
            fig.subplots_adjust(left=0.2)
            axes.yaxis.set_major_formatter(percentFormatter)
            fig.savefig('tmp/validate_de.pdf')
        else:
            #fig.savefig('tmp/validate_de_%s.pdf'%key)
            pass

def validate_sp(params):
    data = {}
    keys = ['res']
    for key in keys:
        data[key] = DataPoints()
    minerror = 100
    maxerror = -100
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
        def getError(m, s):
            if s == 0:
                return 0
            return (m - s) / m * 100
        error = getError(resM.mean, resS)
        data['res'].add(ddist.std, error)
        minerror = min(minerror, error)
        maxerror = max(maxerror, error)
        print i, error
    for key in keys:
        fig = plt.figure()
        axes = fig.add_subplot(111)
        x, y = data[key].get((np.average, min, max))
        yave, ymin, ymax = y
        axes.errorbar(x, yave, fmt='o',
                      yerr=[np.array(yave) - np.array(ymin),
                            np.array(ymax) - np.array(yave)])
        #axes.set_ylabel('Error Rate')
        axes.set_xlabel('Network Latency Standard Deviation')
        axes.xaxis.set_major_locator(MaxNLocator(nbins=5))
        axes.yaxis.set_major_locator(MaxNLocator(nbins=5))
        axes.set_xlim([5, 50])
        fig.subplots_adjust(bottom=0.15)
        if key == 'res':
            axes.set_ylim([(minerror - 1) / 2 * 2, (maxerror + 1) / 2 * 2])
            axes.yaxis.set_major_formatter(percentFormatter)
            fig.savefig('tmp/validate_sp.pdf')
        else:
            #fig.savefig('tmp/validate_sp_%s.pdf'%key)
            pass

def validate_ep(params):
    data = {}
    keys = ['res']
    for key in keys:
        data[key] = DataPoints()
    minerror = 100
    maxerror = -100
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
        def getError(m, s):
            if s == 0:
                return 0
            return (m - s) / m * 100
        error = getError(resM.mean, resS)
        data['res'].add(ddist.std, error)
        minerror = min(minerror, error)
        maxerror = max(maxerror, error)
        print i, error
    for key in keys:
        fig = plt.figure()
        axes = fig.add_subplot(111)
        x, y = data[key].get((np.average, min, max))
        yave, ymin, ymax = y
        axes.errorbar(x, yave, fmt='o',
                      yerr=[np.array(yave) - np.array(ymin),
                            np.array(ymax) - np.array(yave)])
        axes.set_xlabel('Network Latency Standard Deviation')
        axes.set_xlim([5, 50])
        axes.xaxis.set_major_locator(MaxNLocator(nbins=5))
        axes.yaxis.set_major_locator(MaxNLocator(nbins=6))
        fig.subplots_adjust(bottom=0.15)
        if key == 'res':
            axes.set_ylim([(minerror - 4) / 5 * 5, (maxerror + 4) / 5 * 5])
            axes.yaxis.set_major_formatter(percentFormatter)
            fig.savefig('tmp/validate_ep.pdf')
        else:
            #fig.savefig('tmp/validate_ep_%s.pdf'%key)
            pass


def validate_fp(params):
    data = {}
    data['res'] = DataPoints()
    minerror = 100
    maxerror = -100
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
        #model
        try:
            resM, eNM, lambdaT = getFPLatencyDist(n, ddist, lambd)
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
        lambdaT = int(lambdaT / 0.1) * 0.1
        def getError(m, s):
            if s == 0:
                return 0
            return (m - s) / m * 100
        error = getError(resM, resS)
        data['res'].add(lambdaT, error)
        minerror = min(minerror, error)
        maxerror = max(maxerror, error)
        print i, error
    fig = plt.figure()
    axes = fig.add_subplot(111)
    x, y = data['res'].get((np.average, min, max))
    yave, ymin, ymax = y
    axes.errorbar(x, yave, fmt='o',
                  yerr=[np.array(yave) - np.array(ymin),
                        np.array(ymax) - np.array(yave)])
    axes.set_xlim([0.1, 0.9])
    axes.xaxis.set_major_locator(MaxNLocator(nbins=5))
    axes.yaxis.set_major_locator(MaxNLocator(nbins=5))
    axes.set_ylim([(minerror - 9) / 10 * 10, (maxerror + 9) / 10 * 10])
    axes.yaxis.set_major_formatter(percentFormatter)
    axes.set_xlabel(r'Arrival Rate $\times$ Average Quorum Latency')
    fig.subplots_adjust(bottom=0.15)
    fig.savefig('tmp/validate_fp.pdf')


def model_ndsys(confdir):
    config = readconfig(confdir)
    network = config['nw.latency.cross.zone']
    nwdist = getDDist(network)
    z = config['num.zones']
    f = z / 2 + 1
    q = quorum(z, f, nwdist)
    c = q.mean
    rc = (c**2 + q.var) / 2 / c
    n = config['dataset.groups'][1]
    k = config['nwrites']
    step = config['txn.classes'][0]['action.intvl.dist']
    stdist = getDDist(step)
    s = stdist.mean
    rs = (s**2 + stdist.var) / 2 / s
    arr = config['txn.arrive.interval.dist'][1]
    zl = 1.0 / arr
    l = z * zl
    C = float(z - 1) / z * nwdist.mean
    res, m, count, stats = calcNDetmnSystem(n, k, s, c, rs, rc, l, C)
    ps, pd, ws, beta = stats
    print ('res=%s, m=%s, count=%s, (ps=%s, pd=%s, ws=%s, beta=%s)'
           % (res, m, count, ps, pd, ws, beta))


def validate_ndsys(params):
    data = {}
    minerror = 100
    maxerror = -100
    keys = ['res', 'm']
    for key in keys:
        data[key] = DataPoints()
    for i, param in enumerate(params):
        config, result = param
        if not 'mstdylock' in config['system.impl']:
            continue
        network = config['nw.latency.cross.zone']
        nwdist = getDDist(network)
        z = config['num.zones']
        f = z / 2 + 1
        q = quorum(z, f, nwdist)
        c = q.mean
        rc = (c**2 + q.var) / 2 / c
        n = config['dataset.groups'][1]
        k = config['nwrites']
        step = config['txn.classes'][0]['action.intvl.dist']
        stdist = getDDist(step)
        s = stdist.mean
        rs = (s**2 + stdist.var) / 2 / s
        arr = config['txn.arrive.interval.dist'][1]
        zl = 1.0 / arr
        l = z * zl
        C = float(z - 1) / z * nwdist.mean
        if n < 4096:
            continue
        #sim
        resS = result['res.mean']
        mS = result['load.mean']
        print 'sim', resS, mS
        #model
        try:
            resM, mM, count, stats = calcNDetmnSystem(n, k, s, c, rs, rc, l, C)
            psM, pdM, wsM, betaM = stats
        except ExceedsCountMaxException as e:
            resM, mM, count = e.args
            print 'Exceeds COUNT_MAX, res=%s, m=%s, count=%s'%(resM, mM, count)
            continue
        except NotConvergeException as e:
            resM, mM, count = e.args
            print 'Not converge, res=%s, m=%s, count=%s'%(resM, mM, count)
            continue
        print 'model', resM, mM
        print ('n=%s, k=%s, s=%s, c=%s, rs=%s, rc=%s, l=%s, C=%s'
               % (n, k, s, c, rs, rc, l, C))
        print k * s + c + C, psM, betaM
        #data
        def getError(m, s):
            if s == 0:
                return 0
            return (m - s) / m * 100
        if resS >= 20 * resM:
            print >> sys.stderr, i, resS, resM, \
                    ('n=%s, k=%s, s=%s, c=%s, rs=%s, rc=%s, l=%s, C=%s'
                     % (n, k, s, c, rs, rc, l, C))
            continue
        error = getError(resM, resS)
        minerror = min(error, minerror)
        maxerror = max(error, maxerror)
        print error
        data['res'].add(betaM, getError(resM, resS))
        data['m'].add(betaM, getError(mM, mS))
    for key in keys:
        fig = plt.figure()
        axes = fig.add_subplot(111)
        x, y = data[key].get()
        axes.plot(x, y, '+')
        axes.xaxis.set_major_locator(MaxNLocator(nbins=4))
        axes.yaxis.set_major_locator(MaxNLocator(nbins=4))
        if key == 'res':
            axes.set_ylim([(minerror - 9) / 10 * 10, (maxerror + 9) / 10 * 10])
            axes.yaxis.set_major_formatter(percentFormatter)
            fig.subplots_adjust(left=0.2)
            fig.savefig('tmp/validate_ndsys.pdf')
        else:
            fig.savefig('tmp/validate_ndsys_%s.pdf' % (key))


def validate_ndsys1(params):
    data = {}
    labels = {}
    minerror = -1
    maxerror = 1
    linestyles = {'sim' : '--', 'model' : '-'}
    colors = ['r', 'b', 'g', 'y']
    markers = ['o', '^', 'x', 's']
    for i, param in enumerate(params):
        config, result = param
        if not 'mstdylock' in config['system.impl']:
            continue
        network = config['nw.latency.cross.zone']
        nwdist = getDDist(network)
        z = config['num.zones']
        f = z / 2 + 1
        q = quorum(z, f, nwdist)
        c = q.mean
        rc = (c**2 + q.var) / 2 / c
        n = config['dataset.groups'][1]
        k = config['nwrites']
        step = config['txn.classes'][0]['action.intvl.dist']
        stdist = getDDist(step)
        s = stdist.mean
        rs = (s**2 + stdist.var) / 2 / s
        arr = config['txn.arrive.interval.dist'][1]
        zl = 1.0 / arr
        l = z * zl
        C = float(z - 1) / z * nwdist.mean
        #sim
        resS = result['res.mean']
        mS = result['load.mean']
        print 'sim', resS, mS
        #model
        try:
            resM, mM, count, stats = calcNDetmnSystem(n, k, s, c, rs, rc, l, C)
            psM, pdM, wsM, betaM = stats
        except ExceedsCountMaxException as e:
            resM, mM, count = e.args
            print 'Exceeds COUNT_MAX, res=%s, m=%s, count=%s'%(resM, mM, count)
            continue
        except NotConvergeException as e:
            resM, mM, count = e.args
            print 'Not converge, res=%s, m=%s, count=%s'%(resM, mM, count)
            continue
        print 'model', resM, mM
        print ('n=%s, k=%s, s=%s, c=%s, rs=%s, rc=%s, l=%s, C=%s'
               % (n, k, s, c, rs, rc, l, C))
        print k * s + c + C, psM, betaM
        #data
        def getError(m, s):
            if s == 0:
                return 0
            return (s - m) / s
        error = getError(resM, resS)
        minerror = min(error, minerror)
        maxerror = max(error, maxerror)
        print error
        if l not in labels:
            labels[l] = {}
            labels[l]['sim'] = set([])
            labels[l]['model'] = set([])
        labels[l]['sim'].add(nwdist.mean)
        labels[l]['model'].add(nwdist.mean)
        data[(l, 'sim', nwdist.mean)] = resS
        data[(l, 'model', nwdist.mean)] = resM
    fig = plt.figure()
    axes = fig.add_subplot(111)
    for typename in ['sim', 'model']:
        for i, l in enumerate(sorted(labels.keys())):
            x = []
            y = []
            linestyle = linestyles[typename]
            color = colors[i]
            marker = markers[i]
            for j, xx in enumerate(sorted(labels[l][typename])):
                x.append(xx)
                y.append(data[(l, typename, xx)])
            axes.plot(x, y, linestyle=linestyle, color=color, marker=marker)
    fig.savefig('tmp/validate_ndsys.pdf')

def validate_desys(params):
    data = {}
    data['res'] = DataPoints()
    data['m'] = DataPoints()
    keys = ['res', 'm']
    minerror = 100
    maxerror = -100
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
        if n < 4096:
            continue
        print n, k, s, l, p
        #sim
        resS = result['res.mean']
        mS = result['load.mean']
        print 'de', 's', resS, mS
        #model
        try:
            resM, mM, count, stats = calcDetmnSystem(n, k, s, l, p)
            ptM, aM, hM, wtM, betaM = stats
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
            return (m - s) / m * 100
        data['res'].add(betaM, getError(resM, resS))
        data['m'].add(betaM, getError(mM, mS))
        error = getError(resM, resS)
        minerror = min(minerror, error)
        maxerror = max(maxerror, error)
    for key in keys:
        fig = plt.figure()
        axes = fig.add_subplot(111)
        x, y = data[key].get()
        axes.plot(x, y, '+')
        axes.xaxis.set_major_locator(MaxNLocator(nbins=4))
        axes.yaxis.set_major_locator(MaxNLocator(nbins=5))
        if key == 'res':
            axes.set_ylim([(minerror - 9) / 10 * 10, (maxerror + 9) / 10 * 10])
            axes.yaxis.set_major_formatter(percentFormatter)
            fig.subplots_adjust(left=0.2)
            fig.savefig('tmp/validate_desys.pdf')
        else:
            fig.savefig('tmp/validate_desys_%s.pdf' % (key))

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
    elif key == 'model_ndsys':
        model_ndsys(sys.argv[2])
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
