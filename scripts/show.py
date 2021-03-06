import math
import sys

import matplotlib
matplotlib.use('pdf')
import matplotlib.pylab as plt
from matplotlib.ticker import MaxNLocator

from model.ddist import DDist
from model.execute import calcNDetmnExec
from model.execute import calcDetmnExec
from model.protocol import getSLPLatencyDist
from model.protocol import getFPLatencyDist
from model.protocol import getEPLatencyDist
from model.system import calcNDetmnSystem
from model.system import calcDetmnSystem
from model.system import ExceedsCountMaxException
from model.system import NotConvergeException

matplotlib.rc('xtick', labelsize=24)
matplotlib.rc('ytick', labelsize=24)
matplotlib.rc('font', size=24)
matplotlib.rc('lines', markersize=14)

DDists = { }

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
        DDists[ddistkey] = DDist.sample(config, h=1.0)
    return DDists[ddistkey]

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

def show_sysres(n, k, s, lambds, lmeans):
    if len(lambds) > 4:
        raise ValueError('only support len(lambdas) == 4')
    lines = {}
    for key in ['nd', 'de']:
        for lambd in lambds:
            if key not in lines:
                lines[key] = {}
            lines[key][lambd] = DataPoints()
    #compute
    for lambd in lambds:
        for lmean in lmeans:
            c = 2 * lmean
            rs = s
            rc = c
            C = lmean
            try:
                res, m, count, params = calcNDetmnSystem(n, k, s, c, rs, rc, lambd, C)
            except ExceedsCountMaxException as e:
                res, m, count = e.args
                print 'Exceeds COUNT_MAX, res=%s, m=%s, count=%s'%(res, m, count)
                continue
            except NotConvergeException as e:
                res, m, count = e.args
                print ('Not converge, n=%s, k=%s, s=%s, lambd=%s, lmean=%s, '
                       'res=%s, m=%s, count=%s'
                       % (n, k, s, lambd, lmean, res, m, count))
                continue
            lines['nd'][lambd].add(lmean, res)
            print 'nd', lambd, lmean, res
            try:
                res, m, count, params = calcDetmnSystem(n, k, s, lambd, 3 * lmean)
            except ExceedsCountMaxException as e:
                res, m, count = e.args
                print 'Exceeds COUNT_MAX, res=%s, m=%s, count=%s'%(res, m, count)
                continue
            except NotConvergeException as e:
                res, m, count = e.args
                print ('Not converge, n=%s, k=%s, s=%s, lambd=%s, lmean=%s, '
                       'res=%s, m=%s, count=%s'
                       % (n, k, s, lambd, lmean, res, m, count))
                continue
            lines['de'][lambd].add(lmean, res)
            print 'de', lambd, lmean, res
    #plot
    fig = plt.figure()
    axes = fig.add_subplot(111)
    legend_labels = []
    legend_lines = []
    markers = {'nd': '^', 'de':'o'}
    defaultLinestyles = ['-', '--', '-.', ':']
    linestyles = {}
    for i, lambd in enumerate(lambds):
        linestyles[lambd] = defaultLinestyles[i]
    defaultColors = ['b', 'r', 'g', 'k']
    colors = {}
    labelkey = {'nd':'EBR', 'de':'RBE'}
    for i, lambd in enumerate(lambds):
        colors[lambd] = defaultColors[i]
    for key in ['nd', 'de']:
        for lambd in lambds:
            x, y = lines[key][lambd].get()
            line, = axes.plot(x, y, marker=markers[key], linestyle=linestyles[lambd],
                              color=colors[lambd])
            legend_labels.append('%s, $\lambda=%s$'%(labelkey[key], lambd))
            legend_lines.append(line)
    axes.set_xlabel('Average Network Latency')
    axes.set_ylabel('Average Response Time')
    axes.set_ylim([80, 420])
    axes.xaxis.set_major_locator(MaxNLocator(nbins=5))
    axes.yaxis.set_major_locator(MaxNLocator(nbins=5))
    fig.subplots_adjust(bottom=0.15, left=0.15)
    axes.legend(legend_lines, legend_labels, loc='upper left', prop={'size':24})
    fig.savefig('tmp/show_sysres.pdf')

def show_execm(n, k, s, lmeans):
    lines = {}
    for syskey in ['nd', 'de']:
        for mkey in ['capacity', 'active']:
            if syskey not in lines:
                lines[syskey] = {}
            lines[syskey][mkey] = DataPoints()
    #compute
    for lmean in lmeans:
        cap, active, params = getMaxNDActive(n, k, s, 2 * lmean)
        lines['nd']['capacity'].add(lmean, cap)
        lines['nd']['active'].add(lmean, active)
        print 'nd', cap, active, params
        cap, active, params = getMaxDEActive(n, k, s)
        lines['de']['capacity'].add(lmean, cap)
        lines['de']['active'].add(lmean, active)
        print 'de', cap, active, params
    #plot
    fig = plt.figure()
    axes = fig.add_subplot(111)
    legend_labels = []
    legend_lines = []
    markers = {'nd': '^', 'de':'o'}
    linestyles = {'capacity': '--', 'active': '-'}
    colors = {'capacity': 'r', 'active':'b'}
    labelkey = {'nd':'EBR', 'de':'RBE'}
    for syskey in ['nd', 'de']:
        for mkey in ['capacity', 'active']:
            x, y = lines[syskey][mkey].get()
            line, = axes.plot(x, y, marker=markers[syskey], linestyle=linestyles[mkey],
                              color=colors[mkey])
            if mkey == 'capacity':
                legend_labels.append('%s, %s'%(labelkey[syskey], 'Num Txns in System'))
            else:
                legend_labels.append('%s, %s'%(labelkey[syskey], 'Peak Throughput'))
            legend_lines.append(line)
    axes.set_xlabel('Average Network Latency')
    axes.set_ylabel('Max Number of Transactions')
    axes.xaxis.set_major_locator(MaxNLocator(nbins=5))
    axes.yaxis.set_major_locator(MaxNLocator(nbins=5))
    axes.set_ylim([0, 80])
    fig.subplots_adjust(bottom=0.15, left=0.15)
    axes.legend(legend_lines, legend_labels, loc='upper right', prop={'size':22})
    fig.savefig('tmp/show_execm.pdf')

def getMaxNDActive(n, k, s, g):
    m = 1
    prev = 1
    while True:
        m += 1
        ps, pd, ws, res, beta = calcNDetmnExec(n, m, k, s, g, s, 0.5 * g * s)
        curr = m * (1 - beta)
        #print 'nd', m, curr, beta
        if curr < prev + 1e-1:
            break
        prev = curr
        print m, prev, (ps, pd, ws, res, beta)
    return m, prev, (ps, pd, ws, res, beta)

def getMaxDEActive(n, k, s):
    m = 1
    prev = 1
    while True:
        m += 1
        pt, a, h, wt, res, beta = calcDetmnExec(n, m, k, s)
        curr = m * (1 - beta)
        #print 'de', m, curr, beta
        if curr < prev + 1e-1:
            break
        prev = curr
    return m, prev, (pt, a, h, wt, res, beta)

def show_mres(n, k, s, lmeans):
    lines = {}
    for syskey in ['nd', 'de']:
        lines[syskey] = DataPoints()
    #compute
    for lmean in lmeans:
        cap, active, params = getMaxNDActive(n, k, s, 2 * lmean)
        ps, pd, ws, res, beta = params
        lines['nd'].add(lmean, res)
        print 'nd', cap, active, params
        cap, active, params = getMaxDEActive(n, k, s)
        pt, a, h, wt, res, beta = params
        lines['de'].add(lmean, res)
        print 'de', cap, active, params
    #plot
    fig = plt.figure()
    axes = fig.add_subplot(111)
    legend_labels = []
    legend_lines = []
    markers = {'nd': '^', 'de':'o'}
    labelkey = {'nd':'EBR', 'de':'RBE'}
    for syskey in ['nd', 'de']:
        x, y = lines[syskey].get()
        line, = axes.plot(x, y, marker=markers[syskey], linestyle='-', color='r')
        legend_labels.append('%s'%(labelkey[syskey]))
        legend_lines.append(line)
    axes.set_xlabel('Average Network Latency')
    axes.set_ylabel('Response Time')
    axes.xaxis.set_major_locator(MaxNLocator(nbins=5))
    axes.yaxis.set_major_locator(MaxNLocator(nbins=5))
    fig.subplots_adjust(bottom=0.15, left=0.15)
    axes.legend(legend_lines, legend_labels, loc='upper left', prop={'size':24})
    fig.savefig('tmp/show_mres.pdf')

def show_spvsep(n, elen, mean, lb, ub, sigmas, lambds):
    lines = {}
    keys = ['sp', 'ep']
    ddists = []
    sync = getDDist(('fixed', 0))
    for sigma in sigmas:
        ddists.append(
            getDDist(('lognorm', -1,
                      {'mu' : math.log(mean) - sigma**2 / 2,
                       'sigma' : sigma, 'lb' : lb, 'ub' : ub})))
    for lambd in lambds:
        for syskey in keys:
            if lambd not in lines:
                lines[lambd] = {}
            lines[lambd][syskey] = DataPoints()
    #compute
    for lambd in lambds:
        for ddist in ddists:
            res, delay, rtrip = getSLPLatencyDist(n, ddist, lambd)
            lines[lambd]['sp'].add(ddist.std / ddist.mean, res.mean)
            print 'sp', res.mean
            res, delay, rtrip = getEPLatencyDist(n, ddist, sync, elen)
            lines[lambd]['ep'].add(ddist.std / ddist.mean, res.mean)
            print 'ep', res.mean
    #plot
    fig = plt.figure()
    axes = fig.add_subplot(111)
    legend_labels = []
    legend_lines = []
    markers = {'sp': '^', 'ep':'o'}
    defaultLinestyles = ['-', '--', '-.', ':']
    linestyles = {}
    for i, lambd in enumerate(lambds):
        linestyles[lambd] = defaultLinestyles[i]
    defaultColors = ['b', 'r', 'g', 'k']
    colors = {}
    for i, lambd in enumerate(lambds):
        colors[lambd] = defaultColors[i]
    for key in keys:
        for lambd in lambds:
            x, y = lines[lambd][key].get()
            line, = axes.plot(x, y, marker=markers[key], linestyle=linestyles[lambd],
                              color=colors[lambd])
            legend_labels.append('%s, $\lambda=%s$'%(key, lambd))
            legend_lines.append(line)
    axes.set_xlabel('Network Latency Std / Network Latency Mean')
    axes.set_ylabel('Response Time')
    axes.xaxis.set_major_locator(MaxNLocator(nbins=5))
    axes.yaxis.set_major_locator(MaxNLocator(nbins=5))
    fig.subplots_adjust(bottom=0.15, left=0.15)
    axes.legend(legend_lines, legend_labels, loc='upper left', prop={'size':24})
    fig.savefig('tmp/show_spvsep.pdf')

def show_spvsfp(n, m, lb, ub, sigmas, lambds):
    lines = {}
    keys = ['sp', 'fp']
    ddists = {}
    for sigma in sigmas:
        ddists[sigma] = getDDist(('lognorm', -1,
                                  {'mu' : math.log(m) - sigma**2 / 2,
                                   'sigma' : sigma, 'lb' : lb, 'ub' : ub}))
    for syskey in keys:
        for sigma in sigmas:
            if syskey not in lines:
                lines[syskey] = {}
            lines[syskey][sigma] = DataPoints()
    #compute
    for lambd in lambds:
        for sigma in sigmas:
            res, delay, rtrip = getSLPLatencyDist(n, ddists[sigma], lambd)
            lines['sp'][sigma].add(lambd * 2 * m, res.mean)
            print 'sp', res.mean
            res, eN = getFPLatencyDist(n, ddists[sigma], lambd)
            lines['fp'][sigma].add(lambd * 2 * m, res)
            print 'fp', res
            print 'sigma', sigma
            print
    #plot
    fig = plt.figure()
    axes = fig.add_subplot(111)
    legend_labels = []
    legend_lines = []
    markers = {'sp': '^', 'fp':'o'}
    defaultLinestyles = ['-', '--', '-.', ':']
    linestyles = {}
    for i, sigma in enumerate(sigmas):
        linestyles[sigma] = defaultLinestyles[i]
    defaultColors = ['r', 'b', 'g', 'k']
    colors = {}
    for i, sigma in enumerate(sigmas):
        colors[sigma] = defaultColors[i]
    for key in keys:
        for sigma in sigmas:
            x, y = lines[key][sigma].get()
            line, = axes.plot(x, y, marker=markers[key],
                              linestyle=linestyles[sigma],
                              color=colors[sigma])
            #legend_labels.append('%s, $\sigma=%s$'%(key, sigma))
            legend_labels.append('%s'%(key))
            legend_lines.append(line)
    axes.set_xlabel(r'Arrival Rate $\times$ Average Round Trip Latency')
    axes.set_ylabel('Response Time')
    axes.legend(legend_lines, legend_labels, loc='upper left', prop={'size':24})
    axes.xaxis.set_major_locator(MaxNLocator(nbins=5))
    axes.yaxis.set_major_locator(MaxNLocator(nbins=5))
    fig.subplots_adjust(bottom=0.15, left=0.15)
    fig.savefig('tmp/show_spvsfp.pdf')

def show_epelen(n, mean, lb, ub, sigmas, elens):
    lines = {}
    ddists = []
    sync = getDDist(('fixed', 0))
    for sigma in sigmas:
        lines[sigma] = DataPoints()
    for sigma in sigmas:
        ddists.append(
            getDDist(('lognorm', -1,
                      {'mu' : math.log(mean) - sigma**2 / 2,
                       'sigma' : sigma, 'lb' : lb, 'ub' : ub})))
    #compute
    for elen in elens:
        for i, ddist in enumerate(ddists):
            res, delay, rtrip = getEPLatencyDist(n, ddist, sync, elen)
            lines[sigmas[i]].add(elen, res.mean)
            print 'ep', res.mean
    #plot
    fig = plt.figure()
    axes = fig.add_subplot(111)
    legend_labels = []
    legend_lines = []
    defaultMarkers = ['^', 'o', 's', 'x']
    markers = {}
    for i, sigma in enumerate(sigmas):
        markers[sigma] = defaultMarkers[i]
    for i, sigma in enumerate(sigmas):
        x, y = lines[sigma].get()
        line, = axes.plot(x, y, marker=markers[sigma], linestyle='-', color='r')
        legend_labels.append('Network STD =%.2f'%(ddists[i].std))
        legend_lines.append(line)
    axes.set_xlabel('Epoch Length')
    axes.set_ylim([200, 300])
    axes.set_ylabel('Response Time')
    axes.legend(legend_lines, legend_labels, loc='lower right', prop={'size':24})
    axes.xaxis.set_major_locator(MaxNLocator(nbins=5))
    axes.yaxis.set_major_locator(MaxNLocator(nbins=5))
    fig.subplots_adjust(bottom=0.15, left=0.15)
    fig.savefig('tmp/show_epelen.pdf')

def show_epsynch(n, elen, mean, sigma, lb, ub, synchubs):
    lines = {}
    lines['synch'] = DataPoints()
    ddist = getDDist(('lognorm', -1,
                      {'mu' : math.log(mean) - sigma**2 / 2,
                       'sigma' : sigma, 'lb' : lb, 'ub' : ub}))
    #compute
    for synchub in synchubs:
        sync = getDDist(('uniform', -1, {'lb':0, 'ub':synchub}))
        res, delay, rtrip = getEPLatencyDist(n, ddist, sync, elen)
        lines['synch'].add(synchub, res.mean)
        print 'ep', res.mean
    #plot
    fig = plt.figure()
    axes = fig.add_subplot(111)
    x, y = lines['synch'].get()
    line, = axes.plot(x, y, marker='^', linestyle='-', color='r')
    axes.set_xlabel('Time Drift Upper Bound')
    axes.set_ylabel('Response Time')
    axes.xaxis.set_major_locator(MaxNLocator(nbins=5))
    axes.yaxis.set_major_locator(MaxNLocator(nbins=5))
    fig.subplots_adjust(bottom=0.15, left=0.15)
    fig.savefig('tmp/show_epsynch.pdf')

def main():
    if len(sys.argv) != 3:
        print 'show <key> <args>'
        print
        sys.exit()
    key = sys.argv[1]
    args = sys.argv[2]
    if key == 'sysres':
        try:
            n, k, s, lambds, lmeans = args.split(';')
            n, k, s = map(float, (n, k, s))
            lambds, lmeans = map(eval, (lambds, lmeans))
        except Exception as e:
            print 'Error: %s'%e
            print 'Args <n; k; s; [lambds]; [lmeans]>. \n\tGot: %s.'%args
            print 'Example <1024; 12; 10; [0.04, 0.05]; [0, 10, 20, 30, 40, 50, 60]'
            sys.exit(-1)
        show_sysres(n, k, s, lambds, lmeans)
    elif key == 'execm':
        try:
            n, k, s, lmeans = args.split(';')
            n, k, s = map(float, (n, k, s))
            lmeans = eval(lmeans)
        except Exception as e:
            print 'Error: %s'%e
            print 'Args <n; k; s; [lmeans]>. \n\tGot: %s.'%args
            sys.exit(-1)
        show_execm(n, k, s, lmeans)
    elif key == 'mres':
        try:
            n, k, s, lmeans = args.split(';')
            n, k, s = map(float, (n, k, s))
            lmeans = eval(lmeans)
        except Exception as e:
            print 'Error: %s'%e
            print 'Args <n; k; s; [lmeans]>. \n\tGot: %s.'%args
            sys.exit(-1)
        show_mres(n, k, s, lmeans)
    elif key == 'spvsep':
        try:
            n, e, m, lb, ub, sigmas, lambds = args.split(';')
            n = int(n)
            e, m, lb, ub = map(float, (e, m, lb, ub))
            sigmas, lambds = map(eval, (sigmas, lambds))
        except Exception as e:
            print 'Error: %s'%e
            print 'Args <n; elen; mu; lb; ub; [sigmas]; [lambds]>. \n\tGot: %s.'%args
            sys.exit(-1)
        show_spvsep(n, e, m, lb, ub, sigmas, lambds)
    elif key == 'spvsfp':
        try:
            n, m, lb, ub, sigmas, lambds = args.split(';')
            n = int(n)
            m, lb, ub = map(float, (m, lb, ub))
            sigmas = eval(sigmas)
            lambds = eval(lambds)
        except Exception as e:
            print 'Error: %s'%e
            print 'Args <n; m; lb; ub; [sigmas]; [lambds]>. \n\tGot: %s.'%args
            sys.exit(-1)
        show_spvsfp(n, m, lb, ub, sigmas, lambds)
    elif key == 'epelen':
        try:
            n, m, lb, ub, sigmas, elens = args.split(';')
            n = int(n)
            m, lb, ub = map(float, (m, lb, ub))
            sigmas, elens = map(eval, (sigmas, elens))
        except Exception as e:
            print 'Error: %s'%e
            print 'Args <n; m; lb; ub; [sigmas]; [elens]>. \n\tGot: %s.'%args
            sys.exit(-1)
        show_epelen(n, m, lb, ub, sigmas, elens)
    elif key == 'epsynch':
        try:
            n, elen, m, s, lb, ub, synchubs = args.split(';')
            n = int(n)
            elen, m, s, lb, ub = map(float, (elen, m, s, lb, ub))
            synchubs = eval(synchubs)
        except Exception as e:
            print 'Error: %s'%e
            print 'Args <n; m; s; lb; ub; [synchubs]>. \n\tGot: %s.'%args
            sys.exit(-1)
        show_epsynch(n, elen, m, s, lb, ub, synchubs)
    else:
        print 'key error: %s'%key

if __name__ == '__main__':
    main()


