import numpy as np
import matplotlib
matplotlib.use('pdf')
import matplotlib.pylab as plt
from matplotlib import rc
rc('text', usetex=True)

from protocol import DDist, getSLPLatencyDist, getFPLatencyDist, getEBLatencyDist

def plotLatencyNormal():
    n = 7
    lambd = 1 / 1000.0
    sync = DDist.sample({'rv.name':'normal', 'mu':0, 'sigma':5}, h=0.5)
    elen = 10
    points = {}
    x = [10, 30, 50, 70]; d = 20; delta = d / 10.0 / 3 * 2
    points['slp.r'] = ([], [], -4 * delta, 'r*')
    points['slp.d'] = ([], [], -3 * delta, 'r*')
    points['slp'] = ([], [], -2 * delta, 'r*')
    points['fp.r'] = ([], [], -1 * delta, 'bo')
    points['fp.c'] = ([], [], 0, 'bo')
    points['fp'] = ([], [], 1 * delta, 'bo')
    points['ep.r'] = ([], [], 2 * delta, 'gx')
    points['ep.s'] = ([], [], 3 * delta, 'gx')
    points['ep.m'] = ([], [], 4 * delta, 'gx')
    points['ep'] = ([], [], 5 * delta, 'gx')
    for sigma in x:
        nlat = DDist.sample({'rv.name':'normal', 'mu':100, 'sigma': sigma}, h=0.5)
        slp, slpstats = getSLPLatencyDist(n, nlat, lambd)
        slprmean, slprstd, slpdmean, slpdstd = slpstats
        print ('slp rtrip.mean=%s, rtrip.std=%s, delay.mean=%s, delay.std=%s, mean=%s, std=%s'
               %(slprmean, slprstd, slpdmean, slpdstd, slp.mean, slp.std))
        points['slp.r'][0].append(slprmean); points['slp.r'][1].append(slprstd);
        points['slp.d'][0].append(slpdmean); points['slp.d'][1].append(slpdstd);
        points['slp'][0].append(slp.mean);   points['slp'][1].append(slp.std);
        fp, fpstats = getFPLatencyDist(n, nlat, lambd)
        fprmean, fprstd, fpcmean, fpcstd = fpstats
        print ('fp rtrip.mean=%s, rtrip.std=%s, collison.mean=%s, collison.std=%s, mean=%s, std=%s'
               %(fprmean, fprstd, fpcmean, fpcstd, fp.mean, fp.std))
        points['fp.r'][0].append(fprmean); points['fp.r'][1].append(fprstd);
        points['fp.c'][0].append(fpcmean); points['fp.c'][1].append(fpcstd);
        points['fp'][0].append(fp.mean);   points['fp'][1].append(fp.std);
        ep, epstats = getEBLatencyDist(n, nlat, sync, elen)
        eprmean, eprstd, epsmean, epsstd, epmmean, epmstd = epstats
        print ('ep rtrip.mean=%s, rtrip.std=%s, sync.mean=%s, sync.std=%s, max.mean=%s, max.std=%s, mean=%s, std=%s'
               %(eprmean, eprstd, epsmean, epsstd, epmmean, epmstd, ep.mean, ep.std))
        points['ep.r'][0].append(eprmean); points['ep.r'][1].append(eprstd);
        points['ep.s'][0].append(epsmean); points['ep.s'][1].append(epsstd);
        points['ep.m'][0].append(epmmean); points['ep.m'][1].append(epmstd);
        points['ep'][0].append(ep.mean);   points['ep'][1].append(ep.std);
    fig = plt.figure()
    axes = fig.add_subplot(111)
    bars = {}
    for key, val in points.iteritems():
        y, e, shift, color = val
        bar = axes.errorbar(np.array(x) + shift, y, yerr=e, fmt=color, markersize=10)
        if '.' not in key:
            bars[key] = bar
    axes.set_xlabel('sigma')
    axes.set_ylabel('latency')
    axes.set_xticks(x)
    fig.legend((bars['slp'][0], bars['fp'][0], bars['ep'][0]), ('slp', 'fp', 'ep'),
              loc='upper center', ncol = 3)
    fig.savefig('LatencyNormal.pdf')

def plotLatencyNormal1():
    n = 7
    lambd1 = 1 / 800.0
    lambd2 = 1 / 200.0
    s1 = 4
    s2 = 64
    sync1 = DDist.sample({'rv.name':'normal', 'mu':0, 'sigma':s1}, h=0.5)
    sync2 = DDist.sample({'rv.name':'normal', 'mu':0, 'sigma':s2}, h=0.5)
    elen = 10
    points = {}
    x = [10, 30, 50]; d = 20; delta = d / 3.0 / 4 * 3
    points['slp.l1'] = ([], -1.5 * delta, 'DarkOrange', '//')
    points['slp.l2'] = ([], -1.5 * delta, 'DarkOrange', '//')
    points['fp.l1'] = ([], -0.5 * delta, 'Aqua', 'xx')
    points['fp.l2'] = ([], -0.5 * delta, 'Aqua', 'xx')
    points['ep.s1'] = ([], 0.5 * delta, 'GreenYellow', '..')
    points['ep.s2'] = ([], 0.5 * delta, 'GreenYellow', '..')
    for sigma in x:
        #slp
        nlat = DDist.sample({'rv.name':'normal', 'mu':100, 'sigma': sigma}, h=0.5)
        slp, slpstats = getSLPLatencyDist(n, nlat, lambd1)
        slprmean, slprstd, slpdmean, slpdstd = slpstats
        print ('slp rtrip.mean=%s, rtrip.std=%s, delay.mean=%s, delay.std=%s, mean=%s, std=%s'
               %(slprmean, slprstd, slpdmean, slpdstd, slp.mean, slp.std))
        points['slp.l1'][0].append(slp.mean);
        slp, slpstats = getSLPLatencyDist(n, nlat, lambd2)
        slprmean, slprstd, slpdmean, slpdstd = slpstats
        print ('slp rtrip.mean=%s, rtrip.std=%s, delay.mean=%s, delay.std=%s, mean=%s, std=%s'
               %(slprmean, slprstd, slpdmean, slpdstd, slp.mean, slp.std))
        points['slp.l2'][0].append(slp.mean);
        #fp
        fp, fpstats = getFPLatencyDist(n, nlat, lambd1)
        fprmean, fprstd, fpcmean, fpcstd = fpstats
        print ('fp rtrip.mean=%s, rtrip.std=%s, collison.mean=%s, collison.std=%s, mean=%s, std=%s'
               %(fprmean, fprstd, fpcmean, fpcstd, fp.mean, fp.std))
        points['fp.l1'][0].append(fp.mean);
        fp, fpstats = getFPLatencyDist(n, nlat, lambd2)
        fprmean, fprstd, fpcmean, fpcstd = fpstats
        print ('fp rtrip.mean=%s, rtrip.std=%s, collison.mean=%s, collison.std=%s, mean=%s, std=%s'
               %(fprmean, fprstd, fpcmean, fpcstd, fp.mean, fp.std))
        points['fp.l2'][0].append(fp.mean);
        #ep
        ep, epstats = getEBLatencyDist(n, nlat, sync1, elen)
        eprmean, eprstd, epsmean, epsstd, epmmean, epmstd = epstats
        print ('ep rtrip.mean=%s, rtrip.std=%s, sync.mean=%s, sync.std=%s, max.mean=%s, max.std=%s, mean=%s, std=%s'
               %(eprmean, eprstd, epsmean, epsstd, epmmean, epmstd, ep.mean, ep.std))
        points['ep.s1'][0].append(ep.mean);
        ep, epstats = getEBLatencyDist(n, nlat, sync2, elen)
        eprmean, eprstd, epsmean, epsstd, epmmean, epmstd = epstats
        print ('ep rtrip.mean=%s, rtrip.std=%s, sync.mean=%s, sync.std=%s, max.mean=%s, max.std=%s, mean=%s, std=%s'
               %(eprmean, eprstd, epsmean, epsstd, epmmean, epmstd, ep.mean, ep.std))
        points['ep.s2'][0].append(ep.mean);
    fig = plt.figure()
    axes = fig.add_subplot(121)
    bars = {}
    for key, val in points.iteritems():
        if 'l1' in key or 's1' in key:
            y, shift, color, hatch = val
            bar = axes.bar(np.array(x) + shift, y, delta, color=color, hatch=hatch)
            if 'slp' in key and 'SingleLeader' not in bars:
                bars['SingleLeader'] = bar
            elif 'fp' in key and 'Fast' not in bars:
                bars['Fast'] = bar
            elif 'ep' in key and 'Epoch-based' not in bars:
                bars['Epoch-based'] = bar
    axes.text(5, 350, r'$\lambda=%.2e, \sigma=%s$'%(lambd1, s1), fontsize=14)
    axes.set_xlabel('Low-level Delay Deviation')
    axes.set_ylabel('Mean Protocol-level Delay')
    axes.set_ylim([150, 400])
    axes.set_xticks(x)
    axes = fig.add_subplot(122)
    for key, val in points.iteritems():
        if 'l2' in key or 's2' in key:
            y, shift, color, hatch = val
            bar = axes.bar(np.array(x) + shift, y, delta, color=color, hatch=hatch)
    axes.text(5, 350, r'$\lambda=%.2e, \sigma=%s$'%(lambd2, s2), fontsize=14)
    axes.set_xlabel('Low-level Delay Deviation')
    #axes.set_ylabel('Mean Protocol-level Delay')
    axes.set_ylim([150, 400])
    axes.set_xticks(x)
    fig.legend((bars['SingleLeader'][0], bars['Fast'][0], bars['Epoch-based'][0]), 
               ('SingleLeader', 'Fast', 'Epoch-based'),
              loc='upper center', ncol = 3)
    fig.savefig('LatencyNormal.pdf')


def plotLatencyPareto():
    n = 5
    lambd = 1 / 1000.0
    sync = DDist.sample({'rv.name':'normal', 'mu':0, 'sigma':5}, h=2)
    elen = 10
    points = {}
    x = [1.2, 1.6, 2, 2.4]; d = 0.4; delta = d / 10.0 / 3 * 2
    points['slp.r'] = ([], [], -4 * delta, 'r*')
    points['slp.d'] = ([], [], -3 * delta, 'r*')
    points['slp'] = ([], [], -2 * delta, 'r*')
    points['fp.r'] = ([], [], -1 * delta, 'bo')
    points['fp.c'] = ([], [], 0, 'bo')
    points['fp'] = ([], [], 1 * delta, 'bo')
    points['ep.r'] = ([], [], 2 * delta, 'gx')
    points['ep.s'] = ([], [], 3 * delta, 'gx')
    points['ep.m'] = ([], [], 4 * delta, 'gx')
    points['ep'] = ([], [], 5 * delta, 'gx')
    for a in x:
        xm = 100.0 * (a - 1) / a
        nlat = DDist.sample({'rv.name':'pareto', 'inf':xm, 'a': a}, h=2)
        nlat.plot('/tmp/test_pareto_%s.pdf'%a)
        slp, slpstats = getSLPLatencyDist(n, nlat, lambd)
        slprmean, slprstd, slpdmean, slpdstd = slpstats
        print ('slp rtrip.mean=%s, rtrip.std=%s, delay.mean=%s, delay.std=%s, mean=%s, std=%s'
               %(slprmean, slprstd, slpdmean, slpdstd, slp.mean, slp.std))
        points['slp.r'][0].append(slprmean); points['slp.r'][1].append(slprstd);
        points['slp.d'][0].append(slpdmean); points['slp.d'][1].append(slpdstd);
        points['slp'][0].append(slp.mean);   points['slp'][1].append(slp.std);
        fp, fpstats = getFPLatencyDist(n, nlat, lambd)
        fprmean, fprstd, fpcmean, fpcstd = fpstats
        print ('fp rtrip.mean=%s, rtrip.std=%s, collison.mean=%s, collison.std=%s, mean=%s, std=%s'
               %(fprmean, fprstd, fpcmean, fpcstd, fp.mean, fp.std))
        points['fp.r'][0].append(fprmean); points['fp.r'][1].append(fprstd);
        points['fp.c'][0].append(fpcmean); points['fp.c'][1].append(fpcstd);
        points['fp'][0].append(fp.mean);   points['fp'][1].append(fp.std);
        ep, epstats = getEBLatencyDist(n, nlat, sync, elen)
        eprmean, eprstd, epsmean, epsstd, epmmean, epmstd = epstats
        print ('ep rtrip.mean=%s, rtrip.std=%s, sync.mean=%s, sync.std=%s, max.mean=%s, max.std=%s, mean=%s, std=%s'
               %(eprmean, eprstd, epsmean, epsstd, epmmean, epmstd, ep.mean, ep.std))
        points['ep.r'][0].append(eprmean); points['ep.r'][1].append(eprstd);
        points['ep.s'][0].append(epsmean); points['ep.s'][1].append(epsstd);
        points['ep.m'][0].append(epmmean); points['ep.m'][1].append(epmstd);
        points['ep'][0].append(ep.mean);   points['ep'][1].append(ep.std);
        print ep.lb, ep.ub
    fig = plt.figure()
    axes = fig.add_subplot(111)
    bars = {}
    for key, val in points.iteritems():
        y, e, shift, color = val
        bar = axes.errorbar(np.array(x) + shift, y, yerr=e, fmt=color, markersize=11)
        if '.' not in key:
            bars[key] = bar
    axes.set_xlabel('sigma')
    axes.set_ylabel('latency')
    axes.set_xticks(x)
    fig.legend((bars['slp'][0], bars['fp'][0], bars['ep'][0]), ('slp', 'fp', 'ep'),
              loc='upper center', ncol = 3)
    fig.savefig('LatencyPareto.pdf')

def plotImpactOfCollision():
    n = 7
    x = [50, 100, 200, 400, 800, 1600, 3200]
    y = []
    e = []
    nlat = DDist.sample({'rv.name':'normal', 'mu':100, 'sigma': 30}, h=0.5)
    for t in x:
        lambd = 1.0 / t
        fp, fpstats = getFPLatencyDist(n, nlat, lambd)
        y.append(fp.mean)
        e.append(fp.std)
        print fp.mean, fp.std
    fig = plt.figure()
    axes = fig.add_subplot(111)
    #axes.errorbar(x, y, fmt='+r', yerr=e, markersize=12)
    axes.plot(x, y, '-+b', markersize=12)
    axes.set_xscale('log')
    axes.set_xlabel('1.0 / lambda')
    axes.set_ylabel('latency')
    axes.set_xticks(x)
    axes.set_xticklabels(x)
    axes.set_xlim([25, 6400])
    fig.savefig('ImpactOfCollision.pdf')

def plotImpactOfSync():
    n = 7
    x = [2, 4, 8, 16, 32, 64]
    y = []
    e = []
    nlat = DDist.sample({'rv.name':'normal', 'mu':100, 'sigma': 30}, h=0.5)
    elen = 10
    for sigma in x:
        sync = DDist.sample({'rv.name':'normal', 'mu':0, 'sigma':sigma}, h=0.5)
        ep, epstats = getEBLatencyDist(n, nlat, sync, elen)
        y.append(ep.mean)
        e.append(ep.std)
        print ep.mean, ep.std
    fig = plt.figure()
    axes = fig.add_subplot(111)
    #axes.errorbar(x, y, fmt='+b', yerr=e, markersize=12)
    axes.plot(x, y, '-+b')
    axes.set_xscale('log')
    axes.set_xlabel('sync sigma')
    axes.set_ylabel('latency')
    axes.set_xticks(x)
    axes.set_xticklabels(x)
    axes.set_xlim([1, 128])
    fig.savefig('ImpactOfSync.pdf')

def plotImpactOfElen():
    n = 7
    x = [10, 20, 30, 40, 50]
    y = []
    e = []
    nlat = DDist.sample({'rv.name':'normal', 'mu':100, 'sigma': 50}, h=0.5)
    sync = DDist.sample({'rv.name':'normal', 'mu':0, 'sigma':5}, h=0.5)
    for elen in x:
        ep, epstats = getEBLatencyDist(n, nlat, sync, elen)
        y.append(ep.mean)
        e.append(ep.std)
        print ep.mean, ep.std
    fig = plt.figure()
    axes = fig.add_subplot(111)
    axes.plot(x, y, '-+b')
    axes.set_xlabel('epoch length')
    axes.set_ylabel('latency')
    axes.set_xticks(x)
    axes.set_xticklabels(x)
    fig.savefig('ImpactOfElen.pdf')

def main():
    #plotLatencyNormal()
    plotLatencyNormal1()
    #plotLatencyPareto()
    #plotImpactOfCollision()
    #plotImpactOfSync()
    #plotImpactOfElen()

if __name__ == '__main__':
    main()

