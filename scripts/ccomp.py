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

def compare(params):
    index = {}
    for i, param in enumerate(params):
        config, result = param
        m = config['max.num.txns.in.system']
        k = config['nwrites']
        n = config['dataset.groups'][1]
        s = config['intvl']
        c = config.get('commit.time', 0)
        impl = config['system.impl']
        rt = result['res.mean']
        if (m, k, n, s) not in index:
            index[(m, k, n, s)] = {}
        if 'cdetmn' not in impl:
            index[(m, k, n, s)]['dyn.sim'] = rt
            pc, pd, w, res, beta = calcNDetmnExec(n, m, k, s, c)
            index[(m, k, n, s)]['dyn.ana'] = res
        else:
            index[(m, k, n, s)]['det.sim'] = rt
            pt, nr, h, w, res, beta = calcDetmnExec(n, m, k, s)
            index[(m, k, n, s)]['det.ana'] = res
    x = []
    dyn_sim = []
    det_sim = []
    ratio_sim = []
    dyn_ana = []
    det_ana = []
    ratio_ana = []
    for key, val in index.iteritems():
        m, k, n, s = key
        p = float(m - 1) * k / 2 / n
        x.append(p)
        dyn_sim.append(val['dyn.sim'])
        det_sim.append(val['det.sim'])
        dyn_ana.append(val['dyn.ana'])
        det_ana.append(val['det.ana'])
        ratio_sim.append(val['dyn.sim'] / val['det.sim'])
        ratio_ana.append(val['dyn.ana'] / val['det.ana'])
    fig = plt.figure()
    axes = fig.add_subplot(311)
    line1 = axes.plot(x, dyn_sim, '.r')
    line2 = axes.plot(x, dyn_ana, '+b')
    axes.set_ylabel('Response time')
    axes = fig.add_subplot(312)
    axes.plot(x, det_sim, '.r')
    axes.plot(x, det_ana, '+b')
    axes.set_ylabel('Response time')
    axes = fig.add_subplot(313)
    axes.plot(x, ratio_sim, '.r')
    axes.plot(x, ratio_ana, '+b')
    axes.set_ylabel('Non-det / Det')
    axes.set_xlabel('Lock conflict rate')
    fig.legend((line1, line2), ('sim', 'model'), loc='upper center', ncol=2)
    fig.savefig('tmp/compare_rt.pdf')

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

def approx(params):
    ndetData = {}
    detData = {}
    ndetKeys = ['pc', 'pd', 'w', 'res', 'load']
    detKeys = ['pt', 'h', 'nr', 'w', 'res', 'load']
    for k in ndetKeys:
        ndetData[k] = DataPoints()
    for k in detKeys:
        detData[k] = DataPoints()
    sshift = 10 * 5 * 5
    nshift = 10 * 5
    mshift = 10
    kshift = 1
    sindex = [10, 20, 30, 40, 50]
    nindex = [1024, 4096, 16384]
    mindex = [12, 16, 20]
    kindex = [8, 12, 16, 20]
    for i, param in enumerate(params):
        config, result = param
        m = config['max.num.txns.in.system']
        k = config['nwrites']
        n = config['dataset.groups'][1]
        s = config['intvl']
        c = config.get('commit.time', 0)
        index = findIndex(s, sindex) * sshift + \
                findIndex(k, kindex) * kshift + \
                findIndex(m, mindex) * mshift + \
                findIndex(n, nindex) * nshift
        if config['system.impl'] == 'sim.impl.cdylock.CentralDyLockSystem':
            #model
            pcM, pdM, wM, resM, beta = calcNDetmnExec(n, m, k, s, c)
            #real
            pcR = result['lock.block.prob']
            pdR = result['abort.deadlock.prob']
            wR = result['lock.block.time.mean']
            resR = result['res.mean']
            lR = result['load.mean']
            hR = result['block.height.mean']
            #data
            ndetData['pc'].x.append(pcM)
            ndetData['pc'].y.append(pcR / pcM)
            ndetData['pd'].x.append(pcM)
            ndetData['pd'].y.append(pdR / pdM)
            ndetData['w'].x.append(pcM)
            ndetData['w'].y.append(wR / wM)
            ndetData['res'].x.append(pcM)
            ndetData['res'].y.append(resR / resM)
            ndetData['load'].x.append(pcM)
            ndetData['load'].y.append(lR / m)
            #ndetData['h'].x.append(pcM)
            #ndetData['h'].y.append((h1 + ph2 * h2 + ph3 * h3) / wR)
        elif config['system.impl'] == \
                'sim.impl.cdetmn.CentralDetmnSystem':
            #m = result['load.mean']
            #model
            ptM, nrM, hM, wM, resM, beta = calcDetmnExec(n, m, k, s)
            #real
            ptR = result['lock.block.prob']
            hR = result['block.height.cond.mean']
            loadR = result.get('load.mean', m)
            nrR = m - result['num.blocking.txns.mean']
            wR = result['lock.block.time.mean']
            resR = result['res.mean']
            #data
            detData['pt'].x.append(beta)
            detData['pt'].y.append(ptR / ptM)
            detData['h'].x.append(beta)
            detData['h'].y.append(hR / hM)
            detData['nr'].x.append(beta)
            detData['nr'].y.append(nrR / nrM)
            detData['w'].x.append(beta)
            detData['w'].y.append(wR / wM)
            detData['res'].x.append(beta)
            detData['res'].y.append(resR / resM)
            detData['load'].x.append(beta)
            detData['load'].y.append(loadR / m)
        else:
            raise ValueError('Unknown system: %s'%config['system.impl'])
    for key, val in ndetData.iteritems():
        fig = plt.figure()
        axes = fig.add_subplot(111)
        axes.plot(val.x, val.y, '.r')
        fig.savefig('tmp/%s_%s.pdf'%('ccomp_ndet', key))
    for key, val in detData.iteritems():
        fig = plt.figure()
        axes = fig.add_subplot(111)
        axes.plot(val.x, val.y, '+b')
        fig.savefig('tmp/%s_%s.pdf'%('ccomp_det', key))

def main():
    if len(sys.argv) != 2:
        print 'process <result file>'
        sys.exit()
    params = readparams(sys.argv[1])
    compare(params)
    approx(params)

if __name__ == '__main__':
    main()
