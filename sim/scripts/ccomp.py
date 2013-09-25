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

def compare(params):
    index = {}
    for i, param in enumerate(params):
        config, result = param
        m = config['max.num.txns.in.system']
        k = config['nwrites']
        n = config['dataset.groups'][1]
        s = config['intvl']
        impl = config['system.impl']
        rt = result['res.mean']
        if (m, k, n, s) not in index:
            index[(m, k, n, s)] = {}
        if 'cdetmn' not in impl:
            index[(m, k, n, s)]['dyn.sim'] = rt
            pc = float(m - 1) * k / 2 / n
            pw = 1 - (1 - pc)**k
            pd = pw**2 / (m - 1)
            w1 = float(k - 1) / 3 * s + s
            nc = k * pc
            A = w1 / s / k
            alpha = nc * A
            w = w1 + 0.5 * alpha * w1 + 1.5 * alpha**2 * w1
            anaRt = (k * s + k * pc * w) * (1 + 2 * pd)
            index[(m, k, n, s)]['dyn.ana'] = anaRt
        else:
            index[(m, k, n, s)]['det.sim'] = rt
            pt = 1 - (float(n - (m - 1)*k) / n)**k
            p1 = (float(n-k)/ n)**k
            h = (m - 2) *(1 - (float(n - k) / n)**k) + 1
            anaRt = k * s + pt * (1 - 0.5 * p1 + h - 1 + (m - h - 1) / 2 * (1 - p1)**2) * k * s
            index[(m, k, n, s)]['det.ana'] = anaRt
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
    fig.savefig('compare_rt.pdf')

class DataPoints(object):
    def __init__(self):
        self.x = []
        self.y = []

def approx(params):
    ndetData = {}
    detData = {}
    ndetKeys = ['pc', 'pd', 'w', 'res', 'load']
    detKeys = ['pt', 'h', 'w', 'res', 'load']
    for k in ndetKeys:
        ndetData[k] = DataPoints()
    for k in detKeys:
        detData[k] = DataPoints()
    for i, param in enumerate(params):
        config, result = param
        m = config['max.num.txns.in.system']
        k = config['nwrites']
        n = config['dataset.groups'][1]
        s = config['intvl']
        c = config.get('commit.time', 0)
        gamma = float(c) / s
        if config['system.impl'] == 'impl.cdylock.CentralDyLockSystem':
            #model
            l = float(k) / 2
            L = float(m - 1) * l
            #L = n * (1 - (1 - l / n)**(m - 1))
            pcM = L / n
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
        elif config['system.impl'] == \
                'impl.cdetmn.CentralDetmnSystem':
            #model
            ptM = 1 - (float(n - (m - 1)*k) / n)**k
            p = (1 - (float(n - k) / n)**k)
            hcond = (m - 2) * p
            #hcond = 0
            #for i in range(0, m - 1):
            #    hcond += i * sp.misc.comb(m - 2, i) * p**i * (1 - p)**(m - 2 - i)
            hM = hcond + 1
            #wM = hM * k * s
            wM = (0.5 * p + hM) * k * s
            #wM = (0.5 * p + hM + (m - hM - 1) / 2 * (1 - (float(n - k)/n)**k)**2) * k * s
            resM = k * s + ptM * wM
            #real
            ptR = result['lock.block.prob']
            hR = result['block.height.mean']
            wR = result['lock.block.time.mean']
            resR = result['res.mean']
            loadR = result.get('load.mean', m)
            #data
            detData['pt'].x.append(p)
            detData['pt'].y.append(ptR / ptM)
            detData['h'].x.append(p)
            detData['h'].y.append(hR / hM)
            detData['w'].x.append(p)
            detData['w'].y.append(wR / wM)
            detData['res'].x.append(p)
            detData['res'].y.append(resR / resM)
            detData['load'].x.append(p)
            detData['load'].y.append(loadR / m)
        else:
            raise ValueError('Unknown system: %s'%config['system.impl'])
    for key, val in ndetData.iteritems():
        fig = plt.figure()
        axes = fig.add_subplot(111)
        axes.plot(val.x, val.y, '.r')
        fig.savefig('%s_%s.pdf'%('ccomp_ndet', key))
    for key, val in detData.iteritems():
        fig = plt.figure()
        axes = fig.add_subplot(111)
        axes.plot(val.x, val.y, '+b')
        fig.savefig('%s_%s.pdf'%('ccomp_det', key))

def toss(m, n, k, queue, heights, keepLast):
    newqueue = []
    newheights = []
    if keepLast:
        #remove entries with 0 height
        for i, height in enumerate(heights):
            if height > 0:
                newqueue.append(queue[i])
                newheights.append(-1)
    #add new items
    while len(newqueue) < m:
        items = getItems(n, k)
        newqueue.append(items)
        newheights.append(-1)
    #compute new heights
    computeHeights(newqueue, newheights)
    return newqueue, newheights

def getItems(n, k):
    items = set([])
    while len(items) < k:
        r = random.randint(0, n - 1)
        items.add(r)
    return items

def computeHeights(queue, heights):
    _computeHeights(queue, heights, len(queue) - 1)

def _computeHeights(queue, heights, i):
    if i == 0:
        heights[0] = 0
        return
    if heights[i] != -1:
        return
    maxheight = 0
    items = queue[i]
    for j in range(i):
        if heights[j] == -1:
            _computeHeights(queue, heights, j)
    for j in range(i):
        if len(items.intersection(queue[j])) != 0:
            if maxheight < heights[j] + 1:
                maxheight = heights[j] + 1
    heights[i] = maxheight

def run(m, n, k, keepLast):
    results = []
    for i in range(m):
        results.append([])
    queue = []
    heights = []
    for i in range(1000):
        queue, heights = toss(m, n, k, queue, heights, keepLast)
        for j, height in enumerate(heights):
            results[j].append(height)
    means = []
    for i in range(len(results)):
        means.append(np.mean(results[i]))
    return means[-1]

def main():
    if len(sys.argv) != 2:
        print 'process <result file>'
        sys.exit()
    params = readparams(sys.argv[1])
    compare(params)
    approx(params)

if __name__ == '__main__':
    main()
