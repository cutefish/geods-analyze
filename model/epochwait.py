"""
Shifted iid Extreme Value.

Y_n = max(X_n, X_{n-1} - d, X_{n-2} - 2 * d, ..., X_0 - n * d)
"""

import sys

import numpy as np
import matplotlib
matplotlib.use('pdf')
import matplotlib.pylab as plt

class XgenDist(object):
    def __init__(self, config):
        self.sup = float(config['sup'])
        self.inf = float(config['inf'])

class LogNormal(XgenDist):
    def __init__(self, config):
        XgenDist.__init__(self, config)
        self.mu = float(config['mu'])
        self.sigma = float(config['sigma'])

    def generate(self):
        x =  np.random.lognormal(self.mu, self.sigma)
        if x < self.inf:
            x = np.random.uniform(self.inf, self.sup)
        elif x > self.sup:
            x = self.sup
        return x

class Pareto(XgenDist):
    def __init__(self, config):
        XgenDist.__init__(self, config)
        self.a = float(config['a'])

    def generate(self):
        x = np.random.pareto(self.a) + self.inf
        if x > self.sup:
            x = self.sup
        return x

class TwoStep(XgenDist):
    def __init__(self, config):
        XgenDist.__init__(self, config)
        self.p = float(config['p'])

    def generate(self):
        r = np.random.random()
        if r < self.p:
            return self.inf
        else:
            return self.sup

class ThreeStep(XgenDist):
    def __init__(self, config):
        XgenDist.__init__(self, config)
        self.p1 = float(config['p1'])
        self.p2 = float(config['p2'])
        self.m = float(config['m'])

    def generate(self):
        r = np.random.random()
        if r < self.p1:
            return self.inf
        elif r < self.p2:
            return self.m
        else:
            return self.sup

XGENS = {
    'lognormal': LogNormal,
    'pareto' : Pareto,
    'twostep' : TwoStep,
    'threestep' : ThreeStep,
}

def generate(xgen, num, d):
    """Calculate Y_n:
        xgen    -- X_n generator
        num     -- n
        d       -- d
        xsup    -- sup(X_n), to decrease complexity

    """
    xx = []
    yy = []
    for i in range(num):
        xx.append(xgen.generate())
    for i in range(num):
        xmax = 0
        for j in range(i, -1, -1):
            if (i - j) * d > xgen.sup:
                break
            y = xx[j] - (i - j) * d
            if xmax < y:
                xmax = y
        yy.append(xmax)
    return xx, yy

def run(num, d, config):
    genClass = XGENS[config['xgen']]
    xgen = genClass(config)
    xx, yy = generate(xgen, num, d)
    fig = plt.figure()
    axes = fig.add_subplot(211)
    axes.hist(xx, bins=20)
    axes = fig.add_subplot(212)
    axes.hist(yy, bins=20)
    fig.savefig('../tmp/sevt.pdf')
    print np.mean(xx)
    print np.mean(yy)

def parse(string):
    config = {}
    strings = string.split(',')
    for cfgstr in strings:
        key, val = cfgstr.split('=')
        key = key.strip()
        val = eval(val)
        config[key] = val
    return config

def tstepPdfSim(a, b, e, p):
    alpha = (b - a) / e
    s = a * p ** alpha
    for i in range(1, alpha + 1):
        s += (a + i * e) * (p**(alpha - i) - p**(alpha - i + 1))
    return s

def tstepPdfAna(a, b, e, p):
    alpha = (b - a) / e
    return b - e * p * (1 - p**alpha) / (1 - p)

def thstepPdfSim(a, b, e, p1, p2, m):
    alpha = (m - a) / e
    beta = (b - m) / e
    s = a * p1**alpha * p2**beta
    for i in range(1, alpha + 1):
        s += (a + i * e) * p2**beta * (p1**(alpha - i) - p1**(alpha - i + 1))
    for i in range(1, beta + 1):
        s += (a + alpha * e + i * e) * (p2**(beta - i) - p2 **(beta - i + 1))
    return s

def thstepPdfAna(a, b, e, p1, p2, m):
    alpha = (m - a) / e
    beta = (b - m) / e
    return b - e * p2**beta * p1 * (1 - p1**alpha) / (1 - p1) - \
            e * p2 * (1 - p2**beta) / (1 - p2)

def main():
    #print tstepPdfSim(100, 1000, 10, 0.9)
    #print tstepPdfAna(100, 1000, 10, 0.9)
    if len(sys.argv) != 5 and len(sys.argv) != 3:
        print 'epochwait <sim> <num> <d> <config>'
        print 'or'
        print 'epochwait <ana> <eval expr>'
        sys.exit()
    if sys.argv[1] == 'sim':
        num = int(sys.argv[2])
        d = float(sys.argv[3])
        config = parse(sys.argv[4])
        run(num, d, config)
    elif sys.argv[1] == 'ana':
        print eval(sys.argv[2])
    else:
        print 'epochwait <sim> <num> <d> <config>'
        print 'or'
        print 'epochwait <ana> <eval expr>'
        sys.exit()

if __name__ == '__main__':
    main()
