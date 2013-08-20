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

def generate(xgen, ntry, n):
    """Calculate Y_n:
        xgen    -- X_n generator
        ntry    -- number of tries
        n       -- total number
    """
    xx = []
    yy = []
    f = (n + 1) / 2
    for i in range(ntry):
        ixx = []
        for j in range(n):
            x = xgen.generate()
            xx.append(x)
            ixx.append(x)
        yy.append(np.median(ixx))
    return xx, yy

def run(ntry, n, config):
    genClass = XGENS[config['xgen']]
    xgen = genClass(config)
    xx, yy = generate(xgen, ntry, n)
    fig = plt.figure()
    axes = fig.add_subplot(211)
    axes.hist(xx, bins=20)
    axes = fig.add_subplot(212)
    axes.hist(yy, bins=20)
    fig.savefig('../tmp/msgpass.pdf')
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

def main():
    if len(sys.argv) != 5 and len(sys.argv) != 3:
        print 'msgpass <sim> <ntry> <n> <config>'
        print 'or'
        print 'msgpass <ana> <eval expr>'
        sys.exit()
    if sys.argv[1] == 'sim':
        ntry = int(sys.argv[2])
        n = int(sys.argv[3])
        config = parse(sys.argv[4])
        run(ntry, n, config)
    elif sys.argv[1] == 'ana':
        print eval(sys.argv[2])
    else:
        print 'msgpass <sim> <num> <d> <config>'
        print 'or'
        print 'msgpass <ana> <eval expr>'
        sys.exit()

if __name__ == '__main__':
    main()
