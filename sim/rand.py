import sys
import random

class ExpoInterval(object):
    def __init__(self, mean, config):
        self.lb = config.get('lb', 0)
        self.ub = config.get('ub', sys.maxint)
        self.lambd = float(1) / float(mean)

    def next(self):
        ret = random.expovariate(self.lambd)
        while self.lb > ret or ret > self.ub:
            ret = random.expovariate(self.lambd)
        return ret

class NormInterval(object):
    def __init__(self, mean, config):
        self.lb = config.get('lb', 0)
        self.ub = config.get('ub', sys.maxint)
        self.mu = mean
        self.sigma = config.get('sigma', 0)

    def next(self):
        ret = random.normalvariate(self.mu, self.sigma)
        while self.lb > ret or ret > self.ub:
            ret = random.normalvariate(self.mu, self.sigma)
        return ret

class FixedInterval(object):
    def __init__(self, mean, config):
        self.value = mean

    def next(self):
        return self.value

class UniformInterval(object):
    def __init__(self, mean, config):
        self.lb = config.get('lb', 0)
        self.ub = config.get('ub', sys.maxint)
        self.span = float(self.ub - self.lb)

    def next(self):
        return self.lb + random.random() * self.span

class LogNormalInterval(object):
    def __init__(self, mean, config):
        self.lb = config.get('lb', 0)
        self.ub = config.get('ub', sys.maxint)
        self.mu = float(config['mu'])
        self.sigma = float(config['sigma'])

    def next(self):
        ret =  random.lognormvariate(self.mu, self.sigma)
        while self.lb > ret or ret > self.ub:
            ret = random.lognormvariate(self.mu, self.sigma)
        return ret

class ParetoInterval(object):
    def __init__(self, mean, config):
        """Shifted type II pareto.

        pdf = a / (x - inf + 1)**(a + 1)
        mean = 1 / (a - 1) + inf

        """
        self.lb = config.get('lb', 0)
        self.ub = config.get('ub', sys.maxint)
        self.a = float(config['a'])

    def next(self):
        ret = random.paretovariate(self.a) + self.lb
        while self.lb > ret or ret > self.ub:
            ret = random.paretovariate(self.a) + self.lb
        return ret

class DDistInterval(object):
    def __init__(self, mean, config):
        self.values = config['values']
        self.probs = config['probs']
        if len(self.values) != len(self.probs):
            raise ValueError('len(values) = %s == %s = len(probs)'
                             %(len(self.values), len(self.probs)))
        self.bins = self._getBins()

    def _getBins(self):
        #normalize
        s = 0
        for p in self.probs:
            s += p
        for i, p in enumerate(self.probs):
            self.probs[i] = float(p) / s
        #get bins
        bins = [0.0]
        for p in self.probs:
            bins.append(p + bins[-1])
        bins.pop(0)
        return bins

    def next(self):
        r = random.random()
        for i, b in enumerate(self.bins):
            if r <= b:
                return self.values[i]
        assert False

DISTRIBUTIONS = {
    'expo': ExpoInterval,
    'norm': NormInterval,
    'fixed': FixedInterval,
    'uniform': UniformInterval,
    'lognorm' : LogNormalInterval,
    'pareto' : ParetoInterval,
    'ddist' : DDistInterval,
}

class RandInterval:
    @classmethod
    def get(cls, key, mean, config={}):
        return DISTRIBUTIONS[key](mean, config)

    @classmethod
    def generate(cls, key, mean, config={}, nrun=1000):
        rintval = RandInterval.get(key, mean, config)
        values = []
        for i in range(nrun):
            values.append(rintval.next())
        return values

### test ###
import numpy as np

def main():
    ntests = 10000
    #exponential distribution
    print 'test expo: 100'
    values = RandInterval.generate('expo', 100, nrun=ntests)
    print 'mean:', np.mean(values)
    print 'std:', np.std(values)
    #normal distribution
    print 'test norm: mean, 100, sigma, 50'
    values = RandInterval.generate('norm', 100, {'sigma': 50}, ntests)
    print 'mean:', np.mean(values)
    print 'std:', np.std(values)
    #fixed distribution
    print 'test fixed: mean, 100'
    values = RandInterval.generate('fixed', 100, nrun=ntests)
    print 'mean:', np.mean(values)
    print 'std:', np.std(values)
    #uniform distribution
    print 'test uniform: mean, 100'
    values = RandInterval.generate('uniform', -1, {'lb':50, 'ub':150}, ntests)
    print 'mean:', np.mean(values)
    print 'std:', np.std(values)
    #lognormal distribution
    print 'test lognormal: mean, 100'
    values = RandInterval.generate('lognorm', -1, {'mu':100, 'sigma':50}, ntests)
    print 'mean:', np.mean(values)
    print 'std:', np.std(values)
    #pareto distribution
    print 'test pareto: mean, 100'
    values = RandInterval.generate('pareto', -1, {'lb': 100, 'a':3}, ntests)
    print 'mean:', np.mean(values)
    print 'std:', np.std(values)
    #pareto distribution
    print 'test ddist: mean 2, std %s'%(np.sqrt(2.0 / 3))
    values = RandInterval.generate('ddist', -1,
                              {'values': [1, 2, 3], 'probs':[1, 1, 1]}, ntests)
    print 'mean:', np.mean(values)
    print 'std:', np.std(values)

if __name__ == '__main__':
    main()



