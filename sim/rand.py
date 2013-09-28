import sys
import random

import sim

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

DISTRIBUTIONS = {
    'expo': ExpoInterval,
    'norm': NormInterval,
    'fixed': FixedInterval,
    'uniform': UniformInterval,
}

class RandInterval:
    @classmethod
    def get(cls, key, mean, config={}):
        return DISTRIBUTIONS[key](mean, config)

### test ###
def main():
    for i in range(10):
        print RandInterval.get('expo', 100).next()
    print
    for i in range(10):
        print RandInterval.get('norm', 100, {'sigma':10}).next()

if __name__ == '__main__':
    main()



