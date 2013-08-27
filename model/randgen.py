
import numpy as np

class RandDist(object):
    def __init__(self, config):
        try:
            self.sup = float(config['sup'])
        except KeyError:
            self.sup = None
        try:
            self.inf = float(config['inf'])
        except KeyError:
            self.inf = None

class Normal(RandDist):
    def __init__(self, config):
        RandDist.__init__(self, config)
        self.mu = float(config['mu'])
        self.sigma = float(config['sigma'])

    def generate(self):
        x =  np.random.normal(self.mu, self.sigma)
        if self.inf is not None and x < self.inf:
            x = np.random.uniform(self.inf, self.sup)
        elif self.sup is not None and x > self.sup:
            x = np.random.uniform(self.inf, self.sup)
        return x

class LogNormal(RandDist):
    def __init__(self, config):
        RandDist.__init__(self, config)
        self.mu = float(config['mu'])
        self.sigma = float(config['sigma'])

    def generate(self):
        x =  np.random.lognormal(self.mu, self.sigma)
        if self.inf is not None and x < self.inf:
            x = np.random.uniform(self.inf, self.sup)
        elif self.sup is not None and x > self.sup:
            x = np.random.uniform(self.inf, self.sup)
        return x

class Pareto(RandDist):
    def __init__(self, config):
        """Shifted type II pareto.

        pdf = a / (x - inf + 1)**(a + 1)
        mean = 1 / (a - 1) + inf

        """
        RandDist.__init__(self, config)
        self.a = float(config['a'])

    def generate(self):
        x = np.random.pareto(self.a) + self.inf
        if self.sup is not None and x > self.sup:
            x = self.sup
        return x

class TwoStep(RandDist):
    def __init__(self, config):
        RandDist.__init__(self, config)
        self.p = float(config['p'])

    def generate(self):
        r = np.random.random()
        if r < self.p:
            return self.inf
        else:
            return self.sup

class ThreeStep(RandDist):
    def __init__(self, config):
        RandDist.__init__(self, config)
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

class RVGen(object):
    DISTS = {
        'normal' : Normal,
        'lognormal': LogNormal,
        'pareto' : Pareto,
        'twostep' : TwoStep,
        'threestep' : ThreeStep,
    }
    RV_NAME_KEY = 'rv.name'
    @classmethod
    def run(cls, config, num):
        rv = RVGen.DISTS[config[RVGen.RV_NAME_KEY]](config)
        x = []
        for i in range(num):
            x.append(rv.generate())
        return x

