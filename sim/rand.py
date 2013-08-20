import random

class RandInterval:
    MIN_INTERVAL = 0.01

    @classmethod
    def get(cls, key, mean, kargs={}):
        DIST_METHODS = {
            'expo' : RandInterval.expo,
            'norm' : RandInterval.norm,
            'fix'  : RandInterval.fix,
        }
        return DIST_METHODS[key](mean, kargs)

    @classmethod
    def expo(cls, mean, kargs):
        lambd = float(1) / float(mean)
        ret = random.expovariate(lambd)
        if ret > cls.MIN_INTERVAL:
            return ret
        return cls.MIN_INTERVAL

    @classmethod
    def norm(cls, mean, kargs):
        sigma = kargs.get('sigma', 0)
        ret = random.normalvariate(mean, sigma)
        if ret > cls.MIN_INTERVAL:
            return ret
        return cls.MIN_INTERVAL

    @classmethod
    def fix(cls, mean, kargs):
        return mean

### test ###
def main():
    for i in range(10):
        print RandInterval.get('expo', 100)
    print
    for i in range(10):
        print RandInterval.get('norm', 100, {'sigma':10})

if __name__ == '__main__':
    main()



