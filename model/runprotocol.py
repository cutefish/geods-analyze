import sys

from model.ddist import DDist
from model.protocol import getSLPLatencyDist
from model.protocol import getFPLatencyDist
from model.protocol import getEBLatencyDist

def calcSLPMean(n, nwcfgstr, cfgstr):
    n = int(n)
    nwcfg = eval(nwcfgstr)
    cfg = eval(cfgstr)
    ddist = DDist.sample(nwcfg, h=0.5)
    lambd = 1.0 / cfg['1/lambd']
    res, delay, rtrip = getSLPLatencyDist(n, ddist, lambd)
    print 'res.mean', res.mean
    print 'res.std', res.std
    print 'delay.mean', delay.mean
    print 'delay.std', delay.std
    print 'rtrip.mean', rtrip.mean
    print 'rtrip.std', rtrip.std

def main():
    if len(sys.argv) != 5:
        print
        print 'runprotocol <key> <n> <nwcfg> <cfg>'
        sys.exit(1)
    key = sys.argv[1]
    n = sys.argv[2]
    nwcfg = sys.argv[3]
    cfg = sys.argv[4]
    if key == 'slp':
        calcSLPMean(n, nwcfg, cfg)
    else:
        print 'unknown key: %s'%key
        sys.exit(-1)

if __name__ == '__main__':
    main()
