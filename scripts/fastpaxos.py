"""
Simple tests for fast paxos
"""
import sys

from rintvl import RandInterval

infinite = sys.maxint

def runSingleAcceptorFailDiscarded(np, mean, lcfg, debug=False):
    #generate X_i
    expoGen = RandInterval.get('expo', mean)
    X = [expoGen.next()]
    for i in range(1, np):
        X.append(expoGen.next() + X[i-1])
    #run
    try:
        key, mean, cfg = lcfg
    except:
        key, mean = lcfg
        cfg = {}
    zGen = RandInterval.get(key, mean, cfg)
    A = [0] * np
    Y = []
    L = [0] * np
    F = [True] * np
    for j in range(np):
        Y.append([0] * np)
        finish = True
        for i in range(np):
            if j == 0 and (Y[j-1][i] != infinite or X[i] < A[j-1]):
                Y[j][i] = X[i] + zGen.next()
                L[i] = Y[j][i]
                finish = False
            else:
                Y[j][i] = infinite
        mval = Y[j][0]
        midx = 0
        for i in range(1, np):
            if Y[j][i] < mval:
                mval = Y[j][i]
                midx = i
        A[j] = mval
        F[midx] = False
        if debug:
            xstrs = ['X:']
            for i in range(np):
                xstrs.append('%.2f'%X[i])
            print ' '.join(xstrs)
            ystrs = ['Y:']
            for i in range(np):
                if Y[j][i] == infinite:
                    ystrs.append('nan')
                else:
                    ystrs.append('%.2f'%Y[j][i])
            print ' '.join(ystrs)
            print 'A: %.2f'%A[j]
            fstrs = ['F:']
            for i in range(np):
                fstrs.append(str(F[i])[0])
            print ' '.join(fstrs)
        if finish:
            break

def main():
    if len(sys.argv) < 5:
        print
        print 'fastpaxos <key> <num proposal> <mean> <lcfg> [--debug]'
        print '          key: '
        print '                 safd    -- runSingleAcceptorFailDiscarded'
        print
        sys.exit(-1)
    debug = False
    if '--debug' in sys.argv:
        debug = True
        sys.argv.remove('--debug')
    key = sys.argv[1]
    np = int(sys.argv[2])
    mean = float(sys.argv[3])
    lcfg = eval(sys.argv[4])
    if key == 'safd':
        runSingleAcceptorFailDiscarded(np, mean, lcfg, debug)

if __name__ == '__main__':
    main()
