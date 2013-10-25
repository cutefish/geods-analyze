"""
Simple tests for fast paxos
"""
import numpy
import sys
import time

from rintvl import RandInterval

infinite = sys.maxint

def runSingleAcceptorFailDiscarded(nprop, mean, lcfg, debug=False):
    #generate X_i
    expoGen = RandInterval.get('expo', mean)
    X = [expoGen.next()]
    for i in range(1, nprop):
        X.append(expoGen.next() + X[i-1])
    #run
    try:
        key, mean, cfg = lcfg
    except:
        key, mean = lcfg
        cfg = {}
    zGen = RandInterval.get(key, mean, cfg)
    A = [0] * nprop
    Y = []
    B = [-1] * nprop
    E = [-1] * nprop
    S = [False] * nprop
    prevTime = time.time()
    for j in range(nprop):
        curr = time.time()
        if curr - prevTime > 5:
            print 'on slot %s'%j
            prevTime = curr
        Y.append([0] * nprop)
        finish = True
        #compute Y
        for i in range(nprop):
            if j == 0:
                Y[j][i] = X[i] + zGen.next()
                finish = False
            elif X[i] > A[j-1]:
                Y[j][i] = X[i] + zGen.next()
                finish = False
            else:
                assert E[i] != -1
                Y[j][i] = infinite
        if finish:
            break
        #compute A
        mval = Y[j][0]
        midx = 0
        for i in range(1, nprop):
            if Y[j][i] < mval:
                mval = Y[j][i]
                midx = i
        assert mval != infinite
        A[j] = mval
        for i in range(nprop):
            if X[i] < A[j] and B[i] == -1:
                B[i] = j
                E[i] = j
        S[midx] = True
        #compute fail latency
        if debug:
            xstrs = ['X:']
            for i in range(nprop):
                xstrs.append('%.2f'%X[i])
            print ' '.join(xstrs)
            ystrs = ['Y:']
            for i in range(nprop):
                if Y[j][i] == infinite:
                    ystrs.append('nan')
                else:
                    ystrs.append('%.2f'%Y[j][i])
            print ' '.join(ystrs)
            print 'A: %.2f'%A[j]
            bstrs = ['B:']
            for i in range(nprop):
                bstrs.append(str(B[i]))
            print ' '.join(bstrs)
            estrs = ['E:']
            for i in range(nprop):
                estrs.append(str(E[i]))
            print ' '.join(estrs)
            sstrs = ['S:']
            for i in range(nprop):
                sstrs.append(str(S[i])[0])
            print ' '.join(sstrs)
            print
    #compute fail rate and fail latency
    failCnt = 0
    succLatencies = []
    failLatencies = []
    for i in range(nprop):
        if S[i] is False:
            failCnt += 1
            failLatencies.append(A[E[i]] - X[i])
        else:
            succLatencies.append(A[E[i]] - X[i])
    print 'fail.prob=%s'%(float(failCnt) / nprop)
    print 'succ.latency.mean=%s'%numpy.mean(succLatencies)
    print 'succ.latency.std=%s'%numpy.std(succLatencies)
    print 'succ.latency.histo=(%s, %s)'%numpy.histogram(succLatencies)
    print 'fail.latency.mean=%s'%numpy.mean(failLatencies)
    print 'fail.latency.std=%s'%numpy.std(failLatencies)
    print 'fail.latency.histo=(%s, %s)'%numpy.histogram(failLatencies)

def runSingleAcceptorFailRestart(nprop, mean, lcfg, debug=False):
    #generate X_i
    expoGen = RandInterval.get('expo', mean)
    X = [expoGen.next()]
    for i in range(1, nprop):
        X.append(expoGen.next() + X[i-1])
    #run
    try:
        key, mean, cfg = lcfg
    except:
        key, mean = lcfg
        cfg = {}
    zGen = RandInterval.get(key, mean, cfg)
    A = [0] * nprop
    Y = []
    B = [-1] * nprop
    E = [-1] * nprop
    prevTime = time.time()
    for j in range(nprop):
        curr = time.time()
        if curr - prevTime > 5:
            print 'on slot %s'%j
            prevTime = curr
        Y.append([0] * nprop)
        #compute Y
        for i in range(nprop):
            if j == 0:
                Y[j][i] = X[i] + zGen.next()
            #elif Y[j-1][i] == infinite:
            #    Y[j][i] = infinite
            #elif Y[j-1][i] == A[j-1]:
            #    Y[j][i] = infinite
            elif E[i] != -1:
                assert j > E[i]
                Y[j][i] = infinite
            elif X[i] > A[j-1]:
                Y[j][i] = X[i] + zGen.next()
            else:
                assert X[i] <= A[j-1] and Y[j-1][i] >= A[j-1], \
                        ('X[%s] = %s <= %s = A[%s-1] and '
                         'Y[%s-1][%s] =%s > %s = A[%s-1]'
                         %(i, X[i], A[j-1], j, j, i, Y[j-1][i], A[j-1],j))
                Y[j][i] = A[j-1] + zGen.next()
        #compute A and R
        mval = Y[j][0]
        midx = 0
        for i in range(1, nprop):
            if Y[j][i] < mval:
                mval = Y[j][i]
                midx = i
        assert mval != infinite
        A[j] = mval
        for i in range(nprop):
            if X[i] < A[j] and B[i] == -1:
                B[i] = j
        E[midx] = j
        if debug:
            xstrs = ['X:']
            for i in range(nprop):
                xstrs.append('%.2f'%X[i])
            print ' '.join(xstrs)
            ystrs = ['Y:']
            for i in range(nprop):
                if Y[j][i] == infinite:
                    ystrs.append('nan')
                else:
                    ystrs.append('%.2f'%Y[j][i])
            print ' '.join(ystrs)
            print 'A: %.2f'%A[j]
            bstrs = ['B:']
            for i in range(nprop):
                bstrs.append(str(B[i]))
            print ' '.join(bstrs)
            estrs = ['E:']
            for i in range(nprop):
                estrs.append(str(E[i]))
            print ' '.join(estrs)
            print
    #compute stats
    if debug:
        lstrs = ['ET:']
        for i in range(nprop):
            lstrs.append('%.2f'%Y[E[i]][i])
        print ' '.join(lstrs)
    R = [0] * nprop
    T = [0] * nprop
    S = []
    F = []
    for i in range(nprop):
        R[i] = E[i] - B[i]
        T[i] = Y[E[i]][i] - X[i]
        if B[i] != E[i]:
            F.append(A[B[i]] - X[i])
        for k in range(B[i] + 1, E[i]):
            F.append(A[k] - A[k-1])
        if B[i] == E[i]:
            S.append(A[E[i]] - X[i])
        else:
            S.append(A[E[i]] - A[E[i] - 1])
    if debug:
        lstrs = ['L:']
        for i in range(nprop):
            lstrs.append('%.2f'%L[i])
        print ' '.join(lstrs)
    print 'nretries.mean=%s'%numpy.mean(R)
    print 'nretries.std=%s'%numpy.std(R)
    print 'nretries.histo=(%s, %s)'%numpy.histogram(R)
    print 'total.latency.mean=%s'%numpy.mean(T)
    print 'total.latency.std=%s'%numpy.std(T)
    print 'total.latency.histo=(%s, %s)'%numpy.histogram(T)
    print 'succ.latency.mean=%s'%numpy.mean(S)
    print 'succ.latency.std=%s'%numpy.std(S)
    print 'succ.latency.histo=(%s, %s)'%numpy.histogram(S)
    print 'fail.latency.mean=%s'%numpy.mean(F)
    print 'fail.latency.std=%s'%numpy.std(F)
    print 'fail.latency.histo=(%s, %s)'%numpy.histogram(F)


def main():
    if len(sys.argv) < 5:
        print
        print 'fastpaxos <key> <num proposal> <mean> <lcfg> [--debug]'
        print '          key: '
        print '                 safd    -- runSingleAcceptorFailDiscarded'
        print '                 safr    -- runSingleAcceptorFailRestart'
        print
        sys.exit(-1)
    debug = False
    if '--debug' in sys.argv:
        debug = True
        sys.argv.remove('--debug')
    key = sys.argv[1]
    nprop = int(sys.argv[2])
    mean = float(sys.argv[3])
    lcfg = eval(sys.argv[4])
    if key == 'safd':
        runSingleAcceptorFailDiscarded(nprop, mean, lcfg, debug)
    elif key == 'safr':
        runSingleAcceptorFailRestart(nprop, mean, lcfg, debug)

if __name__ == '__main__':
    main()
