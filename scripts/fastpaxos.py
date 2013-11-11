"""
Simple tests for fast paxos
"""
import numpy
import sys
import time

from rintvl import RandInterval

from model.ddist import DDist
from model.protocol import getFPLatencyDist

infinite = sys.maxint
prevtime = time.time()

def getGenerator(gcfg):
    try:
        key, mean, cfg = gcfg
    except:
        key, mean = gcfg
        cfg = {}
    return RandInterval.get(key, mean, cfg)

def printSlot(j):
    global prevtime
    curr = time.time()
    if curr - prevtime > 5:
        print 'on slot %s'%j
        prevtime = curr

def computeYInd(j, X, Y, A, V, B, E, lGen):
    finish = True
    for i in range(len(Y[j])):
        if E[i] != -1:
            #if already end
            assert j > E[i]
            Y[j][i] = infinite
        elif X[i] <= V[j-1][i]:
            assert B[i] != -1 and j > B[i]
            assert Y[j-1][i] >= A[j-1], \
                    ('Y[%s-1][%s] =%s >= %s = A[%s-1] '
                     'j=%s, B[%s]=%s'
                     %(j, i, Y[j-1][i], A[j-1], j, j, i, B[i]))
            Y[j][i] = V[j-1][i] + lGen.next()
        else:
            Y[j][i] = X[i] + lGen.next()
            finish = False
    return finish

def computeYNImm(j, X, Y, A, V, B, E, lGen):
    finish = True
    for i in range(len(Y[j])):
        if E[i] != -1:
            #already end
            assert j > E[i]
            Y[j][i] = infinite
        elif B[i] != -1:
            #already begin but failed
            assert j > B[i]
            Y[j][i] = V[j-1][i] + lGen.next()
            finish = False
        else:
            #not begin but possbily begin on this slot
            Y[j][i] = X[i] + lGen.next()
            finish = False
    return finish

def computeA(j, Y, A):
    mval = Y[j][0]
    midx = 0
    for i in range(1, len(Y[j])):
        if Y[j][i] < mval:
            mval = Y[j][i]
            midx = i
    assert mval != infinite
    A[j] = mval
    return mval, midx

def printXYAVBE(j, X, Y, A, V, B, E):
    nprop = len(X)
    strs = ['X:']
    for i in range(nprop):
        strs.append('%.2f'%X[i])
    print ' '.join(strs)
    strs = ['Y:']
    for i in range(nprop):
        if Y[j][i] == infinite:
            strs.append('nan')
        else:
            strs.append('%.2f'%Y[j][i])
    print ' '.join(strs)
    print 'A: %.2f'%A[j]
    strs = ['V:']
    for i in range(nprop):
        strs.append('%.2f'%V[j][i])
    print ' '.join(strs)
    strs = ['B:']
    for i in range(nprop):
        strs.append(str(B[i]))
    print ' '.join(strs)
    strs = ['E:']
    for i in range(nprop):
        strs.append(str(E[i]))
    print ' '.join(strs)

def runSingleAcceptorFailDiscarded(nprop, mean, lcfg, debug=False):
    #generate X_i
    expoGen = RandInterval.get('expo', mean)
    X = [expoGen.next()]
    for i in range(1, nprop):
        X.append(expoGen.next() + X[i-1])
    #run
    lGen = getGenerator(lcfg)
    A = [0] * nprop
    Y = []
    V = []
    B = [-1] * nprop
    E = [-1] * nprop
    S = [None] * nprop
    for j in range(nprop):
        Y.append([0] * nprop)
        V.append([0] * nprop)
        finish = True
        #compute Y
        finish = computeYInd(j, X, Y, A, V, B, E, lGen)
        if finish:
            break
        #compute A
        mval, midx = computeA(j, Y, A)
        #compute V
        for i in range(nprop):
            V[j][i] = A[j] + lGen.next()
        #compute B and E
        for i in range(nprop):
            if X[i] < V[j][i] and B[i] == -1:
                B[i] = j
                E[i] = j
                S[i] = False
        S[midx] = True
        if debug:
            printXYAVBE(j, X, Y, A, V, B, E)
            strs = ['S:']
            for i in range(nprop):
                strs.append(str(S[i])[0])
            print ' '.join(strs)
            print
    #compute fail rate and fail latency
    failCnt = 0
    succLatencies = []
    failLatencies = []
    for i in range(nprop):
        if S[i] is False:
            failCnt += 1
            failLatencies.append(V[E[i]][i] - X[i])
        else:
            succLatencies.append(V[E[i]][i] - X[i])
    print 'fail.prob=%s'%(float(failCnt) / nprop)
    print 'succ.latency.mean=%s'%numpy.mean(succLatencies)
    print 'succ.latency.std=%s'%numpy.std(succLatencies)
    print 'succ.latency.histo=(%s, %s)'%numpy.histogram(succLatencies)
    print 'fail.latency.mean=%s'%numpy.mean(failLatencies)
    print 'fail.latency.std=%s'%numpy.std(failLatencies)
    print 'fail.latency.histo=(%s, %s)'%numpy.histogram(failLatencies)

def runSingleAcceptorFailRestartInd(nprop, mean, lcfg, debug=False):
    #generate X_i
    expoGen = RandInterval.get('expo', mean)
    X = [expoGen.next()]
    for i in range(1, nprop):
        X.append(expoGen.next() + X[i-1])
    #run
    lGen = getGenerator(lcfg)
    A = [0] * nprop
    Y = []
    V = []
    B = [-1] * nprop
    E = [-1] * nprop
    prevTime = time.time()
    for j in range(nprop):
        curr = time.time()
        if curr - prevTime > 5:
            print 'on slot %s'%j
            prevTime = curr
        Y.append([0] * nprop)
        V.append([0] * nprop)
        #compute Y
        computeYInd(j, X, Y, A, V, B, E, lGen)
        #compute A
        mval, midx = computeA(j, Y, A)
        #compute V
        for i in range(nprop):
            V[j][i] = A[j] + lGen.next()
        #compute B and E
        for i in range(nprop):
            if X[i] < V[j][i] and B[i] == -1:
                B[i] = j
        E[midx] = j
        if debug:
            printXYAVBE(j, X, Y, A, V, B, E)
            print
    #compute stats
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
        strs = ['Yk:']
        for i in range(nprop):
            strs.append('%.2f'%Y[E[i]][i])
        print ' '.join(strs)
        strs = ['T:']
        for i in range(nprop):
            strs.append('%.2f'%T[i])
        print ' '.join(strs)
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

def runSingleAcceptorFailRestartNImm(nprop, mean, lcfg, debug=False):
    #generate X_i
    expoGen = RandInterval.get('expo', mean)
    X = [expoGen.next()]
    for i in range(1, nprop):
        X.append(expoGen.next() + X[i-1])
    #run
    lGen = getGenerator(lcfg)
    A = [0] * nprop
    Y = []
    V = []
    B = [-1] * nprop
    E = [-1] * nprop
    prevTime = time.time()
    for j in range(nprop):
        curr = time.time()
        if curr - prevTime > 5:
            print 'on slot %s'%j
            prevTime = curr
        Y.append([0] * nprop)
        V.append([0] * nprop)
        #compute Y
        computeYNImm(j, X, Y, A, V, B, E, lGen)
        #compute A
        mval, midx = computeA(j, Y, A)
        #compute V
        for i in range(nprop):
            V[j][i] = A[j] + lGen.next()
        #compute B and E
        for i in range(nprop):
            if X[i] < A[j] and B[i] == -1:
                B[i] = j
        E[midx] = j
        if debug:
            printXYAVBE(j, X, Y, A, V, B, E)
            print
    #compute stats
    R = [0] * nprop
    T = [0] * nprop
    S = []
    F = []
    for i in range(nprop):
        R[i] = E[i] - B[i]
        T[i] = V[E[i]][i] - X[i]
        if B[i] != E[i]:
            F.append(V[B[i]][i] - X[i])
        for k in range(B[i] + 1, E[i]):
            F.append(V[k][i] - V[k-1][i])
        if B[i] == E[i]:
            S.append(V[E[i]][i] - X[i])
        else:
            S.append(V[E[i]][i] - V[E[i] - 1][i])
    if debug:
        strs = ['Yk:']
        for i in range(nprop):
            strs.append('%.2f'%Y[E[i]][i])
        print ' '.join(strs)
        strs = ['T:']
        for i in range(nprop):
            strs.append('%.2f'%T[i])
        print ' '.join(strs)
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

def runModel(n, mean, lcfg, debug):
    ddist = DDist.sample(lcfg)
    lambd = 1.0 / mean
    res, eN = getFPLatencyDist(n, ddist, lambd)
    print 'system.num.proposal.mean=%s'%(eN)
    print 'res.mean=%s'%(res)

def main():
    if len(sys.argv) < 5:
        print
        print 'fastpaxos <key> <num proposal> <mean> <lcfg> [--debug]'
        print '          key: '
        print '                 safd    -- runSingleAcceptorFailDiscarded'
        print '                 safri   -- runSingleAcceptorFailRestartInd'
        print '                 safrm   -- runSingleAcceptorFailRestartMax'
        print '                 safrn   -- runSingleAcceptorFailRestartNImm'
        print '                 rm      -- runModel'
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
    elif key == 'safri':
        runSingleAcceptorFailRestartInd(nprop, mean, lcfg, debug)
    elif key == 'safrn':
        runSingleAcceptorFailRestartNImm(nprop, mean, lcfg, debug)
    elif key == 'rm':
        runModel(nprop, mean, lcfg, debug)
    else:
        raise ValueError('unknown key:%s'%key)

if __name__ == '__main__':
    main()
