"""
Simple tests for fast paxos
"""
import numpy
import sys
import time

from rintvl import RandInterval

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
    curr = time.time()
    if curr - prevTime > 5:
        print 'on slot %s'%j
        prevtime = curr

def computeY(j, X, Y, A, V, B, E, lGen):
    finish = True
    for i in range(len(Y[j])):
        if E[i] != -1:
            assert j > E[i]
            Y[j][i] = infinite
        elif B[i] != -1:
            assert j > B[i]
            assert X[i] <= V[j-1][i] and Y[j-1][i] >= A[j-1], \
                    ('X[%s] = %s <= %s = V[%s-1][%s] and '
                     'Y[%s-1][%s] =%s >= %s = A[%s-1] '
                     'j=%s, B[%s]=%s'
                     %(i, X[i], V[j-1][i], j, i, j, i, Y[j-1][i], A[j-1],j,
                       j, i, B[i]))
            Y[j][i] = V[j-1][i] + lGen.next()
        else:
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
        finish = computeY(j, X, Y, A, V, B, E, lGen)
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

def runSingleAcceptorFailRestart(nprop, mean, lcfg, debug=False):
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
        computeY(j, X, Y, A, V, B, E, lGen)
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
