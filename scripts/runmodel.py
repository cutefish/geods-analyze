import sys

import matplotlib
matplotlib.use('pdf')
import matplotlib.pylab as plt

from tpc import runModel as runTPCModel
from tide import runModel as runTideModel

#runTPCModel(N, D, d, a, l, r, b, eta)
#runTideModel(N, D, d, a, l, r, e)

def impactOfLambda(outdir):
    N = 64
    D = 4096
    d = 32
    l = 80
    r = 10
    b = 100
    eta = 0
    e = 20
    X = [20, 40, 60, 80, 100]
    tpcY = []
    tideY = []
    for a in X:
        nw, nr, nq, loss, ts = runTPCModel(N, D, d, a, l, r, b, eta)
        tpcY.append(ts)
        nw, nr, nq, loss, ts = runTideModel(N, D, d, a, l, r, e)
        tideY.append(ts)
    fig = plt.figure()
    axes = fig.add_subplot(111)
    ltpc = axes.plot(X, tpcY, '-ob')
    ltide = axes.plot(X, tideY, '-+r')
    axes.set_xlabel('Average Txn Arrive Interval')
    axes.set_ylabel('Average Txn Service Time')
    fig.legend((ltpc, ltide), ('tpc', 'tide'))
    fig.savefig('%s/impact_of_lambda.pdf'%outdir)

def impactOfNumItems(outdir):
    N = 64
    D = 4096
    a = 100
    l = 80
    r = 10
    b = 100
    eta = 0
    e = 20
    X = [2, 4, 8, 16, 32, 64]
    tpcY = []
    tideY = []
    for d in X:
        nw, nr, nq, loss, ts = runTPCModel(N, D, d, a, l, r, b, eta)
        tpcY.append(ts)
        nw, nr, nq, loss, ts = runTideModel(N, D, d, a, l, r, e)
        tideY.append(ts)
    fig = plt.figure()
    axes = fig.add_subplot(111)
    ltpc = axes.plot(X, tpcY, '-ob')
    ltide = axes.plot(X, tideY, '-+r')
    axes.set_xlabel('Number of Items Per Txn')
    axes.set_ylabel('Average Txn Service Time')
    fig.legend((ltpc, ltide), ('tpc', 'tide'))
    fig.savefig('%s/impact_of_num_items.pdf'%outdir)

def impactOfExecTime(outdir):
    N = 64
    D = 4096
    d = 32
    a = 100
    l = 10
    b = 100
    eta = 0
    e = 20
    X = [10, 20, 30, 40, 50]
    tpcY = []
    tideY = []
    for r in X:
        nw, nr, nq, loss, ts = runTPCModel(N, D, d, a, l, r, b, eta)
        tpcY.append(ts)
        nw, nr, nq, loss, ts = runTideModel(N, D, d, a, l, r, e)
        tideY.append(ts)
    fig = plt.figure()
    axes = fig.add_subplot(111)
    ltpc = axes.plot(X, tpcY, '-ob')
    ltide = axes.plot(X, tideY, '-+r')
    axes.set_xlabel('Average Txn Execution Time')
    axes.set_ylabel('Average Txn Service Time')
    fig.legend((ltpc, ltide), ('tpc', 'tide'))
    fig.savefig('%s/impact_of_exec_time.pdf'%outdir)

def impactOfMaxNumTxns(outdir):
    D = 4096
    d = 32
    a = 100
    l = 80
    r = 10
    b = 100
    eta = 0
    e = 20
    X = [2, 4, 8, 16, 32, 64]
    tpcY = []
    tideY = []
    for N in X:
        nw, nr, nq, loss, ts = runTPCModel(N, D, d, a, l, r, b, eta)
        tpcY.append(ts)
        nw, nr, nq, loss, ts = runTideModel(N, D, d, a, l, r, e)
        tideY.append(ts)
    fig = plt.figure()
    axes = fig.add_subplot(111)
    ltpc = axes.plot(X, tpcY, '-ob')
    ltide = axes.plot(X, tideY, '-+r')
    axes.set_xlabel('Max Number of Txns')
    axes.set_ylabel('Average Txn Service Time')
    fig.legend((ltpc, ltide), ('tpc', 'tide'))
    fig.savefig('%s/impact_of_max_num_txns.pdf'%outdir)

def impactOfBackoffTime(outdir):
    N = 64
    D = 4096
    d = 32
    a = 100
    l = 80
    r = 10
    eta = 0
    e = 20
    X = [10, 30, 50, 70, 90, 110, 130, 150]
    tpcY = []
    tideY = []
    for b in X:
        nw, nr, nq, loss, ts = runTPCModel(N, D, d, a, l, r, b, eta)
        tpcY.append(ts)
        nw, nr, nq, loss, ts = runTideModel(N, D, d, a, l, r, e)
        tideY.append(ts)
    fig = plt.figure()
    axes = fig.add_subplot(111)
    ltpc = axes.plot(X, tpcY, '-ob')
    ltide = axes.plot(X, tideY, '-+r')
    axes.set_xlabel('Average Backoff Time')
    axes.set_ylabel('Average Txn Service Time')
    fig.legend((ltpc, ltide), ('tpc', 'tide'))
    fig.savefig('%s/impact_of_backoff_time.pdf'%outdir)

def impactOfEta(outdir):
    N = 8
    D = 4096
    a = 10
    l = 0
    r = 10
    e = 0
    X = [4, 8, 16, 32, 64, 128]
    tpcY = []
    tideY = []
    tpc5Y = []
    for d in X:
        nw, nr, nq, loss, ts = runTPCModel(N, D, d, a, l, r, 100, 0)
        tpcY.append(ts)
        nw, nr, nq, loss, ts = runTideModel(N, D, d, a, l, r, e)
        tideY.append(ts)
        nw, nr, nq, loss, ts = runTPCModel(N, D, d, a, l, r, 1, 0.5)
        tpc5Y.append(ts)
    fig = plt.figure()
    axes = fig.add_subplot(111)
    ltpc = axes.plot(X, tpcY, '-ob')
    ltide = axes.plot(X, tideY, '-+r')
    ltpc5 = axes.plot(X, tpc5Y, '-*g')
    axes.set_xlabel('Number of Data Item')
    axes.set_ylabel('Average Txn Service Time')
    fig.legend((ltpc, ltide, ltpc5), ('tpc', 'tide', 'tpc5'))
    fig.savefig('%s/impact_of_eta.pdf'%outdir)

def main():
    if len(sys.argv) != 2:
        print 'runmodel <outdir>'
        sys.exit(-1)
    outdir = sys.argv[1]
    #impactOfLambda(outdir)
    #impactOfNumItems(outdir)
    #impactOfExecTime(outdir)
    #impactOfMaxNumTxns(outdir)
    #impactOfBackoffTime(outdir)
    impactOfEta(outdir)

if __name__ == '__main__':
    main()
