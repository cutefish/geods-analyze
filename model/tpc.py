import sys

from scipy.misc import comb

from common import Markov2D

class TPCModel(Markov2D):
    """TPC model.

    Each state is represented by (w, r) where w is the number of txns waiting
    for retry and r is the number of running txns.

    Transitions:
        (w, r) -> (w, r + 1)        --  arrive,  p = a * dt
        (w, r) -> (w - 1, r + 1)    --  backoff, p = w * b * dt
        (w, r) -> (w, r - 1)        --  commit,  p = alpha**(r - 1) * r * c * dt
        (w, r) -> (w + 1, r - 1)    --  abort,   p = (1 - alpha**(r - 1)) * r * c * dt

    """
    def __init__(self, N, D, d, a, l, r, b, eta):
        """
        @args:
            N   --  total allowed number of txns.
            D   --  total number of items.
            d   --  number of items each txn.
            a   --  arrival interval.
            l   --  latency.
            r   --  execution time.
            b   --  backoff interval after abort.
            eta --  percentage of conflict txns to be able to commit.

        """
        Markov2D.__init__(self, N + 1, N + 1)
        self.N = N
        self.alpha = comb(D - d, d) / comb(D, d)
        self.arvRate = 1.0 / a
        self.bkfRate = 1.0 / b
        self.cmtRate = 1.0 / (l + r)
        self.eta = eta
        self.epsilon = 1.0 / self.size / 1000.0

    def fillOffDiagonal(self, Q):
        #arrive 
        for w in range(0, self.N):
            for r in range(0, self.N - w):
                Q[(self.state(w, r), self.state(w, r + 1))] = self.arvRate
        #backoff and retry
        for w in range(1, self.N + 1):
            for r in range(0, self.N - w + 1):
                Q[(self.state(w, r), self.state(w - 1, r + 1))] = \
                        w * self.bkfRate
        #commit
        for w in range(0, self.N):
            for r in range(1, self.N - w + 1):
                ncfr = self.alpha**(r - 1)
                val = (ncfr + self.eta * (1 - ncfr)) * r * self.cmtRate
                if val > self.epsilon:
                    Q[(self.state(w, r), self.state(w, r - 1))] = val
        #abort
        for w in range(0, self.N):
            for r in range(1, self.N - w + 1):
                ncfr = self.alpha**(r - 1)
                val = (1 - self.eta) * (1 - ncfr) * r * self.cmtRate
                if val > self.epsilon:
                    Q[(self.state(w, r), self.state(w + 1, r - 1))] = val

    def run(self):
        """
        Calculate the following parameters:
            nwait   --  number of the txn waiting to run
            nrun    --  number of the txns actually running
            nqueue  --  total number of txns in the system
            tserve  --  time between txn submission and leave the system
        """
        pi = self.computePi()
        print ('N=%s, alpha=%s, arvRate=%s, bkfRate=%s, cmtRate=%s, eta=%s'
               %(self.N, self.alpha,
                 self.arvRate, self.bkfRate, self.cmtRate, self.eta))
        print 'sum(pi)=%s' %sum(pi)
        print self.pi2str(pi, 10)
        nwait, nrun, nqueue, loss, tserve = self.getPiStats(pi, self.arvRate)
        print ('nwait=%s, nrun=%s, nqueue=%s, loss=%s, tserve=%s'
               %(nwait, nrun, nqueue, loss, tserve))
        return nwait, nrun, nqueue, loss, tserve

def runModel(N, D, d, a, l, r, b, eta):
    model = TPCModel(N, D, d, a, l, r, b, eta)
    return model.run()

def main():
    if not len(sys.argv) == 9:
        print 'tpc <N> <D> <d> <a> <l> <r> <b> <eta>'
        sys.exit(-1)
    N = int(sys.argv[1])
    D = int(sys.argv[2])
    d = int(sys.argv[3])
    a = float(sys.argv[4])
    l = float(sys.argv[5])
    r = float(sys.argv[6])
    b = float(sys.argv[7])
    eta = float(sys.argv[8])
    runModel(N, D, d, a, l, r, b, eta)

if __name__ == '__main__':
    main()
