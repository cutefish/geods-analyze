from numpy import ones, zeros, empty, average
import scipy.sparse as sp
from matplotlib.pylab import matshow, savefig
from scipy.linalg import norm
from scipy.misc import comb
import time

def getAlpha(D, d):
    return comb(D - d, d) / comb(D, d)

class Markov2D(object):
    """
    Methods for set up and compute markov chain.

    Copied from http://wiki.scipy.org/Cookbook/Solving_Large_Markov_Chains.
    """
    def __init__(self, N1, N2):
        self.N1 = N1
        self.N2 = N2
        self.size = N1 * N2

    def state(self, i, j):
        return j * self.N1 + i

    def fillOffDiagonal(self, Q):
        raise NotImplementedError(
            'Q matrix fill off diagnoal method not implemented')

    def computePi(self, printQ=False):
        e0 = time.time()
        Q = sp.dok_matrix((self.size, self.size))
        self.fillOffDiagonal(Q)
        if printQ is True:
            print 'Q='
            self.printQ(Q)
        #self.printQ(Q)
        print 'finish filling Q matrix'
        # Set the diagonal of Q such that the row sums are zero
        diag = -Q * ones(self.size)
        Q.setdiag( ones(self.size) )
        Q.setdiag( diag )
        # Compute a suitable stochastic matrix by means of uniformization
        l = min(Q.values())*1.001  # avoid periodicity, see trivedi's book
        P = sp.eye(self.size, self.size) - Q/l
        # compute Pi
        P =  P.tocsr()
        pi = zeros(self.size);  pi1 = zeros(self.size)
        pi[0] = 1;
        n = norm(pi - pi1,1); i = 0;
        tol = 1.0 / self.size / 100.0 if self.size > 10000 else 1e-6
        while n > tol and i < 1e6:
            pi1 = pi*P
            pi = pi1*P   # avoid copying pi1 to pi
            n = norm(pi - pi1,1); i += 1
        if i < 1e6:
            print ("compute Pi converges: time=%s, i=%s, n=%s" 
                   %(time.time() - e0, i, n))
        else:
            print ("compute Pi did not converge: time=%s, n=%s"
                   %(time.time() - e0, n))
        return pi

    def printQ(self, Q):
        for key, val in Q.iteritems():
            state1, state2 = key
            n11 = state1 % self.N1
            n12 = state1 / self.N1
            n21 = state2 % self.N1
            n22 = state2 / self.N1
            print ('%s: (%s, %s) -> (%s, %s) :%s'
                   %(key, n11, n12, n21, n22, val))

    def plotPi(self, pi, outfile):
        pi = pi.reshape(self.N2, self.N1)
        matshow(pi)
        savefig(outfile)

    def getPiStats(self, pi, lambd):
        """Return E(n1), E(n2), E(n1 + n2), W."""
        en1 = 0
        en2 = 0
        eL = 0
        for i in range(0, self.N1):
            for j in range(0, self.N2):
                en1 += i * pi[self.state(i, j)]
                en2 += j * pi[self.state(i, j)]
                eL += (i + j) * pi[self.state(i, j)]
        #the effective lambd is the one when it sees the system is not full
        p = 0
        for i in range(0, self.N1):
            p += pi[self.state(i, self.N1 - i - 1)]
        effLambda = lambd * (1 - p)
        W = eL / effLambda
        return en1, en2, eL, p, W

    def pi2str(self, pi, num=-1):
        ave = average(pi)
        disps = []
        for i in range(0, self.N1):
            for j in range(0, self.N2):
                curr = pi[self.state(i, j)]
                if curr < ave and num < len(pi):
                    continue
                disps.append((i, j, curr))
        def sortpi(entry):
            i, j, curr = entry
            return curr
        sortedDisps = sorted(disps, key=sortpi, reverse=True)
        strings = []
        count = 0
        for entry in sortedDisps:
            i, j, curr = entry
            strings.append('P{i=%s, j=%s}=%.2e' %(i, j, curr))
            count += 1
            if num != -1 and count > num:
                break
        return ', '.join(strings)

class BirthDeathProcess(object):
    def __init__(self):
        raise NotImplementedError('Abstract class')

    def lambdaFunc(self, count):
        raise NotImplementedError('Abstract class lambda function')

    def muFunc(self, count):
        raise NotImplementedError('Abstract class mu function')

    def stop(self, count, value):
        raise NotImplementedError('Abstract class stop condition')

    def calculate(self):
        """
        Implement the birth-death process formula:
            S = 1 + la_0 / mu_1 + la_0 * la_1 / (mu_1 * mu_2) + ...
            P_j = lim_{t->inf} P_j(t)
            P_j = S^ -1 if j = 0
                = l_0 * l_1 * ... * l_{j-1} / (m_1 * m_2 * ... m_j) P_0
        """
        count = 0
        coeffs = []
        lambd = self.lambdaFunc(count)
        mu = self.muFunc(count)
        coeffs.append(lambd / mu)
        while True:
            count += 1
            lambd = self.lambdaFunc(count)
            mu = self.muFunc(count)
            prev = coeffs[count - 1]
            curr = lambd / mu * prev
            coeffs.append(curr)
            if self.stop(count, curr):
                break
        return coeffs
