import sys

from model.execute import calcNDetmnExec
from model.execute import calcDetmnExec

COUNT_MAX = 1000
THRESHOLD = 0.1

class ExceedsCountMaxException(Exception):
    pass

class NotConvergeException(Exception):
    pass

def calcNDetmnSystem(n, k, s, l, q, c):
    """
        n   --  total number of locks
        k   --  number of locks each txn
        s   --  lock step overhead
        l   --  lambda, arrival rate
        q   --  quorum round trip mean
        c   --  client-leader overhead mean
    """
    g = q / s
    res0 = (k + g) * s
    m0 = l * res0
    resp = res0
    resc = 0
    m = m0
    count = 0
    while True:
        count += 1
        if count > COUNT_MAX:
            raise ExceedsCountMaxException(resp, m, count)
        if resc > 100 * res0:
            raise NotConvergeException(resp, m, count)
        ps, pd, ws, res, beta = calcNDetmnExec(n, m, k, s, g)
        resc = res
        m = l * resc
        if abs(resp - resc) < THRESHOLD:
            break
        resp = resc
    return resc + c, m, count

def calcDetmnSystem(n, k, s, l, p):
    """
        n   --  total number of locks
        k   --  number of locks each txn
        s   --  lock step overhead
        l   --  lambda, arrival rate
        p   --  protocol latency mean
    """
    res0 = k * s
    m0 = l * res0
    resp = res0
    resc = 0
    m = m0
    count = 0
    while True:
        count += 1
        if count > COUNT_MAX:
            raise ExceedsCountMaxException(resp, m, count)
        if resc > 100 * res0:
            raise NotConvergeException(resp, m, count)
        pt, a, h, wt, res, beta = calcDetmnExec(n, m, k, s)
        resc = res
        m = l * resc
        if abs(resp - resc) < THRESHOLD:
            break
        resp = resc
    return resc + p, m, count

def main():
    if len(sys.argv) != 3:
        print 'system <key> <args>'
        print
        sys.exit(-1)
    key = sys.argv[1]
    args = sys.argv[2]
    if key == 'nd':
        try:
            n, k, s, l, q, c = args.split(' ')
            n, k, s, l, q, c = map(float, (n, k, s, l, q, c))
        except:
            print 'Args <n k s l q c>. \n\tGot: %s.'%args
            print calcNDetmnSystem.__doc__
            print
            sys.exit(-1)
        try:
            res, m, count = calcNDetmnSystem(n, k, s, l, q, c)
            print 'res=%s, m=%s, count=%s'%(res, m, count)
        except ExceedsCountMaxException as e:
            res, m, count = e.args
            print 'Exceeds COUNT_MAX, res=%s, m=%s, count=%s'%(res, m, count)
        except NotConvergeException as e:
            res, m, count = e.args
            print 'Not converge, res=%s, m=%s, count=%s'%(res, m, count)
    elif key == 'de':
        try:
            n, k, s, l, p = args.split(' ')
            n, k, s, l, p = map(float, (n, k, s, l, p))
        except:
            print 'Args <n k s l p>. \n\tGot: %s.'%args
            print calcDetmnSystem.__doc__
            print
            sys.exit(-1)
        try:
            res, m, count = calcDetmnSystem(n, k, s, l, p)
            print 'res=%s, m=%s, count=%s'%(res, m, count)
        except ExceedsCountMaxException as e:
            res, m, count = e.args
            print 'Exceeds COUNT_MAX, res=%s, m=%s, count=%s'%(res, m, count)
        except NotConvergeException as e:
            res, m, count = e.args
            print 'Not converge, res=%s, m=%s, count=%s'%(res, m, count)
    else:
        print 'Unknown key: %s'%key

if __name__ == '__main__':
    main()
