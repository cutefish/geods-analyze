import sys


from model.ddist import DDist
from model.execute import calcNDetmnExec
from model.execute import calcDetmnExec
from model.protocol import quorum, getSLPLatencyDist


COUNT_MAX = 1000


class ExceedsCountMaxException(Exception):
    pass


class NotConvergeException(Exception):
    pass


def calcNDetmnSystem(n, k, s, c, rs, rc, l, C):
    """
        n   --  total number of locks
        k   --  number of locks each txn
        s   --  average step time
        c   --  average commit time
        rs  --  step time var
        rc  --  commit time var
        l   --  lambda, arrival rate
        C   --  average client-leader latency
    """
    #print n, k, s, c, rs, rc, l, C
    res0 = k * s + c
    m0 = int(l * res0)
    resp = res0
    resc = resp
    mp = m0
    mc = mp
    count = 0
    while True:
        count += 1
        #print resp, mp
        if count > COUNT_MAX:
            raise ExceedsCountMaxException(resp, mp, count)
        if resc > 100 * res0:
            raise NotConvergeException(resp, mp, count)
        ps, pd, ws, res, beta = calcNDetmnExec(n, mp, k, s, c, rs, rc)
        resc = res
        mc = int(l * resc)
        if abs(mc - mp) < 1:
            break
        resp = resc
        mp = mc
    return resc + C, mc, count, (ps, pd, ws, beta)


def calcDetmnSystem(n, k, s, l, p):
    """
        n   --  total number of locks
        k   --  number of locks each txn
        s   --  average step time
        l   --  lambda, arrival rate
        p   --  average protocol latency
    """
    res0 = k * s
    m0 = l * res0
    resp = res0
    resc = resp
    mp = m0
    mc = mp
    count = 0
    while True:
        count += 1
        if count > COUNT_MAX:
            raise ExceedsCountMaxException(resp, mp, count)
        if resc > 100 * res0:
            raise NotConvergeException(resp, mp, count)
        pt, a, h, wt, res, beta = calcDetmnExec(n, mp, k, s)
        resc = res
        mc = int(l * resc)
        if abs(mc - mp) < 1:
            break
        resp = resc
        mp = mc
    return resc + p, mc, count, (pt, a, h, wt, beta)


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
