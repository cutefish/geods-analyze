import sys


def calcNDetmnWait(k, c, s, u, rs, rc):
    norm = k * (k + 1) / 2 * s + k * (k - 1) / 2 * u + k * c
    tmp = -s * (s + u) * 1 / 6 * k * (k + 1) * (2 * k + 1) + \
            (k * s + c + rs) * (s + u) * 1 / 2 * k * (k + 1) + \
            s * u * 1/ 2 * k * (k + 1) - \
            k * (k * s + c + rs) * u + \
            rc * k * c
    return tmp / norm

def calcNDetmnExec(n, m, k, s, c, rs, rc):
    """
        n   --  total number of locks
        m   --  max number of txns
        k   --  number of locks each txn
        s   --  lock step overhead
        c   --  commit time
    """
    n, m, k, s, c = map(float, (n, m, k, s, c))
    la = (k * (k + 1) * s + 2 * k * c) / (2 * (k * s + c))
    lb = k / 2
    mr = m - 1 if m > 1 else 0
    L = n * (1 - (1 - 1.0 / n)**(mr * la))
    #L = (m - 1) * la
    ps = L / n
    u = ps * rc
    u = 0
    #u = ps * k * rc
    w1 = calcNDetmnWait(k, c, s, u, rs, rc)
    alpha = ps * k * w1 / (k * s + c + ps * k * w1)
    #ws = w1
    ws = w1 * (0.5 - alpha / (1 - alpha) + 0.5 / (1 - alpha) + alpha / (1 - alpha)**2)
    pt = 1 - (1 - ps)**k
    pd = ps * alpha / (m - 1)
    res = (k * s + c + ps * k * ws)
    beta = ps * k * ws / res
    return ps, pd, ws, res, beta


def calcDetmnExec(n, m, k, s):
    """
        n   --  total number of locks
        m   --  max number of txns
        k   --  number of locks each txn
        s   --  lock step overhead
    """
    n, m, k, s = map(float, (n, m, k, s))
    l = n * (1 - (1 - 1.0 / n)**((m - 1)* k))
    pt = 1 - ((n - l) / n)**k
    p = 1 - ((n - k) / n)**k
    a = (1 - (1 - p)**(m - 1)) / p
    h = (m - 1) / a
    wt = (p * 1 + (1 - p) * 0.5 + h - 1) * k * s
    res = k * s + pt * wt
    beta = pt * wt / res
    return pt, a, h, wt, res, beta


def main():
    if len(sys.argv) != 3:
        print 'execute <key> "<(args)>"'
        print
        sys.exit()
    key = sys.argv[1]
    args = eval(sys.argv[2])
    if key == 'nd':
        print calcNDetmnExec(*args)
    elif key == 'de':
        print calcDetmnExec(*args)
    else:
        print 'key error: %s' % (key)


if __name__ == '__main__':
    main()
