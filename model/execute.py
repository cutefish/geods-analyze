def calcNDetmnWait(k, g, s, u):
    numerator = k * s * (3 * g**2 * s + 3 * g * k * s + 3 * g * k * u +
                         3 * g * s - 3 * g * u + k**2 * s + k**2 * u +
                         3 * k * s + 2 * s - u)
    denominator = 3 * k * ((k + 1 + 2*g) * s + (k - 1) * u)
    return numerator / denominator

def calcNDetmnExec(n, m, k, s, g, iterative=False):
    """
        n   --  total number of locks
        m   --  max number of txns
        k   --  number of locks each txn
        s   --  lock step overhead
        g   --  commit time / s
    """
    n, m, k, s, c = map(float, (n, m, k, s, g))
    la = k / 2 * (k + 2 * g) / (k + g)
    lb = k / 2
    L = n * (1 - (1 - 1.0 / n)**((m - 1) * la))
    ps = L / n
    u = ps * k * g * s / 2
    w1 = calcNDetmnWait(k, g, s, u)
    alpha = ps * k * w1 / ((k + g) * s + ps * k * w1)
    ws = w1 * (1 + 1.0 / 2 * alpha**2 / (1 - alpha))
    pt = 1 - (1 - ps)**k
    pd = pt / (m - 1)
    #res = ((k + g) * s + ps * k * ws) * (1 + 2 * pd)
    res = ((k + g) * s + ps * k * ws)
    beta = ps * k * ws / res
    wsp = ws
    betap = beta
    while iterative:
        L = n * (1 - (1 - 1.0 / n)**((m - 1) * ((1 - betap) * la + betap * lb)))
        ps = L / n
        u = ps * k * wsp
        w1 = calcNDetmnWait(k, g, s, u)
        #ws = w1 * (1 + 0.5 * betap * (1 + betap) / (1 - betap)**2)
        #ws = w1 * (1 + 0.5 * betap + 1.5 * betap**2 + 2.5 * betap**3)
        ws = w1 * (1 + betap / (1 - betap))
        pd = ps / (m - 1) * betap
        res = ((k + g) * s + ps * k * ws) * (1 + 2 * pd)
        beta = ps * k * ws / res
        if abs(beta - betap) < 1e-3:
            break
        wsp = ws
        betap = beta
    return ps, pd, ws, res, beta
    #print 'ps1', ps, n, m, k, s, c
    #A = (k / 3 + g) / (k + g)
    #alpha = ps * k * A
    #nl = (1 - alpha) * na + alpha * nb
    ##nl = (1 - alpha) * k / 2 + alpha * k / 3
    ##ps = nl * m / n
    ##print 'ps2', ps, n, m, k, s, c, alpha
    ##assert abs(alpha) < 1, (n, m, k, s, c, alpha)

    #h1 = 1.0 / 3 * k * s + g * s
    #ph2 = alpha * m * k / 3 / nl
    #d = nl / ((1 - alpha) * m * k / 2)
    #h2 = 1.0 / 3 * k * s + ps * k * h1 + g * s
    #ph3 = alpha * m / nl
    #h3 = 1.0 / 2 * k * s + ps * k * h1 + g * s
    #w = h1 + ph2 * h2 + ph3 * h3

    #pd = ps / m * alpha
    #res = (k * s + ps * k * w + g * s) * (1 + 2 * pd)
    #beta = w / res

    #return ps, pd, w, res, beta

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


