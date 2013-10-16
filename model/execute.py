import scipy.misc

def calcNDetmnExec(n, m, k, s, c):
    n, m, k, s, c = map(float, (n, m, k, s, c))
    g = c / s
    pc = k / 2 * (k + 2 * g) / (k + g) * m / n
    A = (k / 3 + g) / (k + g)
    alpha = pc * k * A
    nl = (1 - alpha) * m * k / 2 + alpha * m * k / 3
    pc = nl / n

    h1 = 1.0 / 3 * k * s + g * s
    ph2 = alpha * m * k / 3 / nl
    d = nl / ((1 - alpha) * m * k / 2)
    h2 = 1.0 / 3 * k * s + pc * k * h1 + g * s
    ph3 = alpha * m / nl
    h3 = 1.0 / 2 * k * s + pc * k * h1 + g * s
    w = h1 + ph2 * h2 + ph3 * h3

    pd = pc / m * alpha
    res = (k * s + pc * k * w + g * s) * (1 + 2 * pd)
    beta = w / res

    return pc, pd, w, res, beta

def calcDetmnExec(n, m, k, s):
    n, m, k, s = map(float, (n, m, k, s))
    pt = 1 - ((n - (m - 1)*k) / n)**k
    p = 1 - ((n - k) / n)**k
    nr = (1 - (1 - p)**m) / p
    #h = (m - 1) / ((1 - (1 - p)**(m - 1)) / p)
    h = m / ((1 - (1 - p)**(m - 1)) / p)
    #h = (m - 1) * p + 1
    #h = 0
    #for i in range(0, int(m - 1)):
    #    h += i * scipy.misc.comb(m - 2, i) * p**i * (1 - p)**(m - 2 - i)
    #h = h + 1
    w = (p * 1 + (1 - p) * 0.5 + h - 1) * k * s
    res = k * s + pt * w
    beta = pt * w / res
    return pt, nr, h, w, res, beta


