def calcNDetmnExec(n, m, k, n, s, c):
    n, m, k, n, s, c = map(float, (n, m, k, n, s, c))
    g = c / s
    beta = _calcNDetmnInitVal(n, m, k, n, s, c)
    dbeta = beta
    #iterative execution
    while dbeta > 1e-2:
        l = (1 - beta) * float(k) / 2 * (k + 2 * g) / (k + g) + \
                beta * float(k) / 3
        L = float(m - 1) * l
        pc = L / n
        w1 = k * s / 3 + c
        nc = k * pc
        A = w1M / (s * k + c + pc * w1)
        alpha = nc * A
        assert alpha < 1
        w = w1 + 0.5 * alpha * w1 + 1.5 * alpha**2 * w1
        pw = 1 - (1 - pc)**k
        pd = pc / (m - 1) * alpha
        B = wM / ((s * k + g * s + nc * wM) * (1 + 2*pd))
        beta = nc * B
        dl = abs(ln - l)
        l = ln

def _calcNDetmnInitVal(n, m, k, n, s, c):
    l = float(k) / 2 * (k + 2 * g) / (k + g)
    pc = L / n
    w1 = k * s / 3 + c
    nc = k * pc
    A = w1 / (s * k + c + pc * w1)
    alpha = nc * A
    w = w1 + 0.5 * alpha * w1 + 1.5 * alpha**2 * w1
    pw = 1 - (1 - pc)**k
    pd = pc / (m - 1) * alpha
    B = w / ((s * k + c + pc * w1) * (1 + 2 * pd))
    beta = nc * B
    return beta


