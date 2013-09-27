import sys

def readparams(rfile):
    fh = open(rfile, 'r')
    params = []
    while True:
        line = fh.readline()
        if line == '':
            break
        line = line.strip()
        if line == '':
            continue
        if line.startswith('#'):
            continue
        key, val = line.split('=')
        val = val.strip()
        params.append(eval(val))
    fh.close()
    return params

def disp(params, dispConfs, dispKeys):
    print dispConfs, dispKeys
    for param in params:
        config, result = param
        shouldDisp = True
        for key, val in dispConfs.iteritems():
            if not config[key] == val:
                shouldDisp = False
                break
        if shouldDisp:
            print 'config>> ', ' '.join(
                ['%s=%s'%(k, v) for k, v in dispConfs.iteritems()])
            config.update(result)
            print 'result>> ', ' '.join(
                ['%s=%s'%(k, config[k]) for k in dispKeys])
            print

def main():
    if len(sys.argv) != 4:
        print 'disp <result file> <disp configs> <disp keys>'
        sys.exit()
    params = readparams(sys.argv[1])
    disp(params, eval(sys.argv[2]), eval(sys.argv[3]))

if __name__ == '__main__':
    main()

