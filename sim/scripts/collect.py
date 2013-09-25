import os
import re
import sys

def readconfig(indir):
    fn = '%s/__config__'%indir
    fh = open(fn, 'r')
    config = {}
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
        key = key.strip()
        val = val.strip()
        if re.match('[/a-zA-Z.]+', val):
            config[key] = val
        else:
            config[key] = eval(val)
    return config

def readresult(indir):
    fn = '%s/stdout'%indir
    fh = open(fn, 'r')
    result = {}
    skipUntil(fh, '#####  PROFILING RESULTS  #####')
    while True:
        line = fh.readline()
        if line == '':
            break
        line = line.strip()
        if line == '':
            continue
        if '#####  END  #####' in line:
            break
        assert re.match('[_a-zA-Z]+\.INFO:', line), \
                'line not starts with ...INFO: %s'%line
        if re.search('[([{]', line):
            while True:
                if (line.count('(') == line.count(')') and \
                    line.count('[') == line.count(']') and \
                    line.count('{') == line.count('}')):
                    break
                #this line is not complete
                next = fh.readline()
                if next == '' or re.match('[a-zA-Z]+\.INFO:', next):
                    raise ValueError(
                        'line end but incomplete: %s, %s'%(line, next))
                next = next.strip()
                line += next
        logmsg, res = line.split('INFO:')
        if res == '':
            continue
        key, val = res.split('=')
        key = key.strip()
        val = val.strip()
        result[key] = eval(val)
    return result

def skipUntil(fh, pattern):
    while True:
        line = fh.readline()
        if line == '':
            break
        if re.search(pattern, line):
            break

def parseRange(r):
    lb, ub = r.split('-')
    return int(lb), int(ub)

def collect(indir, r):
    lb, ub = parseRange(r)
    for cfgdir in os.listdir(indir):
        if not os.path.isdir('%s/%s'%(indir, cfgdir)):
            continue
        try:
            index = int(cfgdir)
        except:
            continue
        if lb > index or index > ub:
            continue
        rundir = '%s/%s'%(indir, cfgdir)
        config = readconfig(rundir)
        result = readresult(rundir)
        config['out.dir'] = cfgdir
        print 'exp=(%s,%s)'%(config, result)

def main():
    if len(sys.argv) != 3:
        print 'collect <dir> <range>'
        sys.exit(-1)
    collect(sys.argv[1], sys.argv[2])

if __name__ == '__main__':
    main()
