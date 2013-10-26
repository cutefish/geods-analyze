import math
import os
import re
import sys

try:
    import numpy
except:
    pass

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

class RangeStringParser(object):
    REGEX = '\[[0-9,: ]+\]'
    def __init__(self):
        pass

    def parse(self, string):
        """Example string: [1, 3:6, 9]."""
        ret = []
        if not re.match('^%s$'%RangeStringParser.REGEX, string):
            raise SyntaxError(
                'Range string must in the form %s: %s'
                %(string, RangeStringParser.REGEX))
        string = string.strip('[]')
        for n in string.split(','):
            if ':' not in n:
                ret.append(int(n))
            else:
                ranges = n.split(':')
                if len(ranges) == 2:
                    ret += range(int(ranges[0]), int(ranges[1]))
                else:
                    ret += range(int(ranges[0]), int(ranges[2]), int(ranges[1]))
        return ret

def collect(indir, r):
    dirs = RangeStringParser().parse(r)
    for d in dirs:
        rundir = '%s/%s'%(indir, d)
        if not os.path.isdir(rundir):
            print 'file: %s in %s'%(d, indir)
            continue
        try:
            config = readconfig(rundir)
            result = readresult(rundir)
            config['out.dir'] = str(d)
            print 'exp=(%s,%s)'%(config, result)
        except:
            print 'Error in %s'%rundir
            raise

def main():
    if len(sys.argv) != 3:
        print 'collect <dir> <range>'
        sys.exit(-1)
    collect(sys.argv[1], sys.argv[2])

if __name__ == '__main__':
    main()
