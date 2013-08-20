"""
Check config and construct instances.
"""
import re

class Configuration(object):
    def __init__(self):
        self.conf = {}

    def __getitem__(self, key):
        return self.conf[key]

    def get(self, key, default=None):
        return self.conf.get(key, default)

    def clone(self):
        new = Configuration()
        for key, val in self.conf.iteritems():
            new[key] = val
        return new

    def clear(self):
        self.conf.clear()

    def readNext(self, fh, until):
        """Read next configuration.

        If starts with comment mark #, continue.
        If ends with parathesis, continue line.
        """
        while True:
            line = fh.readline()
            if line == '':
                return None
            line = line.strip()
            if line == until:
                return until
            line = line.rsplit('#')[0].strip()
            if line != '':
                break
        linecont = {'(':')', '[':']', '{':'}'}
        cont = line[-1]
        if not cont in linecont:
            return line
        first = line
        while True:
            newline = fh.readline()
            if newline == '':
                raise SyntaxError('%s missing for %s'%(linecont[cont], first))
            newline = newline.strip().rsplit('#')[0].strip()
            line += newline
            if newline == '':
                continue
            if newline[0] == linecont[cont]:
                return line
        return line

    def read(self, f, until='#end config'):
        fh = f
        if not isinstance(f, file):
            fh = open(f, 'r')
        while True:
            next = self.readNext(fh, until)
            if next == None:
                break
            if next == until:
                break
            try:
                key, val = next.split('=')
                key = key.strip()
                val = val.strip()
            except ValueError:
                raise SyntaxError('config must in format key = value: %s' %next)
            try:
                self.conf[key] = eval(val)
            except Exception as e:
                if re.match('[a-zA-Z.]+', val):
                    #if val is a name
                    self.conf[key] = val
                else:
                    raise e
        if not isinstance(f, file):
            fh.close()

    def write(self, fn):
        fh = open(fn, 'w')
        for key, val in self.conf.iteritems():
            fh.write('%s = %s\n' %(key, val))

    def __getitem__(self, key):
        return self.conf[key]

    def __setitem__(self, key, value):
        self.conf[key] = value

    def __str__(self):
        return self.conf.__str__()

#####  TEST  #####

if __name__ == '__main__':
    config = Configuration()
    config.read('__config__')
    print config
    config.write('/tmp/simtestconfig')
    config.clear()
    config.read('__config__')
    print config
