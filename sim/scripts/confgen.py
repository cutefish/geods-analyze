import errno
import os
import re
import sys

def readfile(fn):
    """Read conf, const, variables and subs."""
    fh = open(fn, 'r')
    consts = {}
    variables = {}
    subs = {}
    while True:
        line = fh.readline()
        if line == '':
            break
        if line.startswith('#'):
            continue
        line = line.strip()
        if line == '':
            continue
        key, val = line.split('=')
        key = key.strip()
        val = val.strip()
        if key == 'conf.out.dir':
            outdir = val
        elif key.startswith('const.'):
            key = key[6:]
            val = eval(val)
            consts[key] = val
        elif key.startswith('var.'):
            key = key[4:]
            val = eval(val)
            variables[key] = val
        elif key.startswith('sub.'):
            key = key[4:]
            subs[key] = val
        else:
            raise SyntaxError('Unrecognizable prefix: %s'%key)
    return outdir, consts, variables, subs

def genvars(variables):
    """Generate variables."""
    total = 1
    for v in variables.values():
        total *= len(v)
    count = 0
    keys = variables.keys()
    indices = [0] * len(keys)
    while count < total:
        currvars = {}
        for i, key in enumerate(keys):
            val = variables[key][indices[i]]
            currvars[key] = val
        yield currvars
        count += 1
        indices[0] += 1
        if indices[0] == len(variables[keys[0]]):
            indices[0] = 0
            for i in range(1, len(keys)):
                indices[i] += 1
                if indices[i] < len(variables[keys[i]]):
                    break
                indices[i] = 0

def subvars(configs, subs):
    subre = re.compile('@[a-zA-Z0-9.]+')
    for key, val in subs.iteritems():
        assert '@' in val
        while '@' in val:
            match = subre.search(val)
            if match is None:
                raise SyntaxError('%s is not expandable' %val)
            subname = match.group()
            subkey = subname.lstrip('@')
            if not subkey in configs:
                raise SyntaxError(
                    'Unknown expand name: %s. '
                    'Possibly because multi-reference is not allowed' %(subkey))
            val = re.sub(subname, str(configs[subkey]), val)
        configs[key] = val

def readconfig(fn):
    configs = []
    outdir, consts, variables, subs = readfile(fn)
    for config in genvars(variables):
        config.update(consts)
        subvars(config, subs)
        configs.append(config)
    return outdir, configs

def writelogcfg(outdir):
    logstrings = [
        '[loggers]\n',
        'keys=root\n',
        '\n',
        '[handlers]\n',
        'keys=consoleHandler, fileHandler\n',
        '\n',
        '[formatters]\n',
        'keys=default\n',
        '\n',
        '[logger_root]\n',
        'level=DEBUG\n',
        'handlers=consoleHandler, fileHandler\n',
        '\n',
        '[handler_consoleHandler]\n',
        'class=StreamHandler\n',
        'level=INFO\n',
        'formatter=default\n',
        'args=(sys.stdout,)\n',
        '\n',
        '[handler_fileHandler]\n',
        'class=FileHandler\n',
        'level=DEBUG\n',
        'formatter=default\n',
        'args=("%s/log", "w")\n'%outdir,
        '\n',
        '[formatter_default]\n',
        'format=%(name)s.%(levelname)s: %(message)s\n',
    ]
    outfile = '%s/__logcfg__'%outdir
    outfh = open(outfile, 'w')
    outfh.writelines(logstrings)
    outfh.close()

def writesimcfg(outdir, config):
    outfn = '%s/__config__'%outdir
    outfh = open(outfn, 'w')
    for key, val in config.iteritems():
        outfh.write('%s = %s\n'%(key, val))
    outfh.close()

def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as exc:
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise

def writeconfigs(outdir, configs, startid):
    for i, config in enumerate(configs):
        index = i + startid
        subdir = '%s/%s'%(outdir, index)
        mkdir_p(subdir)
        writelogcfg(subdir)
        writesimcfg(subdir, config)
    print 'last index: %s'%(startid + len(configs) - 1)

def test():
    genstrings = [
        'conf.out.dir = /tmp/perftide\n',
        'const.num.zones = 1\n',
        'const.num.storage.nodes.per.zone = 1\n',
        'const.network.sim.class = "network.FixedLatencyNetwork"\n',
        'const.fixed.latency.nw.within.zone = 5\n',
        'const.fixed.latency.nw.cross.zone = 5\n',
        'const.txn.gen.impl = "txngen.UniformTxnGen"\n',
        'const.total.num.txns = 10000\n',
        'const.txn.arrive.interval.dist = ("expo", 10, )\n',
        'const.simulation.duration = 600000\n',
        'var.dataset.groups = [{1 : 1024}, {1 : 4096}]\n',
        'var.max.num.txns.per.storage.node = [8, 12]\n',
        'var.nwrites = [8, 12]\n',
        'var.intvl = [10, 20]\n',
        'var.system.impl = ["impl.dynlock.DynamicLockingSystem"'
        ', "impl.cendet.CentrializedDeterministicSystem"]\n',
        'sub.txn.classes = [{"freq":1, "nwrites":@nwrites, "intvl.dist":("expo", @intvl,)}]\n',
    ]
    fh = open('/tmp/config', 'w')
    fh.writelines(genstrings)
    fh.close()
    outdir, configs = readconfig('/tmp/config')
    writeconfigs(outdir, configs, 0)

def main():
    if len(sys.argv) < 3:
        print 'usage:'
        print 'confgen <config file> <start id>'
        yes = raw_input('test? y/Y')
        if yes == 'y' or yes == 'Y':
            test()
        sys.exit()
    cfgfile = sys.argv[1]
    startid = int(sys.argv[2])
    outdir, configs = readconfig(cfgfile)
    writeconfigs(outdir, configs, startid)

if __name__ == '__main__':
    main()
