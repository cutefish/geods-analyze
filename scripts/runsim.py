import os
import subprocess
import sys
import time
import datetime

def run(indir, r, numProcs=1):
    commands = []
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
        command = 'python%s sim.py %s --verify' %(
            os.environ['PYTHON_SIM_VERSION'], rundir)
        commands.append((command, '%s/stdout'%rundir, '%s/stderr'%rundir))
    runCommands(commands, numProcs)
    #write a success mark
    fh = open('%s/__success__'%indir, 'w')
    fh.write(str(datetime.datetime.now()))
    fh.write('\n')
    fh.close()

def parseRange(r):
    lb, ub = r.split('-')
    return int(lb), int(ub)

def runCommands(commands, numProcs):
    procs = []
    while True:
        #first remove processes that are finished
        for proc, fh in list(procs):
            if proc.poll() is not None:
                procs.remove((proc, fh))
                fh.close()
        #check terminate condition
        if len(procs) == 0 and len(commands) == 0:
            break
        #try to launch new commands if we can
        if len(procs) < numProcs and len(commands) != 0:
            for i in range(len(procs), numProcs):
                command, outfile, errfile = commands.pop()
                print command, '1>', outfile, '2>', errfile
                outfh = open(outfile, 'w')
                errfh = open(errfile, 'w')
                proc = subprocess.Popen(
                    command.split(' '), stdout=outfh, stderr=errfh)
                procs.append((proc, outfh))
                if len(commands) == 0:
                    break
        time.sleep(10)

def main():
    if len(sys.argv) < 3:
        print 'run.py <in dir> <range> [num procs]'
        sys.exit(-1)
    if len(sys.argv) == 3:
        run(sys.argv[1], sys.argv[2])
    else:
        run(sys.argv[1], sys.argv[2], int(sys.argv[3]))

if __name__ == '__main__':
    main()
