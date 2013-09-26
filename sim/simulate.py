import os
import logging.config
import sys
import time

from SimPy.Simulation import initialize, activate, simulate, now

import sim
from sim.configure import Configuration
from sim.parse import CustomArgsParser
from sim.perf import Profiler
from sim.importutils import loadClass
from sim.rti import RTI
from sim.verify import Verifier

def main():
    start = time.time()
    parser = CustomArgsParser(optFlags=['--verify'])
    parser.parse(sys.argv[1:])
    if len(parser.getPosArgs()) < 1:
        print 'python sim.py <config dir> --verify'
        sys.exit(-1)
    path = parser.getPosArg(0)
    configFile = '%s/__config__' %path
    configs = Configuration()
    configs.read(configFile)

    if parser.getOption('--verify'):
        configs['system.should.verify'] = True

    #simulation init
    #logging.basicConfig(level=logging.DEBUG)
    logging.config.fileConfig('%s/__logcfg__' %path)
    logger = logging.getLogger(__name__)
    #simpy initialize
    initialize()
    #system initialize
    RTI.initialize(configs)
    txnGenCls = loadClass(configs['txn.gen.impl'])
    txnGen = txnGenCls(configs)
    systemCls = loadClass(configs['system.impl'])
    system = systemCls(configs)
    for txn, at in txnGen.generate():
        system.schedule(txn, at)
    system.start()

    #simulate
    logger.info('\n#####  START SIMULATION  #####\n')
    simulate(until=configs.get('simulation.duration', 600000))
    logger.info('simulated time: %s' %now())
    logger.info('\n#####  END  #####\n')

    ##verify
    if parser.getOption('--verify'):
        logger.info('\n#####  START VERIFICATION  #####\n')
        v = Verifier()
        v.check(system)
        logger.info('VERIFICATION SUCCEEDS\n')
        logger.info('\n#####  END  #####\n')

    #get profile
    logger.info('\n#####  PROFILING RESULTS  #####\n')
    system.profile()
    #system.printMonitor()
    logger.info('\n#####  END  #####\n')

    end = time.time()
    logger.info('\n#####  SIMULATION TIME: %s seconds  #####\n' %(end - start))


if __name__ == '__main__':
    main()
