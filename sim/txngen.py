import random

import numpy as np

from data import ItemID
from rand import RandInterval
from txns import Action, Transaction

class UniformTxnGen(object):
    class TxnClass(object):
        def __init__(self, config, groups):
            self.freq = config['freq']
            self.nreads = config.get('nreads', 0)
            self.nwrites = config.get('nwrites', 0)
            if self.nreads + self.nwrites == 0:
                raise ValueError(
                    'number of reads and writes is less than zero: %s'%config)
            self.gids = set(config.get('groups', groups.keys()))
            self.gsize = 0
            for gid in self.gids:
                self.gsize += groups[gid]
            self.config = config

    def __init__(self, configs):
        self.groups = configs['dataset.groups']                 #{gid : size}
        self.numZones = configs['num.zones']
        self.numTxns = configs['total.num.txns']                #int
        self.arrIntvDist = configs['txn.arrive.interval.dist']  #(dist, mean, [kargs])
        self.txnClasses = self.getTxnClasses(configs['txn.classes'])

    def getTxnClasses(self, classes):
        txnClasses = []
        for cfg in classes:
            txnClass = UniformTxnGen.TxnClass(cfg, self.groups)
            txnClasses.append(txnClass)
        #normalize frequence
        s = 0
        for tcls in txnClasses:
            s += tcls.freq
        for tcls in txnClasses:
            tcls.freq = float(tcls.freq) / s
        return txnClasses

    def generate(self):
        count = 1
        at = {}
        while count <= self.numTxns:
            for zoneID in range(self.numZones):
                txnID = count
                #choose a class
                txnCls = self.nextClass()
                #construct actions
                actions = self.nextActions(txnCls)
                txn = Transaction(txnID, zoneID, actions, txnCls.config)
                prev = at.get(zoneID, 0)
                intvl = RandInterval.get(*self.arrIntvDist)
                curr = prev + intvl
                at[zoneID] = curr
                yield txn, curr
                count += 1
                if count > self.numTxns:
                    break

    def nextClass(self):
        r = random.random()
        for txnCls in self.txnClasses:
            r -= txnCls.freq
            if r < 0:
                return txnCls

    def nextActions(self, txnCls):
        itemIDs = set([])
        actions = []
        num = txnCls.nreads + txnCls.nwrites
        for i in range(num):
            while True:
                r = random.randint(1, txnCls.gsize)
                gid = 0
                for g in txnCls.gids:
                    r -= self.groups[g]
                    gid = g
                    if r <= 0:
                        break
                iid = random.randint(0, self.groups[gid] - 1)
                itemID = ItemID(gid, iid)
                if itemID not in itemIDs:
                    itemIDs.add(itemID)
                    break
        for i in range(txnCls.nreads):
            itemID = itemIDs.pop()
            actions.append(Action(Action.READ, itemID))
        for i in range(txnCls.nwrites):
            itemID = itemIDs.pop()
            actions.append(Action(Action.WRITE, itemID, random.random()))
        return actions

#####  TEST  #####
def test():
    configs = {
        'dataset.groups' : { 1 : 128, 2 : 128, 3 : 128 },
        'num.zones' : 2,
        'total.num.txns' : 1000,
        'txn.arrive.interval.dist' : ('expo', 100, ),
        'txn.classes' : [
            {'freq' : 2, 'nwrites' : 3,
             'groups' : [1,2], 'intvl.dist' : ('expo', 10)},
            {'freq' : 4, 'nreads' : 3, 'nwrites' : 1,
             'groups' : [2,3], 'intvl.dist' : ('expo', 10)},
        ]
    }
    gen = UniformTxnGen(configs)
    txns = []
    for txn, at in gen.generate():
        txns.append((txn, at))
    zones = {}
    for i, sched in enumerate(txns):
        txn, at = sched
        #print first 10 txns
        if i < 10:
            print '%r at %s'%(txn, at)
        if txn.zoneID not in zones:
            zones[txn.zoneID] = []
        zones[txn.zoneID].append((txn, at))
    #print num txns per zone
    print ', '.join(
        ['%s:%s' %(key, len(val)) for key, val in zones.iteritems()])
    #stats for arrive interval
    zoneArrStats = {}
    for zoneID, txns in zones.iteritems():
        intvls = []
        prev = 0
        for txn, at in txns:
            intvls.append(at - prev)
            prev = at
        histo = np.histogram(intvls)
        mean = np.mean(intvls)
        std = np.std(intvls)
        zoneArrStats[zoneID] = (mean, std, histo)
    for zoneID, stats in zoneArrStats.iteritems():
        mean, std, histo = stats
        print 'zoneID=%s mean=%s std=%s histo=%s' %(zoneID, mean, std, histo)
    #txn class probabiltiy
    cls1Num = 0
    cls2Num = 0
    for txn, at in txns:
        nreads = 0
        nwrites = 0
        items = set([])
        for action in txn.actions:
            items.add(action.itemID)
            if action.label == Action.READ:
                nreads += 1
            else:
                nwrites += 1
        if nwrites == 3 and nreads == 0:
            cls1Num += 1
        elif nreads == 3 and nwrites == 1:
            cls2Num += 1
        else:
            raise ValueError('Txn %s has %s reads and %s writes'
                             %(txn.ID, nreads, nwrites))
        assert len(items) == nwrites + nreads, \
                ('txn=%s, nreads=%s, nwrites=%s, items=%s'
                 %(txn.ID, nreads, nwrites,
                   ', '.join([str(item) for item in items])))
    print 'cls1 : %s, cls2 : %s'%(cls1Num, cls2Num)
    #item distribution
    groups = {}
    for txn, at in txns:
        for action in txn.actions:
            itemID = action.itemID
            gid = itemID.gid
            if gid not in groups:
                groups[gid] = 0
            groups[gid] += 1
    print groups

def main():
    test()

if __name__ == '__main__':
    main()
