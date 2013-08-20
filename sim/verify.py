import logging

from core import infinite
from data import Dataset

class VerificationError(Exception):
    pass

class Verifier(object):
    """Verify the system execution result.

    We check the following things:
        * The final item values of each zone should be the same
        * If there are multiple txn running, they should be the same

    """
    def __init__(self):
        pass

    def check(self, system):
        self.checkZoneValues(system)

    def checkZoneValues(self, system):
        dataset = system.dataset
        for snodes in system.snodes.values():
            for snode in snodes:
                for group in snode.groups.values():
                    for replica in group.iteritems():
                        item = dataset[replica.itemID]
                        item.verifyConsistent(replica)
                        
#    def checkTxnValues(self, system):
#        for txns in system.txns.values():
#            if len(txns) == 1:
#                continue
#            first = txns[0]
#            for other in txns[1:]:
#                for itemID in first.readset:
#                    value1, version1 = first.reads[itemID]
#                    value2, version2 = other.reads[itemID]
#                    if not (value1 == value2 and version1 == version2):
#                        raise VerificationError(
#                            'Items not equal: '
#                            'txn=%s, itemID=%s, va/ver=(%s, %s)'
#                            %(first.ID, itemID,
#                              (value1, version1), (value2, version2)))

#class TxnContext(object):
#    """The context of a transaction for verification."""
#    def __init__(self, txn):
#        self.txn = txn
#        self.startTs = -1       #the latest ts when txn starts
#        self.ts = -1            #the assigned ts for this txn
#        self.readset = {}       #{itemID : version}
#
#
#class Verifier(object):
#    """Verify transaction operations.
#
#    We do two kinds of verifications:
#        1. Whether a commit is valid
#        2. Whether a conflict is reasonable
#
#    There are several assumptions about the system:
#        1. When read an item, the latest version will be returned.
#        2. Write is buffered until after commit.
#
#    To check whether a commit is valid, the consistency model will try to to
#    assign the committing a timestamp. If a valid assignment can be found by
#    the model, then the commit is valid. 
#
#    We check whether a conflict is reasonable by comparing the lock set and
#    checking the state of the transaction.
#
#    """
#    ClsInstance = None
#
#    def __init__(self):
#        self.dataset = Dataset.new('global')
#        self.runningTxns = {}                   #{txn : context}
#        self.committedTxns = {}                 #{txn : context}
#        self.latestTs = -1
#        self.model = Linearizability(self)
#        self.logger = logging.getLogger(self.__class__.__name__)
#
#    @classmethod
#    def get(cls):
#        if cls.ClsInstance is None:
#            cls.ClsInstance = Verifier()
#        return cls.ClsInstance
#
#    def doTxnStart(self, txn):
#        context = TxnContext(txn)
#        context.startTs = self.latestTs
#        self.runningTxns[txn] = context
#
#    def doTxnRead(self, txn, itemID):
#        context = self.runningTxns[txn]
#        item = self.dataset[itemID]
#        context.readset[itemID] = item.read()
#        self.logger.debug('read txn=%s itemID=%s version=%s'
#                          %(txn.ID, itemID, item.read()))
#
#    def doTxnCommit(self, txn):
#        """Commit a transaction.
#
#        Let the model try to assign a ts for this txn, if it fails, it will
#        raise an exception.
#
#        """
#        if txn in self.committedTxns:
#            return
#        ts = self.model.assignTs(txn)
#        context = self.runningTxns[txn]
#        context.assignTs = ts
#        del self.runningTxns[txn]
#        for itemID in txn.writeset:
#            self.dataset[itemID].write(ts)
#        self.committedTxns[txn] = context
#        if ts > self.latestTs:
#            self.latestTs = ts
#        self.logger.debug('Verified commit txn=%s ts=%s' %(txn.ID, ts))
#
#    def checkConflict(self, first, second, itemID):
#        #check status
#        #assert (first.isCommitting() or second.isCommitting())
#        #the write lock set must have some overlap
#        assert (itemID in first.writeset or itemID in second.writeset)
#
#class Linearizability(object):
#    def __init__(self, verifier):
#        self.verifier = verifier
#
#    def assignTs(self, txn):
#        """Find a valid ts for the transaction.
#
#        We first set the valid range of ts, then the ts can be an arbitrary
#        number in the range. This arbitrary pick will not invalidate a possible
#        linearizable history???
#
#        """
#        context = self.verifier.runningTxns[txn]
#        lb, ub = (-1, infinite)
#        lb = context.startTs
#        #readset
#        for itemID, version in context.readset.iteritems():
#            if lb < version:
#                lb = version
#            currVersion = self.verifier.dataset[itemID].read()
#            if currVersion != version:
#                #someone wrote a new version, we must have a ts before it
#                if ub > currVersion:
#                    ub = currVersion
#            if lb >= ub:
#                raise VerificationError(
#                    'assign ts fail on readset txn=%s item=%s '
#                    'lb=%s ub=%s version=%s currVersion=%s' 
#                    %(txn.ID, itemID, lb, ub, version, currVersion))
#        #writeset
#        for itemID in txn.writeset:
#            #we must commit after the current version.
#            currVersion = self.verifier.dataset[itemID].read()
#            if lb < currVersion:
#                lb = currVersion
#            if lb >= ub:
#                raise VerificationError(
#                    'assign ts fail on writeset txn=%s item=%s '
#                    'lb=%s ub=%s version=%s currVersion=%s' 
#                    %(txn.ID, itemID, lb, ub, version, currVersion))
#        if lb + 1 >= ub:
#            return float(lb + ub) / 2
#        if self.verifier.latestTs + 1 <= ub:
#            return self.verifier.latestTs + 1
#        return lb + 1

