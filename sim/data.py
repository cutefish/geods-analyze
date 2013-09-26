from sim.core import IDable
import sim
from sim.locking import Lockable

class ItemID(IDable):
    GID_PRINT_WIDTH = 2
    IID_PRINT_WIDTH = 5
    def __init__(self, gid, iid):
        IDable.__init__(self, 'G%s/I%s'%(gid, iid))
        self.gid = gid
        self.iid = iid

    def __eq__(self, other):
        return (self.gid == other.gid) and \
                (self.iid == other.iid)

    def __hash__(self):
        return self.gid.__hash__() << 16 + \
                self.iid.__hash__()

    def __str__(self):
        return '%s:%s'%(
            str(self.gid).zfill(ItemID.GID_PRINT_WIDTH),
            str(self.iid).zfill(ItemID.IID_PRINT_WIDTH))

class Item(Lockable):
    def __init__(self, repID, itemID):
        Lockable.__init__(self, 'R%s/%s'%(repID, itemID.ID))
        self.itemID = itemID
        self.gid = itemID.gid
        self.iid = itemID.iid
        self._version = -1
        self._value = None
        self.lastWriteTxn = None

    @property
    def version(self):
        return self._version

    def read(self):
        return self._value, self._version

    def write(self, value, version=None):
        self._value = value
        if version is None:
            self._version += 1
        else:
            self._version = version

    def verifyConsistent(self, replica):
        assert self.itemID == replica.itemID
        assert (self._version == replica._version) and \
                (self._value == replica._value), \
                '(%r, %r, txn=%s)'%(self, replica, self.lastWriteTxn)

    def __repr__(self):
        return '%s:%s:%s %s'%(
            self.ID, self._value, self._version, Lockable.__repr__(self))

class Group(IDable):
    def __init__(self, repID, gid, size):
        IDable.__init__(self, 'R%s/G%s'%(repID, gid))
        self.gid = gid
        self.items = []
        for i in range(size):
            itemid = ItemID(gid, i)
            item = Item(repID, itemid)
            self.items.append(item)

    @property
    def size(self):
        return len(self.items)

    def __getitem__(self, itemID):
        assert itemID.gid == self.gid
        return self.items[itemID.iid]

    def iteritems(self):
        for item in self.items:
            yield item

    def __str__(self):
        return '%s(%s)'%(self.ID, self.size)

class Dataset(IDable):
    """A replication of the whole dataset"""
    def __init__(self, repID, gconfigs):
        IDable.__init__(self, 'D%s'%repID)
        self.groups = {}
        for gid, size in gconfigs.iteritems():
            self.groups[gid] = Group(repID, gid, size)

    def __getitem__(self, itemID):
        group = self.groups[itemID.gid]
        return group[itemID]

    def __str__(self):
        return '%s:{'%self.ID + ', '.join([str(g) for g in self.groups.values()]) + '}'
