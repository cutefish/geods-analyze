
class PrettyFloat(float):
    def __repr__(self):
        return '%8.2f' %self

class RelativeEqualFloat(float):
    def __init__(self, value, abstol=1e-4, reltol=1e-3):
        float.__init__(self, value)
        self.abstol = abstol
        self.reltol = reltol
        self.value = value

    def __eq__(self, other):
        value1 = abs(self.value)
        value2 = abs(other.value)
        if value1 < self.abstol and \
           value2 < self.abstol:
            return True
        maxval = max(value1, value2)
        minval = min(value1, value2)
        if (maxval - minval) / minval < self.reltol:
            return True
        return False
