
class CustomArgsParser():
    """
    CustomArgsParser:
        Parses args with customized option keys.

        Args and options are seperated by space. Strings within a pair of
        quotes are of the same arg or option. Only specified customized options
        are recognized and extracted. Other args are kept intact in order.
    """
    def __init__(self, optKeys=[], optFlags=[], defaults={}):
        self.optKeys = optKeys
        self.optFlags = optFlags
        self.options = defaults
        self.posArgs = []

    def parse(self, args):
        while len(args) > 0:
            arg = args.pop(0)
            if arg in self.optKeys:
                self.options[arg] = args.pop(0)
            if arg in self.optFlags:
                self.options[arg] = True
            else:
                self.posArgs.append(arg)

    def getPosArgs(self):
        return self.posArgs

    def getPosArg(self, idx):
        return self.posArgs[idx]

    def getOption(self, key, default=None):
        if self.options.has_key(key):
            return self.options[key]
        elif key in self.optFlags:
            return False
        else:
            return default

    def getOptions(self):
        return self.options

