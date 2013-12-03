
#from types import dict
#from random import random
from uuid import uuid1


# XXX aus config
attrs_special = [
'status',
]
# XXX beide klassen zusammenlegen

class Datensatz(object):

    def __init__(self):
        self.data = {}
        
        if not hasattr( self, 'uid'):
            self.uid = uuid1().get_hex()


    def setze_per_dict(self, val_dict):

        #print val_dict
        for attr_special in attrs_special:
            if attr_special in val_dict:
                setattr(self, attr_special, val_dict[attr_special])
                # XXX bereits entfernen oder nicht? ZENSUR!! :-)
                #status = val_dict.pop('status')

        for key in val_dict.keys():
            self.data[key] = val_dict[key]


    def get_uid(self):
        return self.uid


    def dump(self):
        out = ''
        for key in self.data.keys():
            out += "%s|%s\n" %(key,self.data[key])

        return out


    def as_dict(self):
        return self.data

    def set_attr_random(self, attr, length=12):
        uuid = uuid1().get_hex()[:length]
        self.data[attr] = uuid



class DataStore(object):
    """ objekt um fuer DB-speicherung fertig aufbereitete daten 
        aufzunehmen """

    def __init__(self):
        self.data = {}
        self.action = None
        # XXX soll es attribute haben wie:
        # _all-not-null-fields-ready?_
        # ready-for-saving ?
    
    def set_action(self, action):
        self.action = action


    def set_field(self, key, val):
        self.data[key] = val


    def dump(self):
        keys = self.data.keys()
        keys.sort()
        for key in keys:
            print key + "\t\t",
            try:
                print self.data[key]
            except:
                print "cannot print"








