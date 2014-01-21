
#from types import dict
#from random import random
from uuid import uuid1
import logging as l


#TODO: aus config
attrs_special = [
'status',
]
# #TODO: beide klassen zusammenlegen

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

        for key in val_dict.keys():
            self.data[key] = val_dict[key]


    def get_uid(self):
        return self.uid


    def dump(self):
        for key in self.data.keys():
            try:
                l.debug(self.data[key])
            except:
                l.debug("ENC!")
            l.debug( "\t\t %s" % type(self.data[key]))


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
        self.data_subitems = {} # index them by a key a 2nd one
        self.tablename = ''
        self.table_sub = ''
        self.action = None
        self.uid = None
        # #TODO: soll es attribute haben wie:
        # _all-not-null-fields-ready?_
        # ready-for-saving ?
        self.subkey = 'typ'  # #TODO: is custom!  
        
    def init_custom(self):
        """ implement in subclass """
        pass
    
    def set_action(self, action):
        self.action = action


    def set_field(self, key, val):
        self.data[key] = val

    def add_data_sub(self, data_sub):
        keyval = data_sub[self.subkey]
        self.data_subitems[keyval] = data_sub
    
    
    def get_sub_by_key(self, key):
        return self.data_subitems[key]



    def dump2(self):
        # #TODO: remove, custom!
        keys = ['coid','kundenid','laborid']
        #self.dumpw(self.data, keys)
        l.debug( "Anzahl subelemente: %s") % len(self.data_subitems)
        l.debug( str(self.data_subitems) )
        

    def dump(self):
        l.info("-------------")
        keys = self.data.keys()
        keys.sort()
        #self.dumpw(self.data, keys)
 


    def dumpw(self, data, keys):
        for key in keys:
            print key + "\t\t",
            try:
                print data[key]
            except:
                print "cannot print"







