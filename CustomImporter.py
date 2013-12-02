import sys, os
from stringutils import suchString

# eine ebene hoeher um xmlflat2db importieren zu koennen
bpath = os.path.abspath(os.path.dirname(__file__)+"../../")
sys.path.insert(0, bpath)
#sys.path.insert(0,os.path.abspath(__file__)+"../../")


from xmlflat2db.BaseImporter import BaseImporter
from datetime import datetime

def now():
    now = datetime.now()
    now_iso = datetime.isoformat(now)
    return now_iso


class CustomImporter(BaseImporter):


    def __init__(self):
        self.dir_app = os.path.abspath(os.path.dirname(__file__))
        self.app_id = 'mc'
        BaseImporter.__init__(self)


    def run(self):

        BaseImporter.run(self)


    def set_operation(self, data_in):
        """ subclass custom method  
            choose operation dependent on data set """
        # default operation
        self.operation = 'insert_or_update'

        # ERSTER SCHRITT: status feld im datensatz
        status = data_in.status
        if status == 'A':
            self.operation = 'insert_or_update'

        elif status == 'H':
            self.operation = 'insert_or_update'

        elif status == 'X':
            #self.operation = 'delete'
            self.operation = 'update'
            self.operation_args = ('status', 9)

        self.data_store.set_action( self.operation )

        '''
        if self.operation == 'insert_or_update':
            # check unique keys here
            # key combos # laborid & kundenid
            res = self.store.exist_keys(self.data_in.data, ('kundenid','laborid') )
            if not res:
                self.operation = 'insert'
            else:
                self.data_in.data[self.keyname] = res
                self.operation = 'update'
        '''
        

    def field_handler_kor_time(self):
        """ function name wird aufgerufen hier um feld in DS zu fuellen """
#        self.data_store.set_field('kor_time', now())

    def field_handler_kor_name(self):
        val = 'KOFL' 
        self.data_store.set_field('kor_name', val)
       

    def field_handler_erf_name(self):
        if self.operation == 'insert':
            self.data_store.set_field('erf_name', 'KOFL')

    def field_handler_erf_time(self):
        if self.operation == 'insert':
            self.data_store.set_field('erf_name', now())


    def suchString(self, val):
        return suchString( val )









