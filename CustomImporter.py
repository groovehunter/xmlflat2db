import sys, os
from stringutils import suchString

# eine ebene hoeher um xmlflat2db importieren zu koennen
bpath = os.path.abspath(os.path.dirname(__file__)+"../../")
sys.path.insert(0, bpath)
#sys.path.insert(0,os.path.abspath(__file__)+"../../")


from xmlflat2db.BaseImporter import BaseImporter, now
from Datensatz import DataStore


# IDEE XXX have a subclassed DataStore 
# der weiss wie default fix werte zu setzen sind, erf_time 

class CustomDataStore(DataStore):
    def __init__(self):
        DataStore.__init__(self)
        
    def init_custom(self):
        self.data['erf_name'] = 'KOFL'
        self.data['kor_name'] = 'KOFL'
        self.data['status'] = u'3'         # XXX Warn, do not set here.



class CustomImporter(BaseImporter):

    def __init__(self):
        self.dir_app = os.path.abspath(os.path.dirname(__file__))
        self.app_id = 'mc'
        BaseImporter.__init__(self)


    def run(self):
        """ main function for all """
        BaseImporter.run(self)
        
    # XXX sollte noch rein
    ''' 
    def initDataStore(self):
        self.data_store = CustomDataStore()
        self.data_store.init_custom()
    '''

    def set_operation(self, data_in):
        """ subclass custom method  
            choose operation dependent on data set """
        # default operation
        self.operation = 'insert_or_update'

        # ERSTER SCHRITT: status feld im datensatz
        status = data_in.status
        if status == 'A':
            self.operation = 'insert_or_update'
            self.api_set('status', '3')

        elif status == 'H':
            self.operation = 'insert_or_update'
            self.api_set('status', '3')

        elif status == 'X':
            self.operation = 'update'
            self.api_set('status', '9')

        self.data_store.set_action( self.operation )

          

    def field_handler_kor_time(self):
        """ function name wird aufgerufen hier um feld in DS zu fuellen """
        dn = now()
        self.data_store.set_field('kor_time', dn)


    def field_handler_erf_time(self):
        """ setze zeit wenn insert modus """
        if self.operation == 'insert':
            dn = now()
            self.data_store.set_field('erf_time', dn)


    def field_handler_transitid(self):
        if not self.api_src_key_exists('transitid'):
            laborid     = self.api_get('laborid')
            kundenid    = self.api_get('kundenid')
            val = u'%s_%s_000' %(laborid[0], kundenid.zfill(6))
            self.api_set('transitid', val)


    def field_handler_strasse(self):
        if self.api_src_key_exists('hausnr'):
            strasse = self.api_get('strasse')
            sp = u''
            if strasse.endswith(u'.'):
                sp = u' '
            str_hnr = strasse+u' '+self.api_get('hausnr') 
            self.api_set('strasse', str_hnr)
            
            
    def field_handler_kundenstatus(self):
        status = self.api_get('status')
        print status
        self.api_set('kundenstatus', status) 


    def field_handler_adrgeaendert(self):
        # only useful on update
        if self.operation != 'update':      return
        aend = [ 'plz', 'ort', 'strasse' ]
        changed = self.api_check_anyfield_changed(aend)
        if changed and changed is True: 
            self.api_set('adrgeaendert', now())
            print "ADR geaendert: JA, flag gesetzt"


    def suchString(self, val):
        """ custom mapping func for some fields """
        return suchString( val )


    def handler_kontakt(self, sub, data_store):
        """ process sub datasets of type kontakt 
            @return data_store
        """
        fields = sub['fields']
        keyname = sub['fk']
#        keyname = self.config['db']['keyname']
        data_store.table_sub = sub['table']
        # alle 0-4 kontaktmoeglichkeiten
        for field_name in fields:
            # wenn feld in input DS vorhanden
            # XXX subitem als CustomDataStore ??
            subitem = {}
            if field_name in self.data_in.data:
                subitem['coid'] = data_store.data['coid']
                subitem['typ']  = field_name[:3]
                subitem['kontakt'] = self.data_in.data[field_name]
                subitem['status']  = '3'
                
                data_store.add_data_sub(subitem)
                
        return data_store


    def field_handler_suchname(self):
        #print self.data_in.data.keys()
        tmp2 = self.data_in.data['name']
        if 'vorname' in self.data_in.data:
            tmp2 += self.data_in.data['vorname']
        
        for i in range(2,4):
            key = 'zeile'+str(i)
            if key in self.data_in.data:
                tmp2 += self.data_in.data[key]
                
        v2 = self.suchString( tmp2 )
        #print v2

        self.data_store.set_field('suchname', v2)
        




