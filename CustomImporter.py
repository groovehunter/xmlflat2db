import sys, os
from stringutils import suchString

# eine ebene hoeher um xmlflat2db importieren zu koennen
bpath = os.path.abspath(os.path.dirname(__file__)+"../../")
sys.path.insert(0, bpath)
#sys.path.insert(0,os.path.abspath(__file__)+"../../")


from xmlflat2db.BaseImporter import BaseImporter, now
from Datensatz import DataStore


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
            self.operation = 'update'
            self.api_set('status', 9)

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
        dn = now()
        self.data_store.set_field('kor_time', dn)

    # XXX fix values -> config
    def field_handler_kor_name(self):
        val = u'KOFL' 
        self.data_store.set_field('kor_name', val)

    def field_handler_erf_name(self):
        if self.operation == 'insert':
            self.data_store.set_field('erf_name', u'KOFL')

    def field_handler_erf_time(self):
        if self.operation == 'insert':
            dn = now()
            self.data_store.set_field('erf_name', dn)

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


    def handler_kontakt(self, fields):
        keyname = self.config['db']['keyname']
        sub_store = DataStore()
        table_name = 'kontakt'
        try:
            print self.data_store.data['name']
        except:
            pass # lol
        # alle 0-4 kontaktmoeglichkeiten
        for field_name in fields:
            # wenn feld in input DS vorhanden
            if field_name in self.data_in.data:
                sub_store.set_field( keyname, self.data_store.data[keyname] )
                sub_store.set_field( 'typ', field_name[:3] )
                sub_store.set_field( 'kontakt', self.data_in.data[field_name] )
                sub_store.set_field( 'status', '3' )
                #sub_store.dump()
        return sub_store


    def field_handler_suchname(self):
        sname = self.suchString( self.data_in.data['name'] )
         
        z2 = self.suchString( self.data_in.data['zeile2'] )
        for w in self.config_importer['mylist01']:
            z2 = z2.replace(w,'')
        
        z3 = self.suchString( self.data_in.data['zeile3'] )

        gname = sname+z2
        if not (z3.find('MEDIZIN') or z3.find('ARZT')):
            gname += z3

        for w in self.config_importer['mylist02']:
            gname = gname.replace(w,'')
        self.data_store.set_field('suchname', gname)
        




