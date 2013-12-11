#from config_main import *
import os
import subprocess
from DummyStore import DummyStore
from SourceScan import SourceScan
from Datensatz import Datensatz, DataStore
from lxml import etree
from lxml.etree import iterparse, XMLSyntaxError, ParseError
import yaml
from datetime import datetime


class MissingDataException(Exception): pass
class ImporterError(Exception):         pass
class FormatError(Exception):       pass


## helper functs
def now():
    now = datetime.now()
    #now_iso = datetime.isoformat(now)
    return now #now_iso



class BaseImporter(SourceScan):

    def __init__(self):
        self.source = None
        self.src_failed = []
        self.test = ''
        # fuer in-memory operationsmodus
        self.keep_in_memory = False
        self.data_array = {}

        self.operation = None
        # hier muss subclass setzen:
        # store typ


    def load_config(self):
        self.config_array = {}

        cfgs = [
            'config_importer.yml',
            'config_main.yml'
        ]
        #pjdir = os.path.abspath(os.path.dirname(__file__))

        for fn in cfgs:
            config_fpath = '%s/config_%s%s/%s' %(
                    self.dir_app, self.app_id, self.test, fn) 
            #print config_fpath
            config_txt = file(config_fpath,'r').read()
            config = yaml.load(config_txt)
            cfgkey = fn.split('.')[0]
            self.config_array[cfgkey] = config

        self.config         = self.config_array['config_main']
        self.config_importer= self.config_array['config_importer'] 

        #self.config_set(self.config)   # stuff XXX

        self.src_main_dir   = self.config['src_main_dir']
        self.src_labor      = 'BIOS'
        self.DATASET_TAG    = self.config['DATASET_TAG']



    def init(self):
        """ init. NUR den importer, kein store hier voraussetzen """
        #self.config_define()   # config von subclass
        self.load_config()
        # setzt vorraus dass store gesetzt ist
        #self.config_set(self.config)

    """
    def config_set(self, config):
        self.config = config
        for ckey in config.keys():
            setattr(self, ckey, config[ckey])
    """

    def init_db(self, config):
        self.set_storage( self.config['typ_store'] )

        self.store.config_set( self.config['db'] )
        # was noch?
        self.store.connect()
        self.id_max = self.store.get_start_id() +1



    def run(self):
        """ START: Ein Lauf pro Zeiteinheit (zb 1h) """
        self.init()
        self.scan_source_dirs_all()

        self.loop_src_dirs()
        # final check!? 
        self.report_manual_todo()
        all_done = self.no_files_left()
        print "ALL DONE? ", str(all_done)
        print "LEFT: "+str(self.src_sub_dirs_todo)


    def set_storage(self, typ):
        store = None
        if typ == 'informix':
            from InformixDbStore import InformixDbStore
            store = InformixDbStore()
        if typ == 'mysql':
            from MysqlDbStore import MysqlDbStore
            store = MysqlDbStore()
        if typ == 'sqlite':
            from SqliteDbStore import SqliteDbStore
            store = SqliteDbStore()
        if not store:
            from DbStore import DummyStore
            store = DummyStore()

        self.store = store



    def parse(self):
        """ parse current source file, check encoding """
        try:
            tree = etree.parse(self.src_cur)
            self.encoding = tree.docinfo.encoding
            self.root = tree.getroot()
        except ParseError:
            raise ImporterError



    def work(self):
        """ work on xml data source and call store workers 
            sollte success=True zurueckgeben wenn datei
            zufriedenstellend verarbeitet wurde (was heisst das?)
        """

        try:
            self.parse()
        except ImporterError:
            # hier merken die failed files.
            pass

        data = {}
        keys_collect = []
        self.id_tmp = 0
        self.num_ds_written = 0
        self.num_ds_given = 0

        ### here
        self.init_db(self.config['db'])
        try:
            tree = etree.parse(self.src_cur)
        except:
            # XXX handle exception 
            return False
        self.encodig = tree.docinfo.encoding
        xmlfile = open(self.src_cur)
        with xmlfile as xml_doc:
            context = iterparse(xml_doc, events=("start", "end"))

            #self.iter_context(context)

            try:
                for event, elem in context:
                    if elem.tag == self.DATASET_TAG:

                        if event == "start":
                            pass

                        elif event == "end":
                            data = {}
                            for child in elem:
                                if child.text is None:
                                    val = None
                                if type(child.text) == type(u""):
                                    val = child.text
                                if type(child.text) == type(""):
                                    val = child.text.decode(self.encoding)
                                    
                                data[child.tag] = val

                            data_in = Datensatz()
                            data_in.setze_per_dict(data)
                            
                            if self.keep_in_memory:
                                self.data_array[ data_in.get_uid() ] = data_in
                            else:
                                # MAIN CALL
                                self.work_ds(data_in)
                                
                            del data_in
                                
                    if elem.getparent() is None:
                        break
                               
                src_success = True
                
            except XMLSyntaxError:
                self.src_failed.append(self.src_cur)
                print "DATASET FAILED "+str(context.root)
                src_success = False
        
        print "src_success: "+str(src_success)
        return src_success


    def work_ds(self, data_in):
        """ steuert einen durchgang komplett """
        #print "WORK ON " + data_in.data['name']
        # erhaelt flachen datensatz
        # initialisiert data_store und mehr
        self.initDataStore()
        self.operation = None

        # setzt aktions-feld im data_store
        #data_in.dump()
        self.set_operation(data_in)
        self.data_in = data_in
        #print data_in.data

        ### === fields 
        # werte weitere wichtige felder aus
        # werfe unnuetze felder weg
        # sammle felder fuer operationen ein, gebe ops frei

        # berechne fehlende Felder des DS
        self.fields_calc()

        # a) auto felder
        # e) aktuelle autoinc id 
        self.field_autoinc()
        # b) aus quell-daten
        # c) muss-felder = none null felder 1:1
        # d) vielleicht-felder aus source , jetzt?

        # felder mit typ-spezieller sonderbehandlung
        self.fields_typed_date()
        self.fields_typed_bool()
        self.fields_typed_int()

        # f) dest-felder die aus quellfeldern berechnet werden
        self.fields2calc()
        self.fields_dest_fetch()

        # alle weiteren felder
        self.fields_do_transfer()

        # AB HIER ist operation bekannt und fields_auto braucht das auch
        self.set_operation_final(self.data_store)
        self.fields_fix()
        self.fields_auto()

        # g) felder fuer subtabellen, inkl operation ausfuehren (?)
        self.data_store = self.fields_to_subtables(self.data_store)

        #print self.data_store
        self.fields_encode_all()
        ### === storage 
        # setzt op in data_store auch?

        #self.data_store.dump2()
        # operation im DB backend auf den weg bringen
        self.operate( self.data_store )
        # pruefe ob vollstaendige verarbeitung
        self.num_ds_written += 1

        #  RETURN zu naechster runde


    def initDataStore(self):
        self.data_store = DataStore()
        self.data_store.init_custom()

        self.data_existing = DataStore()


    def report_manual_todo(self):
        while self.src_failed:
            print "FAILED file : ", self.src_failed.pop()

        return True


    

    def fields_encode_all(self):
        """ encode all text fields finally in data_store to desired db encoding """
        ds_keys = self.data_store.data.keys()
        for key in self.config_importer['fields_typed_date']:
            if key in ds_keys:
                ds_keys.remove(key)
        for key in self.config_importer['fields_typed_bool']:
            if key in ds_keys:
                ds_keys.remove(key)
        for key in self.config_importer['fields_typed_int']:
            if key in ds_keys:
                ds_keys.remove(key)
                
        for key in ds_keys:
            #print key
            self.data_store.data[key] = self.data_store.data[key].encode('utf8')


    def fields_auto(self):
        """ setze auto-felder """
        fields_auto = self.config_importer['fields_auto']
        if fields_auto:
            for key in fields_auto:
                hdl = "self.field_handler_"+key+"()"
                #print "call handler: ",hdl
                eval(hdl)

    def fields_fix(self):
        """ setze felder mit festen default werten """
        fields_fix = self.config_importer['fields_fix']
        if fields_fix:
            for key in fields_fix.keys():
                self.data_store.set_field(key, fields_fix[key])
                

    def field_autoinc(self):
        """ auto inc id im data store setzen """
        id_cur = self.id_max + self.id_tmp
        self.data_store.set_field( self.config_importer['field_autoinc'], 
            id_cur)
        self.id_cur = id_cur


    def fields_set_not_null(self):
        """ bereitstellen der not-null fields 
            hier koennte man aufrufen den transfer mit einer liste """
        for fn in self.config_importer['fields_not_null']:
            data = self.data_in.data
            if fn in data:
                val = data[fn]
                self.data_store.set_field(fn, val)
            else:
                raise MissingDataException


    def fields2calc(self):
        """ wende verarbeitungsfunktion an auf div.konfigurierte felder """
        for calc in self.config_importer['fields2calc']:

            for pair in calc['field_map_dict'].items():
                if pair[1] in self.data_in.data:
                    exstr = "sval=self."+calc['mapfunc']+"(self.data_in.data[pair[1]])"
                    exec exstr

                    self.data_store.set_field( pair[0], sval )


    # ## FIELD TYPES                                     ------------------------------------------------------ 
    def fields_typed_date(self):
        """ transferiere felder des typs datum """
        for fn in self.config_importer['fields_typed_date']:
            data = self.data_in.data
            if fn in data:
                sval = self.prep_date(data[fn])
                self.data_store.set_field(fn, sval)
            else:
                pass  # log XXX

    # XXX man koennte pruefen in diesen typ-routinen ob das feld schon behandelt wurde zb mit handler
    def fields_typed_bool(self):
        """ transferiere felder des typs bool """
        for fn in self.config_importer['fields_typed_bool']:
            data = self.data_in.data
            if fn in data:
                #print data[fn]
                if data[fn] == 'true':
                    self.data_store.set_field(fn, True)
                elif data[fn] == 'false':
                    self.data_store.set_field(fn, False)
                else:
                    self.data_store.set_field(fn, None)
            else:
                pass # wie oben, kann eigentlich nicht passieren(?)     

    def fields_typed_int(self):
        """ transferiere felder des typs integer """
        for fn in self.config_importer['fields_typed_int']:
            data = self.data_in.data
            if fn in data:
                #print fn, data[fn]
                try:
                    sval = int(data[fn])
                except:
                    sval = ''
                    # XXX log here
                self.data_store.set_field(fn, sval)
            else:
                pass  # log XXX
       

    def prep_date(self, v):
        """ if data in US format, convert to custom """
        try:
            d = datetime.strptime(v, self.config_importer['format_date_src'])
            return d

        except ValueError:
        
            try:
                dform = '%Y-%m-%dT%H:%M:%S+01:00'
                d = datetime.strptime(v, dform)
                return d.strftime(self.config_importer['format_date_dest'])
            except ValueError:
                # XXX log
                #raise FormatError
                return None
                

    def get_existing(self):
        """ load existing data from DB, return True or False for match """
        uid = self.data_store.data[self.config['db']['keyname']]

        self.store.query_create_select_all(self.config_importer['fields_dest_all'], uid)
        res = self.store.selectall()
        if res:
            self.data_existing.data = res[0]
            return True
        else:
            return False


    # ## api stuff for application subclass MyImporter to use                ------------------------
    def api_check_anyfield_changed(self, keys):
        """ checks if any field given has changed compared to existing data or None 
            if no data existed yet """
        found = self.get_existing()
        if not found:
            return None
        
        changed = False
        for key in keys:
            if self.data_existing.data[key] != self.data_in.data[key]:
                changed = True
        return changed
    
    
    def api_src_key_exists(self, key):
        """ return bool if a key exists in the src """
        return key in self.data_in.data and self.data_in.data[key]


    def api_get(self, key):
        """ get a field of data source """ 
        return self.data_in.data[key]


    def api_set(self, key, val):
        """ setze feld auf wert in dest dict """
        self.data_store.set_field(key, val)


    def fields_dest_fetch(self):
        """ spezielle felder in dest sollen aus mehreren src feldern 
            errechnet werden """
        if self.config_importer['fields_dest_fetch']:
            for fetch in self.config_importer['fields_dest_fetch']:
                key = fetch.keys()[0]
                eval('self.field_handler_'+key+'()')
                # XXX 


    def fields_do_transfer(self):
        """ kopieren aller restlichen felder die nicht anderweitig
            behandelt werden """
        for fn in self.fields_transfer:
            data = self.data_in.data
            if fn in data:
                val = data[fn]
                sval = val
                ### leave here for unicode
                self.data_store.set_field(fn, sval)
            else:   
                pass
                # XXX store in cloud :)


    def fields_to_subtables(self, data_store):
        """ create data stores to write to subtables with 
            relation to main table """
        
        for sub in self.config_importer['fields_to_subtables']:
            table_name = sub['table']

            exec('data_store = self.handler_'+table_name+'(sub, data_store)')
 
        return data_store



    def fields_calc(self):
        """ algoritmisches ermitteln aller feld-listen fuer fkt. """
        # transfer_fields

        # von allen ueberhaupt moeglichen dest fields subtrahieren das 
        # was immer im dest ausgelassen werden soll
        fields_tmp = []
        for fn in self.config_importer['fields_dest_all']:
            if not fn in self.config_importer['fields_dest_ignore_always']:
                fields_tmp.append(fn)
        fields_tmp_dest_all_possible = fields_tmp

        # von allen dest wanted subtrahiere was 
        # gar niemals im src vorkommen kann
        fields_tmp = []
        for fn in fields_tmp_dest_all_possible:
            if not fn in self.config_importer['fields_src_never_in']:
                fields_tmp.append(fn)
        fields_wanted = fields_tmp

        # von den generell wanted fields entferne die date-type felder
        fields_tmp = []
        for fn in fields_wanted:
            if not fn in self.config_importer['fields_typed_date']:
                fields_tmp.append(fn)
        fields_wanted_wo_date = fields_tmp
                
        self.fields_transfer = fields_wanted_wo_date
        #print str(fields_tmp)

        fields_tmp = []
        for fn in fields_wanted_wo_date:
            if not fn in self.config_importer['fields_typed_bool']:
                fields_tmp.append(fn)
        fields_wanted_wo_datebool = fields_tmp
                
        fields_tmp = []
        for fn in fields_wanted_wo_datebool:
            if not fn in self.config_importer['fields_typed_int']:
                fields_tmp.append(fn)
        fields_wanted_wo_dateboolint = fields_tmp

# ###   self.fields_transfer = fields_wanted_wo_date
        self.fields_transfer = fields_wanted_wo_dateboolint
        #print str(fields_tmp)
        

    def set_operation_final(self, data_store):
        # entscheide ob insert oder update
        #self.data_store.dump()
        if self.store.exist_keys( data_store, 
                                self.config_importer['fields_unique'] ):
            self.operation = 'update'
        else:
            self.operation = 'insert'
        

    def operate(self, data_store):
        # uebergebe data_store and store
        #self.store.set_data_store(data_store)

        if self.operation == 'update':
            self.update(data_store)
        elif self.operation == 'insert':
            self.insert(data_store)
        # 


    # XXX so okay? was ist custom delete zb = update mit status=9
    def insert(self, data_store):
        #data_store = self.store.data_store
        self.store.query_create_insert(data_store.data, self.config['db']['tablename'])
        self.store.insert(data_store.data)
        
        if data_store.data_subitems:
            
            for subitem in data_store.data_subitems.values():
                #print "SUB insert"

                # wenn sub mit coid schon existiert: loeschen und neu anlegen
                if self.store.exists(subitem, self.config['db']['subtablename']):
                    self.store.query_create_delete(subitem, self.config['db']['subtablename'] )
                    self.store.delete()
                    
                    self.store.query_create_insert(subitem, self.config['db']['subtablename'] )
                    self.store.insert(subitem)
                

    def update(self, data_store):
        """ updaten des gesamten datenstruktur inkl subtable? """
        self.store.query_create_update(data_store.data, self.config['db']['tablename'])
        self.store.update(data_store.data)
        if data_store.data_subitems:
            #print "sub update"
            # wenn sub existiert: update
            # wenn nicht: inserten
            subtablename = self.config['db']['subtablename']
            for subitem in data_store.data_subitems.values():
                #print subitem

                if self.store.exists(subitem, subtablename):
                    self.store.query_create_update(subitem, subtablename)
                    self.store.update(subitem)
                else:
                    self.store.query_create_insert(subitem, subtablename)
                    self.store.insert(subitem)
                # XXX custom!
                val = subitem['kontakt']
                res = self.store.query_create_select_by( 'kontakt', val, subtablename)
                if res:
                    print "ENTRY FOUND !!"
                    self.store.query_create_delete(key, val, tablename)
                    self.store.delete( )

       


