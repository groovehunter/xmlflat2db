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
from datetime import date
import logging as l


class MissingDataException(Exception): pass
class ImporterError(Exception):         pass
class FormatError(Exception):       pass


## helper functs
def now():
    now = datetime.now()
    return now 



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
        self.arg_subdirs_wanted = None
        self.arg_files_wanted = None


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

        #self.config_set(self.config)   # stuff #TODO

        self.src_main_dir   = self.config['src_main_dir']


    def init(self):
        """ init. NUR den importer, kein store hier voraussetzen """
        #self.config_define()   # config von subclass
        self.load_config()
        # setzt vorraus dass store gesetzt ist
        #self.config_set(self.config)
        llmap = {   'debug' :   l.DEBUG,
                    'info':     l.INFO,
                    'warning':  l.WARNING,
                }
        today = date.today()
        today_fmt =today.strftime('%y%m%d') 
        logfile = self.config['logfile']+ '_' + today_fmt +'.log'
        print "LOGFILE: "+logfile
        l.basicConfig(filename= logfile, 
                            level= llmap[self.config['loglevel']])
        l.info('Started importer')


    def init_db(self, config):
        self.set_storage( self.config['typ_store'] )

        self.store.config_set( self.config['db'] )
        # was noch?
        self.store.connect()
        self.id_max = self.store.get_start_id() +1



    def run(self):
        """ START: Ein Lauf pro Zeiteinheit (zb 1h) """
        self.init()
        # AB HIER logging!
        self.scan_source_dirs_all()

        l.info("Test mode? %s" %str(self.test))

        # wenn als cli argument angegegeben, nur dies bearbeiten und stoppen
        # als "return" hier unschoen.
        if self.arg_files_wanted:
            for fn in self.arg_files_wanted:
                self.src_cur = self.src_main_dir + self.src_sub_dir + '/'+fn
                #self.src_cur = self.abscwd + '/' +fn
                self.src_sub_dir_cur = self.src_main_dir + self.src_sub_dir
                self.source_work()
                
#        elif self.arg_subdirs_wanted:

        
        else:
            self.loop_src_dirs()

            # final check!? 
            self.report_manual_todo()
            all_done = self.no_files_left()
            print "ALL DONE? ", str(all_done)
            print "LEFT: "+str(self.src_sub_dirs_todo)
            l.info("LEFT: "+str(self.src_sub_dirs_todo))


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
            self.src_failed.append(self.src_cur)
            self.encoding = 'iso-8859-5'
            #raise ImporterError


    def work(self):

        """ work on xml data source and call store workers 
            sollte success=True zurueckgeben wenn datei
            zufriedenstellend verarbeitet wurde (was heisst das?)
        """

        self.id_tmp = 0
        self.num_ds_written = 0
        self.num_ds_given = 0
        src_success = False
        ### here ??
        self.init_db(self.config['db'])
        self.parse()
        
        xmlfile = open(self.src_cur)

        with xmlfile as xml_doc:
            context = iterparse(xml_doc, events=("start", "end"))

            try:
                for event, elem in context:
                    if elem.tag == self.config['DATASET_TAG']:
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
                            data_in.use = 'Incoming Data'
                            data_in.setze_per_dict(data)
                            data_in.dump()
                            self.id_tmp += 1

                            if self.keep_in_memory:
                                self.data_array[ data_in.get_uid() ] = data_in
                            else:
                                # MAIN CALL
                                """ WORKAROUND XXX REMOVE!
                                if 'kundennr' in data_in.data:
                                    if data_in.data['kundennr'] == u'586131':
                                        continue
                                if 'kundenid' in data_in.data:
                                    if data_in.data['kundenid'] in [ u'_15470___2', u'592293___2', u'589958___2',u'772814___2']:
                                        continue
                                """
                                self.work_ds(data_in)
                                
                            del data_in
                                
                    if not event == 'start' and elem.getparent() is None:
                        break
                               
                src_success = True
                
            except XMLSyntaxError:
                self.src_failed.append(self.src_cur)
                l.warning( "DATASET FAILED "+self.src_cur )
                src_success = False
        
        l.debug( "src_success: "+self.src_cur)
        return src_success


    def work_ds(self, data_in):
        """ steuert einen durchgang komplett 
            @return success (bool)
        """
        l.debug( "WORK ON " + str(data_in.dump()) )
        l.info("SOURCE INFO: "+self.src_cur)
        # initialisiert data_store und mehr
        self.initDataStore()
        self.operation = None

        # setzt aktions-feld im data_store
        # HIER als entwurf: op feld muss vorhanden sein!
        fields_src_operation = self.config_importer['fields_src_operation']
        field1 = fields_src_operation[0]

        if not field1 in data_in.data:
            #l.warn('MUSS feld %s not existing' %self.fields_src_operation)
            return
            # WORKAROUND! 
            data_in.data[ field1 ] = 'A'
        
        self.set_operation(data_in)
        self.data_in = data_in

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
        self.fields_auto_tmp()

        # felder mit typ-spezieller sonderbehandlung
        self.fields_typed_date()
        self.fields_typed_bool()
        self.fields_typed_int()

        # f) dest-felder die aus quellfeldern berechnet werden
        self.fields2calc()
        self.fields_dest_fetch()

        self.fields_set_not_null()
        # alle weiteren felder
        self.fields_do_transfer()

        # AB HIER ist operation bekannt und fields_auto braucht das auch
        self.set_operation_final(self.data_store)
        self.fields_fix()
        self.fields_auto()

        # g) felder fuer subtabellen, inkl operation ausfuehren (?)
        self.data_store = self.fields_to_subtables(self.data_store)

        l.debug( self.data_store.dump() )
        self.fields_encode_all()
        ### === storage 
        # setzt op in data_store auch?

        # operation im DB backend auf den weg bringen
        self.operate( self.data_store )
        # pruefe ob vollstaendige verarbeitung
        self.num_ds_written += 1

        #  RETURN zu naechster runde


    def initDataStore(self):
        # setze haupt-datensatz
        self.data_store = DataStore()
        self.data_store.use = 'Outgoing Data'
        # init. zwischen-speicher DS
        self.data_tmp = DataStore()
        # 
        self.data_store.init_custom()

        self.data_existing = DataStore()


    def report_manual_todo(self):
        while self.src_failed:
            print "FAILED file : ", self.src_failed.pop()

        return True
    

    def fields_encode_all(self):
        """ encode all text fields finally in data_store to desired 
            db encoding """
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
                
        dest_enc = self.config['db']['encoding']
        for key in ds_keys:
            if self.data_store.data[key] == None:
                self.data_store.set_field(key, u''.encode(dest_enc))
            else:
                self.data_store.data[key] = self.data_store.data[key].encode(dest_enc)


    def fields_auto(self):
        """ setze auto-felder """
        fields_auto = self.config_importer['fields_auto']
        if fields_auto:
            for key in fields_auto:
                hdl = "self.field_handler_"+key+"()"
                #print "call handler: ",hdl
                eval(hdl)
                
    def fields_auto_tmp(self):
        fields_auto_tmp = self.config_importer['fields_auto_tmp']
        if fields_auto_tmp:
            for key in fields_auto_tmp:
                hdl = "self.field_handler_tmp_"+key+"()"
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
            hier koennte man aufrufen den transfer mit  einer liste 
            #TODO exception bei zB fehlendem Namen?
        """
        for fn in self.config_importer['fields_not_null']:
            data = self.data_in.data
            '''
            if fn in data and not data[fn] == None:
                val = data[fn]
                self.data_store.set_field(fn, val)
            else:
                raise MissingDataException
            '''
            if not fn in data:
                val = u''
            if fn in data and data[fn] == None:
                val = u''
            if fn in data:
                val = data[fn]
            
            self.data_store.set_field(fn, val)


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
                pass  # log #TODO

    # #TODO man koennte pruefen in diesen typ-routinen ob das feld schon behandelt wurde zb mit handler
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
                    # #TODO log here
                self.data_store.set_field(fn, sval)
            else:
                pass  # log #TODO
       

    def prep_date(self, v):
        """ if data in US format, convert to custom """
        # datetime formats - mehrere moegliche angeben in config und hier durchlaufen
        format_date_src = self.config_importer['format_date_src']
        for format in format_date_src:
            #l.debug("Trying format on current date field... "+format )
            try:
                d = datetime.strptime(v, format)
                if d:
                    val_out = d.strftime(self.config_importer['format_date_dest'])
                    l.debug("Found format %s in current field, output is %s " %(format,val_out) )
                    #return val_out
                    return d
            except ValueError:
                l.error("ValueError")
            #    return None
                    

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
        l.debug('pruefe keys auf aenderung: %s' %str(keys))

        
        for key in keys:
            # falls feld gar nicht in incoming data, skip it
            if not key in self.data_in.data:
                continue
            
            if key in self.data_existing.data:
                l.debug("Existing data : %s " %self.data_existing.data[key] )
                l.debug("Incoming data : %s " %self.data_in[key])
            else:
                return True
            
            if self.data_existing.data[key] != self.data_in.data[key]:
                changed = True
        return changed
    
    
    def api_src_key_exists(self, key):
        """ return bool if a key exists in the src """
        return key in self.data_in.data and self.data_in.data[key]


    def api_get(self, key):
        """ get a field of data source """
        if key in self.data_in.data:
            return self.data_in.data[key]
        else:
            return None


    def api_set(self, key, val):
        """ setze feld auf wert in dest dict """
        self.data_store.set_field(key, val)

    def api_set_tmp(self, key, val):
        self.data_tmp.set_field(key, val)

    def fields_dest_fetch(self):
        """ spezielle felder in dest sollen aus mehreren src feldern 
            errechnet werden """
        if self.config_importer['fields_dest_fetch']:
            for fetch in self.config_importer['fields_dest_fetch']:
                key = fetch.keys()[0]
                eval('self.field_handler_'+key+'()')
                # #TODO 


    def fields_do_transfer(self):
        """ kopieren aller restlichen felder die nicht anderweitig
            behandelt werden """
        for fn in self.fields_transfer:
            data = self.data_in.data
            if fn in data:
                ### XXX NEU: Check !
                # falls in data_tmp was vorbereitet wurde: 
#                if fn in self.config_importer['fields_auto_tmp']:
                if fn in self.data_tmp.data:
                    val = self.data_tmp.data[fn]
                else:
                    val = data[fn]
                #sval = val.encode('utf8')
                sval = val
                if not sval is None:
                    ### leave here for unicode
                    self.data_store.set_field(fn, sval)
            else:   
                pass
                l.debug( "FIELD TRANSFER not in data_in: %s" %fn)


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
        

    def set_operation_final(self, data_store):
        # entscheide ob insert oder update
        #self.data_store.dump()
        key_existing = self.store.exist_keys( data_store, self.config_importer['fields_unique'] )
        if key_existing:
            # 140217 bug: update auf falsche coid
            self.data_store.set_field(self.store.keyname, key_existing)
            l.debug("data_store %s wird gesetzt auf %s" %(self.store.keyname, data_store.data[self.store.keyname]) )
            
            self.operation = 'update'
        else:
            if self.data_in.data['status'] == u'X':      # #TODO cUSTOM!!
                self.operation = 'none'
            else:
                self.operation = 'insert'
        l.info("operation final: %s " % self.operation)
        

    def operate(self, data_store):
        # uebergebe data_store and store
        #self.store.set_data_store(data_store)

        if self.operation == 'update':
            self.update(data_store)
        elif self.operation == 'insert':
            self.insert(data_store)
        elif self.operation == 'delete':
            self.delete(data_store)
        elif self.operation == 'none':
            l.debug("NO OPERATION needed")
           


    # #TODO so okay? was ist custom delete zb = update mit status=9
    def insert(self, data_store):
        
        #data_store = self.store.data_store
        self.store.query_create_insert(data_store.data, self.config['db']['tablename'])
        self.store.insert(data_store.data)
        
        if data_store.data_subitems:
            
            for subitem in data_store.data_subitems.values():

                # wenn sub mit coid schon existiert: loeschen und neu anlegen
                if self.store.exists(subitem, self.config['db']['subtablename']):
                    self.store.query_create_delete(subitem, self.config['db']['subtablename'] )
                    self.store.delete()
                    
                    self.store.query_create_insert(subitem, self.config['db']['subtablename'] )
                    self.store.insert(subitem)
                

    def update(self, data_store):
        """ updaten des gesamten datenstruktur inkl subtable? """
        l.debug('VOR db-zugriff nochmal ausgabe des key : %s ' %data_store.data[self.store.keyname] )
        l.debug(data_store.dumpl())
        #l.debug('VOR db-zugriff nochmal ausgabe des keyname : %s ' %self.store.keyname )
        self.store.query_create_update(data_store.data, self.config['db']['tablename'])
        self.store.update(data_store.data)   # TODO! sollte success flag zurueckgeben
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

                val = subitem[subtablename]
                res = self.store.query_create_select_by( subtablename, val, subtablename)
                if res:
                    l.debug(" subtable ENTRY FOUND !!")
                    self.store.query_create_delete(key, val, tablename)
                    self.store.delete( )

       


