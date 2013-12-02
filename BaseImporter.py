#from config_main import *
import os
import subprocess
from DummyStore import DummyStore
from SourceScan import SourceScan
from Datensatz import Datensatz, DataStore
from lxml import etree
from lxml.etree import iterparse, XMLSyntaxError, ParseError
import yaml
import datetime


class MissingDataException(Exception):  pass
class ImporterError(Exception):         pass


class BaseImporter(SourceScan):

    def __init__(self):
        self.source = None
        self.src_failed = []

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
            config_fpath = self.dir_app +'/config'+self.test+'/'+fn 
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
        
        tree = etree.parse(self.src_cur)
        self.encodig = tree.docinfo.encoding
        xmlfile = open(self.src_cur)
        with xmlfile as xml_doc:
            context = iterparse(xml_doc, events=("start", "end"))

            #self.iter_context(context)

            try:
                for event, elem in context:
                    if event == "start" and elem.tag == self.DATASET_TAG:
                        for child in elem:
                            if child.text is None:
                                val = ''
                            else:
                                val = child.text
                            data[child.tag] =  val

                    elif event == "end" and elem.tag == self.DATASET_TAG:
                        self.num_ds_given += 1
                        self.id_tmp += 1
                        data_in = Datensatz()
                        data_in.setze_per_dict(data)
                        if self.keep_in_memory:
                            self.data_array[ data_in.get_uid() ] = data_in
                        else:
                            ### HANDLE CALLS start here
                            #self.data_in = data_in
                            self.work_ds(data_in)

            except XMLSyntaxError:
                self.src_failed.append(self.src_cur)
                #print "DATASET FAILED "+str(context)+str(dir(context.root))
                return False
        return True


    def iter_context(self, context):
        """ iter over the flat xml structure """
        from lxml.etree import XMLSyntaxError
        data = {}
        keys_collect = []
        self.id_tmp = 0
        self.num_ds_written = 0
        self.num_ds_given = 0
        try:
            for event, elem in context:
                if event == "start" and elem.tag == self.DATASET_TAG:
                    for child in elem:
                        if child.text is None:
                            val = ''
                        else:
                            val = child.text
                        data[child.tag] =  val

                        # XXX hier weg, hier wird nicht zensiert! :)
                        if not child.tag in self.keys_src_ignore_always:
                            keys_collect.append(child.tag)

                elif event == "end" and elem.tag == self.DATASET_TAG:
                    self.num_ds_given += 1
                    self.id_tmp += 1

# XXX hier erstmal temp halten
                    data_in = Datensatz()
                    data_in.setze_per_dict(data)
                    if self.keep_in_memory:
                        self.data_array[ data_in.get_uid() ] = data_in
                    else:
                        ### HANDLE CALLS start here
                        #self.data_in = data_in
                        self.work_ds(data_in)

                return True
            
        except XMLSyntaxError:
            self.src_failed.append(self.src_cur)
            #print "DATASET FAILED "+str(context)+str(dir(context.root))
            return False
        


    def work_ds(self, data_in):
        """ steuert einen durchgang komplett """
        #print "WORK ON " + data_in.data['name']
        # erhaelt flachen datensatz
        # initialisiert data_store und mehr
        self.initDataStore()
        self.operation = None

        # setzt aktions-feld im data_store
        self.set_operation(data_in)


        ### === fields 
        # werte weitere wichtige felder aus
        # werfe unnuetze felder weg
        # sammle felder fuer operationen ein, gebe ops frei

        # berechne fehlende Felder des DS
        self.fields_calc()
        self.data_in = data_in
        # a) auto felder
        # e) aktuelle autoinc id 
        self.set_fields_auto()
        # b) aus quell-daten
        # c) muss-felder = none null felder 1:1
        # d) vielleicht-felder aus source , jetzt?

        # felder mit typ-spezieller sonderbehandlung
        self.fields_typed_date()

        # f) dest-felder die aus quellfeldern berechnet werden
        self.fields2calc()
        # g) felder fuer subtabellen, inkl operation ausfuehren (?)

        # alle weiteren felder
        self.fields_do_transfer()

        # setze operation attributes XXX nicht schlau
        self.set_operation_final(self.data_store)
        #self.fields_to_subtables()


        ### === storage 
        # setzt op in data_store auch?

        # operation im DB backend auf den weg bringen
        self.operate( self.data_store )
        # pruefe ob vollstaendige verarbeitung
        self.num_ds_written += 1

        #  RETURN zu naechster runde


    def initDataStore(self):
        self.data_store = DataStore()


    def report_manual_todo(self):
        while self.src_failed:
            print "FAILED file : ", self.src_failed.pop()

        return True


    def set_fields_auto(self):
        """ setze auto-felder """
        fields_auto = self.config_importer['fields_auto']
        if fields_auto:
            for key in fields_auto:
                eval('self.field_handler_'+key+'()')

        # auto inc id im data store setzen
        id_cur = self.id_max + self.id_tmp
        self.data_store.set_field( self.config_importer['field_autoinc'], 
            id_cur)


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
                #sval = calc['mapfunc']( field_map.keys()[0] )
                exstr = "sval=self."+calc['mapfunc']+"(self.data_in.data[pair[1]])"
                exec exstr
                #print "%s | %s" %(self.data_in.data[pair[1]], sval)
                self.data_store.set_field( pair[0], sval )


    def fields_typed_date(self):
        """ transferiere felder des typs datum """
        for fn in self.config_importer['fields_typed_date']:
            data = self.data_in.data
            if fn in data:
                sval = self.prep_date(data[fn])
                self.data_store.set_field(fn, sval)
            else:
                pass  # log XXX


    def prep_date(self, v):
        """ if data in US format, convert to custom """
        # XXX move date format setting in custom importer 
        #print v
        try:
            d = datetime.strptime(v, self.config_importer['format_date_src'])
            return d.strftime(self.config_importer['format_date_dest'])

        except:
            #d = datetime.strptime(v, '%Y-%m-%dT%I:%M:%S+02:00')
            try:
                dform = '%Y-%m-%dT%H:%M:%S+02:00'
                d = datetime.strptime(v, dform)
            except:
                return v
            return d.strftime(self.config_importer['format_date_dest'])




    def fields_do_transfer(self):
        """ kopieren aller restlichen felder die nicht anderweitig
            behandelt werden """
        for fn in self.fields_transfer:
            data = self.data_in.data
            if fn in data:
                val = data[fn]
                sval = val.encode('utf8')
                self.data_store.set_field(fn, sval)
            else:   
                pass
                # XXX store in cloud :)


    def fields_to_subtables(self):
        """ create data stores to write to subtables with 
            relation to main table """
        sub_store = {}
        
        for sub in self.config_importer['fields_to_subtables']:
            table_name = sub['table']

            sub_store[table_name] = DataStore()

            # loopen aller felder der sub config
            for field_name in sub['fields']:
                if field_name in self.data_in.data:
                    val = self.data_in.data[field_name]
                    sub_store[table_name].set_field( field_name, val )

            # forein key set always
            # XXX rechte coid herausfinden, entscheidung auf operation!
            keyname = self.config_importer['keyname']
            val_pk = self.data_store.data[keyname]
            sub_store[table_name].set_field( keyname, val_pk )
            sub_store[table_name].dump()

            self.store.tablename = table_name
            #self.operate( sub_store[table_name] )
            self.store.set_data_store(sub_store[table_name])

            #if self.data_store.action == 'update':
            if self.operation == 'update':
                self.update()
            #if self.data_store.action == 'insert':
            if self.operation == 'insert':
                self.insert()
            print "sub store op done: "+self.operation
        # re-set store 
        self.store.tablename = self.config['db']['tablename']



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
        self.fields_transfer = fields_wanted
        #print str(fields_tmp)


    def set_operation_final(self, data_store):
        # entscheide ob insert oder update
        self.data_store.dump()
        if self.store.exist_keys( data_store, 
                                self.config_importer['fields_unique'] ):
            self.operation = 'update'
        else:
            self.operation = 'insert'
        

    def operate(self, data_store):
        # uebergebe data_store and store
        self.store.set_data_store(data_store)

        if self.operation == 'update':
            self.update()
        elif self.operation == 'insert':
            self.insert()
        # 


    # XXX so okay? was ist custom delete zb = update mit status=9
    def insert(self):
        self.store.query_create_insert()
        self.store.insert()

    def update(self):
        self.store.query_create_update()
        self.store.update()

       


