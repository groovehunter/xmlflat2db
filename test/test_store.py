
from TestBase import TestBase
from TestWorker import TestWorker

from utils import create_test_ds
from DbStore import DbStoreError



# es koennte der MC als attribut hier angeleft werrden




#class TestStore(TestBase):
class TestStore(TestWorker):

    def setUp(self):
        """ MC initialisiert store """
        TestWorker.setUp(self)

        self.mc.set_storage('informix')
        #self.mc.set_storage('sqlite')
        self.mc.init()
        self.mc.load_config()

        self.mc.init_db(self.mc.config['db'])
        suc = self.mc.store.connect()
   

    def tearDown(self):
        """ datenbank-verbindung schliessen """
        self.mc.store.conn_close()
        TestBase.tearDown(self)


    def test_store_init(self):
        """ initalisieren mit config """
        storetest_ok = False

        if hasattr( self.mc.store, 'config'):
            storetest_ok = True
        self.assertTrue(storetest_ok)

    def test_store_connect(self):
        """ storage sollte connected werden koennen """
        if not self.mc.store.is_connected():
            self.assertTrue( self.mc.store.connect() )
        else:
            return True
    '''

    def test_raiseExceptionIfCannotConnect(self):
        creds = ('asd','ewrwg','sfgdfg')
        self.mc.store.config['creds']=creds
        
        self.assertRaises( DbStoreError, self.mc.store.connect )  
    '''


    def test_store_operate(self):
        """ storage sollte arbeiten koennen, read, write """
        storetest_ok = False
        self.mc.store.connect()

        self.mc.fields_calc()
        self.mc.set_fields_auto()
        self.mc.fields_do_transfer()

        self.mc.operate()
        #self.mc.store.query_create_insert()
        #self.mc.store.insert()
        storetest_ok = not self.mc.store.errors
        self.assertTrue(storetest_ok)



#    def test_insert_doublekey_raise_integrityException(self):
#        self.mc.store.connect()
#        ds = create_test_ds()
#        self.mc.store.keys_to_store = ds.data.keys()
#        self.mc.store.data_store = ds.as_dict()
#        self.mc.store.query_create_insert()
#
#        self.assertRaises( DbStoreError, self.mc.store.insert )
#
#    def test_store_prepare_and_insert(self):
#        """ gegeb. dict soll query prepared werden und inserted werden """
#        self.mc.store.connect()
#        ds = create_test_ds()
#        self.mc.store.keys_to_store = ds.data.keys()
#        self.mc.store.data_store = ds.as_dict()
#        self.mc.store.query_create_insert()
#        self.assertTrue( self.mc.store.insert() )


# ds generisch

# ds custom, sind jeweils key lists:
# auto keys
# 

    def test_store_DS_keys_prepared_accord_config(self):
        """ DS fertig, aktion drin, alle felder fuer target DB 
            gecheckt??
        """
        #self.failUnless( 
        #    self.mc.data_store.g ....
        #




    def test_field_auto_set_ok(self):
        """ teste ob das feld im datastore gesetzt wird ueber funktion 
            fields auto -  """
        self.mc.initDataStore()
        self.mc.set_fields_auto()

        if 'fields_auto' in self.mc.config:
            key = self.mc.config['fields_auto'][0]
            self.assertTrue( self.mc.data_store.data[key] )
        else:
            self.assertTrue( True )


    def test_keyexist(self):
        data_store = self.mc.data_store
        self.mc.fields_calc()
        self.mc.fields_do_transfer()
        self.mc.fields_set_not_null()

        exist = self.mc.store.exist_keys(data_store, ['kundenid', 'laborid'])
        self.assertIsNotNone( exist )

    def test_createquery:
        pass



# db backend test suitte

# anlegen db



"""
field:
* typ
* must/need ?
* handler?
"""
