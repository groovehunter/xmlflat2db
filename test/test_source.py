
from TestBase import TestBase
from TestWorker import TestWorker

from utils import create_test_ds
from etree import ParseError


#class TestSource(TestWorker):
class TestSource(TestBase):

    def setUp(self):
        """ MC initialisiert store """
        #TestWorker.setUp(self)
        TestBase.setUp(self)


    def tearDown(self):
        """ datenbank-verbindung schliessen """
        TestBase.tearDown(self)


    def test_dumpfirstTrue(self):
        """ if output is fine """
        self.mc.source_load_first()
        self.mc.parse()
        self.mc.source_cur_dump()        

        self.assertTrue(suc)


    def raiseParseError(self):
        self.mc.source_load_first()
        self.assertRaise( ParseError, self.mc.parse )


        
