


import unittest
import sys, os

sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)+"/.."))

#print sys.path
from TestImporter import TestImporter



class TestBase(unittest.TestCase):

    """ common setup and teardown for importer tests """

    def setUp(self):
        self.mc = TestImporter()
        self.mc.test = '_test'
        self.mc.init()
        self.mc.load_config()


    def tearDown(self):
        if hasattr( self.mc, 'store' ):
            if hasattr( self.mc.store, 'conn' ):
                # close DB connection
                self.mc.store.conn_close()



