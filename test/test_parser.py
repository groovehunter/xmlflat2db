# -*- coding: utf-8 -*-

from TestBase import TestBase

class TestParser(TestBase):

    def setUp(self):
        TestBase.setUp(self)
        self.mc.source_load_first()


    def testAlleDSgeladen(self):
        self.mc.scan_sources()
        self.mc.source_load_first()


    """
    def testDSschreibenOK(self):
        success = self.mc.work()
        self.failUnless(success)
    """

    def testSuchTerm(self):
        """ aus den suchfeldern ein string machen """ 
        from stringutils import suchString
        orig= u'schräg'
        ok  = u'SCHRAEG'
        res = suchString(orig)
        self.assertEqual( ok, res ) 

    '''
    '''
    def testOperationSet(self):
        """ op gesetzt gemaess input data """
        self.mc.src_cur = self.mc.src_main_dir+'BIOV/cust_BIOV.20131121120125.ok'
        self.mc.keep_in_memory = True
        self.mc.work()

        first = self.mc.data_array.items()[0]
        self.mc.initDataStore()
        self.mc.set_operation( first[1] )

        op_list = ['insert_or_update','update']
        self.assertIn( self.mc.operation, op_list )


    def testParsenKurzesFile(self):
        """ parsen ok """
        self.src_cur = self.mc.src_main_dir+'BIOV/cust_BIOV.20131121120125.ok'
        self.mc.keep_in_memory = True
        self.mc.work()

        self.assertTrue( self.mc.data_array )   # not empty



    # XXX die work bzw iter schleife ueberarbeiten
    '''
    def testIterContextRaiseXMLSyntaxError(self):
        from lxml.etree import XMLSyntaxError, iterparse
        file_fail = 'SYSC/custSYSC.04999999'
        self.mc.src_cur = self.mc.src_main_dir + file_fail
        self.mc.keep_in_memory = True
        #xmlfile = open(self.mc.src_cur)
        #with xmlfile as xml_doc:
        #    context = iterparse(xml_doc, events=("start", "end"))

        self.assertRaises(XMLSyntaxError, self.mc.work )
    '''





