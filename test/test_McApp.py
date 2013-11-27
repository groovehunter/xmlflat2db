from TestBase import TestBase


class TestMcApp(TestBase):
    """ sollte grundlegende ablaeufe der app testen 
    """

    def setUp(self):
        """ instantiieren; sources laden; db init. """
        TestBase.setUp(self)

        self.mc.scan_source_dirs_all()
        self.mc.scan_sources()
        self.mc.source_load_first()


    def test_source_exist(self):
        """ sollte source attribute haben , bevor...?"""
        self.assertTrue( hasattr(self.mc, 'source') )


    def test_src_dir(self):
        """ scan des source dir sollte bei ein oder mehr files starten """

        self.assertIsNot( len(self.mc.ls), 0)


    def test_if_src_loaded_if_valid_file(self):
        """ entscheide auf xml und pruefe file auf okay oder nicht """
        self.assertIn( self.mc.source_cur_checkvalid(), [True,False] )


#    def test_ValueError_if_wrong_src(self):
#       self.assertRaises(ValueError, self.mc.source_cur_checkvalid)

    '''
    def test_DSkorrektVerarbeitet(self):
        ds = create_test_ds()
        
        self.mc.store.write(ds)
        uid = ds.get_uid()
        ds_pruef = self.mc.store.read(uid)
        self.failUnless( ds_pruef, ds )
    '''

    def test_xmldateien_mit_problemen(self):
        """ xml files mit problemen in der verarbeitung sollen sich 
            gemerkt werden ,evtl verschieben in unterordner 'problem' 
            o.ae. """
        self.assertTrue( self.mc.report_manual_todo() )

   
    def test_TotaleNrDSgeschriebenBzwVerarbeitet(self):
        self.mc.work()
        self.assertEqual( self.mc.num_ds_written, self.mc.num_ds_given )
       
    ''' FINAL  
    def test_alle_src_files_werden_verarbeitet(self):
        """ in einem Lauf muessen alle files verarbeitet 
            dh. in die archive verschoben werden """
        self.failUnless( self.mc.no_files_left() )
    '''


    def test_load_config(self):
        self.mc.load_config()

        self.assertEqual( self.mc.config['test'], 'test' )


