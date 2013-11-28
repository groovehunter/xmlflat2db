
from TestBase import TestBase

testfile = 'BIOV/cust_BIOV.20131112173319.ok'

class TestWorker(TestBase):
    def setUp(self):
        TestBase.setUp(self)
        self.mc.src_cur = self.mc.src_main_dir+testfile
        self.mc.keep_in_memory = True
        self.mc.work()

        first = self.mc.data_array.items()[0]
        #print str(first)
        self.mc.data_in = first[1]

        ### ab hier work_ds prozedur =   self.mc.work_ds(first)

        self.mc.initDataStore()
        self.mc.set_operation( first[1] )

