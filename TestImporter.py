import sys, os



from BaseImporter import BaseImporter


class TestImporter(BaseImporter):


    def __init__(self):
        self.dir_app = os.path.abspath(os.path.dirname(__file__))
        BaseImporter.__init__(self)
        self.test = '_mc'   # temp


    def run(self):

        BaseImporter.run(self)


    def set_operation(self, data_in):
        """ subclass custom method  
            choose operation dependent on data set """
        # default operation
        self.operation = 'insert_or_update'

	self.data_store.set_action( self.operation )


    def field_handler_autotest(self):
        print "handling field XXX"
	


