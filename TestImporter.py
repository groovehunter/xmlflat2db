import sys, os



from BaseImporter import BaseImporter


class TestImporter(BaseImporter):


    def __init__(self):
        self.dir_app = os.path.abspath(os.path.dirname(__file__))
        BaseImporter.__init__(self)
        self.app_id = 'test'
        self.test = '_test'   # temp


    def run(self):

        BaseImporter.run(self)


    def set_operation(self, data_in):
        """ subclass custom method  
            choose operation dependent on data set """
        # default operation
        self.operation = 'insert_or_update'

        self.data_store.set_action( self.operation )



    def field_handler_kor_time(self):
        """ function name wird aufgerufen hier um feld in DS zu fuellen """
#        self.data_store.set_field('kor_time', now())

    def field_handler_kor_name(self):
        val = 'KOFL' 
        self.data_store.set_field('kor_name', val)
       

    def field_handler_erf_name(self):
        if self.operation == 'insert':
            self.data_store.set_field('erf_name', 'KOFL')

    def field_handler_erf_time(self):
        if self.operation == 'insert':
            self.data_store.set_field('erf_name', now())



