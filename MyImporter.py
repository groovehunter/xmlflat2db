from BaseImporter import BaseImporter


class MyImporter(BaseImporter):


    def __init__(self):
        BaseImporter.__init__(self)



    def run(self):

        BaseImporter.run(self)


    def set_operation(self, data_in):
        """ subclass custom method  
            choose operation dependent on data set """
        # default operation
        self.operation = 'insert_or_update'

        # ERSTER SCHRITT: status feld im datensatz
        status = data_in.status
        if status == 'A':
            self.operation = 'insert_or_update'

        elif status == 'H':
            self.operation = 'insert_or_update'

        elif status == 'X':
            #self.operation = 'delete'
            self.operation = 'update'
            self.operation_args = ('status', 9)

        self.data_store.set_action( self.operation )

        '''
        if self.operation == 'insert_or_update':
            # check unique keys here
            # key combos # laborid & kundenid
            res = self.store.exist_keys(self.data_in.data, ('kundenid','laborid') )
            if not res:
                self.operation = 'insert'
            else:
                self.data_in.data[self.keyname] = res
                self.operation = 'update'
        '''
        

    def field_handler_kor_time(self):
        """ function name wird erzeugt und aufgerufen hier um feld in DS 
            zu fuellen """
        # dummy, XXX curtime
        val = 'bla' 
        self.data_store.set_field('kor_time', val)
        

    def field_handler_kor_name(self):
        # dummy, XXX curtime
        val = 'bla' 
        self.data_store.set_field('kor_name', val)






