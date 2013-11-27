
from datetime import datetime
from DbStore import DbStore
from sqlite3 import connect



class SqliteDbStore(DbStore):
    """ sqlite specific storage handler """

    def __init__(self):
        """ store init """
        DbStore.__init__(self)


    def connect(self):
        """ just connect + set cursor
            @return success """
        try:
            self.conn = connect(self.dbname)
            self.cursor = self.conn.cursor()
            return True
        except:
            return False



    def write(self, ds):
        """ dummy ?!! """
        return True
    


    def delete(self):
        sql = "update %s set %s=%s where %s=%s" %(
            self.tablename, 
            key, self.data[key], 
            self.keyname, data[self.keyname])
        print sql
        self.cursor.execute(sql)



    def error_handler(self):
        print "ERROR: on storage"
        pass

#       except IntegrityError, e:


