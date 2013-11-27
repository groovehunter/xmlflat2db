
from pyinfodb import dbconn
from _informixdb import IntegrityError, InterfaceError, DatabaseError
from _informixdb import Date
import informixdb
from datetime import datetime
from DbStore import DbStore, DbStoreError


class InformixDbStore(DbStore):
    """ Informix specific storage handler """

    def __init__(self):
        """ store init """
        DbStore.__init__(self)
        

    def connect(self):
        dbname = self.config['dbname']
        dbuser = self.config['dbuser']
        dbpass = self.config['dbpass']

        #print "creds" + str( self.config )
        conn = informixdb.connect(dbname, user=dbuser, password=dbpass)
        conn.autocommit = True
        self.conn = conn
        '''
        try:
            conn = informixdb.connect(dbname, user=dbuser, password=dbpass)
            conn.autocommit = True
            self.conn = conn
            #self.conn = dbconn(self.config['creds'])
        except DatabaseError:
            raise DbStoreError
        '''

    def conn_close(self):
        """ gracefully close connection """
        try:
            if self.conn:
                self.conn.close()
        except InterfaceError:
            pass
        return True
        

    def delete(self):
        """ setze db-status auf 9 fuer datensatz """
        # XXX syscomp custom, move to custom importer
        sql = "update %s set %s=%s where %s=%s" %(
            self.tablename, 
            key, self.data[key], 
            self.keyname, data[self.keyname])
        print sql
        self.cursor.execute(sql)


#       except IntegrityError, e:
    def errorhandler(connection, cursor, errorclass, errorvalue):
        print "errorclass: "+str(errorclass)
        print "errorvalue: "+str(errorvalue)



    def select_all(self, table):
        cursor = self.conn.cursor()
        sql = "SELECT * FROM"+table
        cursor.execute(sql)
        all = cursor.fetchall()
        for record in all:
            print record, "\n"


    def delete_all(self, table):
        cursor = self.conn.cursor()
        sql = "DELETE FROM "+table
        cursor.execute(sql)


    def serverinfo(self):
        print "python INFORMIXDB version "+ informixdb.version
        print "driver name : " +self.conn.driver_name
        print "driver version : "+self.conn.driver_version


