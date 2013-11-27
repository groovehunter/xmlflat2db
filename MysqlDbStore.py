
from datetime import datetime
from DbStore import DbStore
import mysql.connector



class MysqlDbStore(DbStore):
    """ Informix specific storage handler """

    def __init__(self):
	""" store init """
	DbStore.__init__(self)


    def connect(self):
        
	self.conn = mysql.connector.connect(
            user = self.config['dbuser'],
            password = self.config['dbpass'],
            host='127.0.0.1',
            database = self.config['dbname'])


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

#	except IntegrityError, e:


