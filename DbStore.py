import sys
import logging as l


class DbStoreError(Exception):  pass


class DbStore(object):

    def __init__(self):
        """ store init """
        self.errors = []
        self.keyname = None   # oder weg?
        self.dry = False
        self.conn = None


    def config_set(self, config):
        """ only set config dict to class attributes """
        self.config = config
        for ckey in config.keys():
            setattr(self, ckey, config[ckey])


    def connect(self):
        """ implement in sublass 
            take credentials from config
            @return success
            @raise DbStoreException on failure
        """
        return NotImplementedError


    def conn_close(self):
        """ implement in superclass """
        return NotImplementedError


    def is_connected(self):
        # TODO: needed?
        if self.conn: 
            return True
        return False

    def write(self, ds):
        """ implement in subclass """
        pass


    def get_start_id(self):
        """ find maximal existing id value to find offset for new inserts"""
        self.cursor = self.conn.cursor()
        sql = "select max(abs(%s)) from %s" %(self.keyname, self.tablename)
        self.cursor.execute( sql )
        maximal = self.cursor.fetchone()
        amax = maximal[0]
        if not amax:
            amax = 0
        l.debug("MAX ID: %s " % str(amax))
        return int(amax)


    def selectall(self):
        """ Abfrage eines kompletten Datensatzes anhand key, vorbereiteter query """
        self.cursor = self.conn.cursor()
        self.cursor.execute( self.sql )
        res = self.cursor.fetchone()
        if not res:
            return None
        return res[0]
    

    def select_all_ids(self):
        """ liste der key values aller datensaetze """

        self.sql = 'SELECT %s FROM %s' %(self.keyname, self.tablename)        
        

    def query_create_select_all(self, keys, uid):
        """ create SELECT ALL statement dynamicall dependent on keys """
        e = ''
        keys.sort()
        for k in keys:
            e += str(k)
            e += ','
        e = e.rstrip(',')
        self.sql = 'SELECT %s FROM %s WHERE %s=%s' %(e, self.tablename,
                    self.keyname, uid)
        l.debug(self.sql)


    def exists(self, data, tablename):
        """ check if a dataset with the given key already exists """

        self.cursor = self.conn.cursor()
        sql = 'select %s from %s where %s=%s' %(
            self.keyname, tablename,
            self.keyname, data[self.keyname])

        self.cursor.execute( sql )
        res = self.cursor.fetchone()
        if not res:
            return False
        return True

    

    def exist_keys(self, data_store, keys):
        """ do query for keys with current values """
        self.cursor = self.conn.cursor()
        sql = 'select %s from %s where ' %(self.keyname, self.tablename)
        if len(keys) > 1:
            for key in keys:
                sql += "%s='%s' and " %(key, data_store.data[key])
            sql = sql[:-4]

        else:
            sql += "%s='%s'" %(key, data_store.data[key])

        self.cursor.execute(sql)
        res = self.cursor.fetchone()
        l.debug(sql)
        l.info( "exist_keys: %s " % str(res))
        if res:
            return res[0]
        else:
            return False


    def set_data_store(self, data_store):
        """ needed, wanted? """
        self.data_store = data_store
        pass


    def insert(self, data):
        """ speichern in DB """
        l.debug( self.sql )
        l.debug( data )
        cursor = self.conn.cursor()
        try:
            cursor.execute( self.sql, data )
            return True

        except UnicodeEncodeError, e:
            l.error( "DbStore, def insert; unicode error: %s" %str(e))
            #self.missed.append[ data[self.keyname] ]
            l.debug( data[self.keyname])
        #except:
#            sys.exit(1)
            # TODO: return False, caller need to remember missed DS
            # raise DbStoreError # TODO: wie geht das? raisen


    def query_create_insert(self, data, tablename):
        """ create INSERT statement dynamicall dependent on keys """
        e = ''
        v = ''
        keys = data.keys()
        keys.sort()
        for k in keys:
            v += ':'
            v += str(k)
            v += ','
            e += str(k)
            e += ','
        e = e.rstrip(',')
        v = v.rstrip(',')

        self.sql = 'INSERT INTO %s (%s) VALUES (%s)' %(tablename, e, v)
        

    def update(self, data):
        """ speicher in DB """
        cursor = self.conn.cursor()
        l.debug(self.sql)
        try:
            cursor.execute( self.sql, data )
            return True

        except UnicodeEncodeError, e:
            l.error("DbStore, def update; unicode error: %s" % str(e))
            #self.missed.append[ data[self.keyname] ]
            return False
        #except:


    def query_create_update(self, data, tablename):
        """ prepare update query, keys are from source dataset """
        keys_query = data.keys()
        l.debug( "KEYS for query: "+str(keys_query) )
        keys_query.remove(self.keyname)
        keys_query.sort()
        e = ''
        for key in keys_query:
            t = "%s=:%s, " %(key,key)
            e += t
        e= e[:-2]

        self.sql = 'UPDATE %s SET %s WHERE %s=%s' %(
            tablename, e, self.keyname, data[self.keyname])


    def query_create_delete(self, data, tablename):
        """ delete entry in subtable """
        self.sql = "DELETE FROM %s WHERE %s=%s" %(
                 tablename, self.keyname, data[self.keyname] )
                

    def delete(self):
        l.debug( self.sql )
        cursor = self.conn.cursor()
        cursor.execute( self.sql )
        
    def query_create_select_by(self, fn, val, tablename):
        """ create SELECT statement to find dataset by a fields value """
        self.sql = 'SELECT * FROM %s WHERE %s=%s' %( tablename, fn, val)



