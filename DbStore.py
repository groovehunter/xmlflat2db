import sys


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
        # XXX needed?
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
        #print "MAX ID "+str(amax)
        return int(amax)


    def selectall(self):
        """ Abfrage eines kompletten Datensatzes anhand key, vorbereiteter query """
        self.cursor = self.conn.cursor()
        self.cursor.execute( self.sql )
        res = self.cursor.fetchone()
        if not res:
            return None
        return res[0]
        

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
#        print self.sql


    def exists(self, data_store):
        """ check if a dataset with the given key already exists """

        # XXX wenn coid key nicht existiert? in ds??
        self.cursor = self.conn.cursor()
        print data_store.data
        sql = 'select %s from %s where %s=%s' %(
            self.keyname, self.tablename,
            self.keyname, data_store.data[self.keyname])

        self.cursor.execute( sql )
        res = self.cursor.fetchone()
        #print "dataset with coid "+self.keyname+" exists yet? "+str(res)
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
        #print "exist_keys: " + str(res)
        if res:
            return res[0]
        else:
            return False


    def set_data_store(self, data_store):
        """ needed, wanted? """
        self.data_store = data_store
        pass

    def insert(self, data_store):
        """ speicher in DB """
        #data_store = self.data_store
        #print self.sql

        if self.dry:
            return True
        # XXX a new cursor?
        cursor = self.conn.cursor()

        try:
            cursor.execute( self.sql, data_store.data )
            return True

        except UnicodeEncodeError, e:
            print "DbStore, def insert; unicode error", str(e)
            #self.missed.append[ data[self.keyname] ]
            print self.sql
            print data_store.data[self.keyname]
            #data_store.dump()
            #sys.exit(1)
            #return False
        #except:
            data_store.dump()
#            sys.exit(1)
            # XXX return False, caller need to remember missed DS
            # raise DbStoreError # XXX wie geht das? raisen


    def query_create_insert(self, data_store):
        """ create INSERT statement dynamicall dependent on keys """
        e = ''
        v = ''
        keys = data_store.data.keys()
        keys.sort()
        for k in keys:
            v += ':'
            v += str(k)
            v += ','
            e += str(k)
            e += ','
        e = e.rstrip(',')
        v = v.rstrip(',')

        self.sql = 'INSERT INTO %s (%s) VALUES (%s)' %(self.tablename, e, v)


    def update(self, data_store):
        """ speicher in DB """
        if self.dry:
            return True
        # XXX a new cursor?
        cursor = self.conn.cursor()
        #self.data_store.dump()
        #print self.sql
        try:
            cursor.execute( self.sql, data_store.data )
            return True

        except UnicodeEncodeError, e:
            print "DbStore, def update; unicode error", str(e)
            #self.missed.append[ data[self.keyname] ]
            return False
        #except:
        #    print "UP: " +self.sql
        #    sys.exit(1)


    def query_create_update(self, data_store):
        """ prepare update query, keys are from source dataset """
        keys_query = data_store.data.keys()
        #print keys_query
        keys_query.remove(self.keyname)
        keys_query.sort()
        e = ''
        for key in keys_query:
            #t = "%s='%s'," %(key,self.data_store.data[key])
            t = "%s=:%s, " %(key,key)
            e += t
        #e = e.rstrip(',')
        e= e[:-2]

        self.sql = 'UPDATE %s SET %s WHERE %s=%s' %(
            self.tablename, e, self.keyname, data_store.data[self.keyname])

