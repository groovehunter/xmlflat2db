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


    def exists(self):
        """ check if a dataset with the given key already exists """

        self.cursor = self.conn.cursor()
        sql = 'select %s from %s where %s=%s' %(
            self.keyname, self.tablename,
            self.keyname, self.data[self.keyname])

        self.cursor.execute( sql )
        res = self.cursor.fetchone()
        print self.keyname+" exists yet? "+str(res)
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
            sql += "%s='%s'" %(key, data[key])

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

    def insert(self):
        """ speicher in DB """
        data_store = self.data_store

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
            data_store.dump()
            sys.exit(1)
            return False
        except:
            print self.sql
            data_store.dump()
            sys.exit(1)
            # XXX return False, caller need to remember missed DS
            # raise DbStoreError # XXX wie geht das? raisen


    def query_create_insert(self):
        """ create INSERT statement dynamicall dependent on keys """
        e = ''
        v = ''
        keys = self.data_store.data.keys()
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

    def update(self):
        """ speicher in DB """
        if self.dry:
            return True
        # XXX a new cursor?
        cursor = self.conn.cursor()

        try:
            cursor.execute( self.sql ) #, self.data_store.data )
            return True

        except UnicodeEncodeError, e:
            print "DbStore, def update; unicode error", str(e)
            #self.missed.append[ data[self.keyname] ]
            return False
        except:
            print self.sql
            data_store.dump()
            sys.exit(1)


    def query_create_update(self):
        """ prepare update query, keys are from source dataset """
        keys_query = self.data_store.data.keys()
        keys_query.remove(self.keyname)
        keys_query.sort()
        e = ''
        for key in keys_query:
            t = "%s='%s'," %(key,self.data_store.data[key])
            e += t
        e = e.rstrip(',')
        self.sql = 'UPDATE %s SET %s WHERE %s=%s' %(
            self.tablename, e, self.keyname, self.data_store.data[self.keyname])

