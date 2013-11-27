from DbStore import DbStore


class DummyStore(DbStore):

    def __init__(self):
	self.data = {}
	self.fn = 'dummy_store_'

    def get_fn(self):
	return self.fn

    def write(self, ds):
	f = file(self.fn + ds.get_uid(), 'w')
	
	f.write( ds.dump() )
	#self.data.pickle
	f.close()


    def read(self, uid):
	f = file(self.fn + uid, 'r')
	lines = f.readlines()
	for line in lines:
	    d1,d2 = line.split('|')
	    self.data[d1] = d2

	return self.data

