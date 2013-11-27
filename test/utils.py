
import os, sys
sys.path.insert(0,os.path.abspath(__file__+"/../.."))


from DummyStore import DummyStore
from Datensatz import Datensatz
"""
"""
import unittest
import os
from glob import glob


def create_test_ds():
    ds = Datensatz()
    
    testdict = {
'coid'	:   'bla001',
'name'	:'testname',
'status':'A',
'plz'	:'86150',
'laborid':'LAB28',
'kundenid':'blubb',
}
    ds.setze_per_dict( testdict )
    ds.set_attr_random('coid')
    ds.set_attr_random('kundenid')
    return ds




class TestBasics(unittest.TestCase):
    """ basic stuff """

    def setUp(self):
        db = DummyStore()
        fn= db.get_fn()
        for item in glob(fn+'*'):
            os.remove(item)

    def test(self):
        """ dummy false test """
        self.assertFalse( False )




