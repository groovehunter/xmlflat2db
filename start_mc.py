#!/usr/bin/python

import sys, os
print sys.path


dir_app = os.path.dirname(__file__)

sys.path.append( os.path.abspath( 
    os.path.join( dir_app, '../' ) ) )

from MedicarImporter import MedicarImporter




if __name__ == '__main__':
    mc = MedicarImporter()
    mc.dir_app = dir_app
    mc.run()



