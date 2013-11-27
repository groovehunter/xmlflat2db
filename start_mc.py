#!/usr/bin/python

import sys, os


dir_app = os.path.dirname(__file__)

sys.path.append( os.path.abspath( 
    os.path.join( dir_app, '../' ) ) )

from CustomImporter import CustomImporter




if __name__ == '__main__':
    mc = CustomImporter()
    mc.dir_app = dir_app
    mc.run()



