#!/usr/bin/env python

import sys, os


dir_app = os.path.dirname(__file__)
abspath_app  = os.path.abspath(dir_app)

lvlup = os.path.join( abspath_app, '../' )
print lvlup
sys.path.append(lvlup) 
 

from CustomImporter import CustomImporter




if __name__ == '__main__':
    mc = CustomImporter()
    mc.test = '_test'
    mc.dir_app = abspath_app
    mc.run()



