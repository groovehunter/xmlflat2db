#!/usr/bin/env python

import sys, os
import argparse
import logging as l

dir_app = os.path.dirname(__file__)
abspath_app  = os.path.abspath(dir_app)
lvlup = os.path.join( abspath_app, '../' )
print lvlup
sys.path.append(lvlup) 
from CustomImporter import CustomImporter
 

def start():
    parser = argparse.ArgumentParser(description='Start medicarImporter.')
    parser.add_argument('-t', '--test', nargs='?', const='_test', default='',
                       help='if the importer run in test environment (test DB)')

    group = parser.add_mutually_exclusive_group()
    group.add_argument('-l', action='append', nargs='*', dest='subdirs',
                       help='Die zu verarbeitenden Unterverzeichnisse')

    group.add_argument('-d', action='append', nargs='*', dest='files',
                       help='Die zu verarbeitenden Dateien')

    args = parser.parse_args()

    mc = CustomImporter()
    mc.test = args.test
    if args.files:
        mc.src_sub_dir  = os.path.abspath(os.curdir).split('/')[-1]
        mc.arg_files_wanted      = args.files[0]

    mc.arg_subdirs_wanted   = args.subdirs
    mc.dir_app = abspath_app

    mc.run()




if __name__ == '__main__':
    #logfile = '/var/syscomp/log/medicarImport/start.log'
    #l.basicConfig(filename= logfile, level=l.DEBUG)
    start()


