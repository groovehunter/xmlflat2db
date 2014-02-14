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
    mc.test = ''
    mc.dir_app = abspath_app

    args = sys.argv
    if len(args) > 1:       # argumente vorhanden
        print "argument: "+ args[1]
        if args[1].find('/'):   # subdir/file
            #subdir, f = args.split('/')
            mc.arg_file_wanted = args[1]
        else:                   # nur subdir
            mc.arg_subdir_wanted = args[1]

    mc.run()



