#!/bin/bash

export INFORMIXDIR=/opt/IBM/informix
export PATH=$PATH:$INFORMIXDIR/bin
export LD_LIBRARY_PATH="/opt/IBM/informix/lib:/opt/IBM/informix/lib/esql"
export INFORMIXSERVER="on_ausoftdb3_tcp"

cd '/home/konnertz/git-test/xmlflat2db'
su -c ./start_mc.py konnertz



