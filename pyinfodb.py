import os
import informixdb

# NEED informix libs c esql
# export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$INFORMIXDIR/lib:$INFORMIXDIR/lib/esql


def errorhandler(connection, cursor, errorclass, errorvalue):
    print "errorclass: "+str(errorclass)
    print "errorvalue: "+str(errorvalue)


    #def dbconn(dbname, dbuser, dbpass):
def dbconn(creds):
    (dbname, dbuser, dbpass) = creds
    conn = informixdb.connect(dbname, user=dbuser, password=dbpass)
    conn.autocommit = True
    return conn


def select_all(conn, table):
    cursor = conn.cursor()
    sql = "SELECT * FROM"+table
    cursor.execute(sql)
    all = cursor.fetchall()
    for record in all:
        print record, "\n"

def delete_all(conn, table):
    cursor = conn.cursor()
    sql = "DELETE FROM "+table
    cursor.execute(sql)

def serverinfo(conn):
    print "python INFORMIXDB version "+ informixdb.version
    print "driver name : " +conn.driver_name
    print "driver version : "+conn.driver_version
