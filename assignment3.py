#!/usr/bin/python2.7
#
# Interface for the assignement
#

import psycopg2
import math

NUM_OF_PARTITIONS = 0

def getOpenConnection(user='postgres', password='1234', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadRatings(ratingstablename, ratingsfilepath, openconnection):
    curs = openconnection.cursor()

    ratingsfile = open(ratingsfilepath,'r')

    createTableAndLoadData(curs, ratingstablename, ratingsfile)


def createTableAndLoadData(curs, ratingstablename, ratingsfile):
    curs.execute("DROP TABLE IF EXISTS " + ratingstablename)

    curs.execute("CREATE TABLE " + ratingstablename + "(id SERIAL PRIMARY KEY, UserID INTEGER NOT NULL, tmp1 VARCHAR(255), MovieID INTEGER NOT NULL, tmp2 VARCHAR(255), Rating FLOAT, tmp3 VARCHAR(255), Timestamp INTEGER)")

    curs.copy_from(ratingsfile,ratingstablename, sep = ":", columns = ("UserID", "tmp1", "MovieID", "tmp2", "Rating", "tmp3", "Timestamp"))

    curs.execute("ALTER TABLE " + ratingstablename + " DROP COLUMN tmp1")

    curs.execute("ALTER TABLE " + ratingstablename + " DROP COLUMN tmp2")

    curs.execute("ALTER TABLE " + ratingstablename + " DROP COLUMN tmp3")

    curs.execute("ALTER TABLE " + ratingstablename + " DROP COLUMN Timestamp")

    curs.close()


def rangePartition(ratingstablename, numberofpartitions, openconnection):
    curs = openconnection.cursor()
    width = float(5/numberofpartitions)
    curs.execute("DROP TABLE IF EXISTS tableforrange")
    curs.execute("CREATE TABLE IF NOT EXISTS tableforrange (N INTEGER);")
    curs.execute("INSERT INTO tableforrange (N) VALUES (%s)",[numberofpartitions])

    pno = 0

    trial =0


    for trial in range(0,5):
        if trial == 0:
            curs.execute("DROP TABLE IF EXISTS range_part" + str(pno))
            curs.execute("CREATE TABLE range_part" + str(pno) + " AS SELECT * FROM " + ratingstablename  + " WHERE Rating >=" + str(trial) + " AND RATING <=" + str(trial+width) + ";")
            pno = pno + 1
            trial =trial + width
        else:
            dropline = """DROP TABLE IF EXISTS range_part""" + str(pno)
            curs.execute(dropline)

            curs.execute("CREATE TABLE range_part" + str(pno) + " AS SELECT * FROM " + ratingstablename  + " WHERE Rating >" + str(trial) + " AND RATING <=" + str(trial+width) + ";")

            pno = pno + 1

            trial =trial + width


    curs.close();

def roundRobinPartition(ratingstablename, numberofpartitions, openconnection):
   curs = openconnection.cursor()

   for pno in range(0, numberofpartitions):
       createline = """CREATE TABLE """ + """rrobin_part""" + str(pno) + """ (Userid INTEGER, Movieid INTEGER, Rating FLOAT);"""
       curs.execute(createline)
       curs.execute("INSERT INTO " + "rrobin_part" + str(pno) + " (Userid, Movieid, Rating) SELECT Userid, Movieid, Rating FROM (SELECT Userid, Movieid, Rating, ROW_NUMBER() over() AS rno FROM " + ratingstablename + ") AS short WHERE mod(short.rno-1, 5) = " + str(pno) + ";")
   openconnection.commit()


def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    curs = openconnection.cursor()
    curs.execute("INSERT INTO " + ratingstablename + "(Userid, Movieid, Rating) VALUES (" + str(userid) + "," + str(itemid) + "," + str(rating) + ");")
    selectline = """SELECT COUNT(*) FROM """ + ratingstablename + """;"""
    curs.execute(selectline);
    numberofrows = (curs.fetchall())[0][0]
    numberofpartitions = partitioncount(openconnection)
    index = int(math.fmod((numberofrows-1),numberofpartitions))
    curs.execute("INSERT INTO " + "rrobin_part" + str(index) + "(Userid, Movieid, Rating) VALUES (" + str(userid) + "," + str(itemid) + "," + str(rating) + ");")
    curs.close()
    con = openconnection
    con.commit()


def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    curs = openconnection.cursor()
    selectline = """SELECT N FROM tableforrange"""
    curs.execute(selectline)
    alln = curs.fetchall()
    numberofpartitions = alln[-1][0]
    width = float(5/ numberofpartitions)
    lower = 0
    pno = 0
    higher = width

    if not (rating >= lower and rating <= higher):
        lower=lower+width
        higher=higher+width
        pno = pno + 1
        while lower<5:
            if rating > lower and rating <= higher:
                break
            else:
                lower=lower+width
                higher=higher+width
                pno = pno + 1
    curs.execute("INSERT INTO range_part"+str(pno)+" (UserID,MovieID,Rating) VALUES (%s, %s, %s)",(userid, itemid, rating))
    curs.execute("INSERT INTO "+ratingstablename+" (UserID,MovieID,Rating) VALUES (%s, %s, %s)",(userid, itemid, rating))
    curs.close()


def partitioncount(openconnection):
    curs = openconnection.cursor()
    curs.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_name LIKE 'rrobin%';")
    count = curs.fetchone()[0]
    curs.close()

    return count

def createDB(dbname='dds_assignment'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print 'A database named {0} already exists'.format(dbname)

    # Clean up
    cur.close()
    con.close()

def deletepartitionsandexit(openconnection):
    cur = openconnection.cursor()
    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
    l = []
    for row in cur:
        l.append(row[0])
    for tablename in l:
        cur.execute("drop table if exists {0} CASCADE".format(tablename))

    cur.close()

def deleteTables(ratingstablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if ratingstablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (ratingstablename))
        openconnection.commit()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
    except IOError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
    finally:
        if cursor:
            cursor.close()
