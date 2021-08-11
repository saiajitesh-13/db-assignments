#!/usr/bin/python2.7
#
# Assignment3 Interface
#

import psycopg2
import os
import sys
from threading import Thread

# Donot close the connection inside this file i.e. do not perform openconnection.close()


def ParallelSort (InputTable, SortingColumnName, OutputTable, openconnection):
    
    curs = openconnection.cursor()
    
    curs.execute("SELECT MAX("+str(SortingColumnName)+") ,  MIN("+str(SortingColumnName)+") FROM "+str(InputTable))

    max1,min1 = curs.fetchone()
  
    parts = float(max1 - min1)/5.0

    curs.execute("DROP TABLE IF EXISTS "+str(OutputTable))

    curs.execute("CREATE TABLE IF NOT EXISTS "+str(OutputTable)+" AS TABLE "+str(InputTable)+" WITH NO DATA")

    threads_list = ["thread1","thread2","thread3","thread4","thread5"]

    threads = []

    i=0

    while i < 5:

        threads.append(Thread(target = threads_sort, args = (threads_list[i], openconnection, InputTable, SortingColumnName, min1+parts, min1)))

        threads[i].start()

        min1 += parts

        i = i + 1

    for thread in threads:

        thread.join()

    for tname in threads_list:

        curs.execute("INSERT INTO "+OutputTable+" SELECT * FROM "+tname)

    openconnection.commit()  


def threads_sort (thread_name, openconnection, InputTable, SortingColumnName, max, min):

    curs = openconnection.cursor()

    curs.execute("DROP TABLE IF EXISTS "+thread_name)

    if not thread_name=="thread1":

        curs.execute("CREATE TABLE "+thread_name+" AS SELECT * FROM "+InputTable+" WHERE "+SortingColumnName+" > "+str(min)+" AND "+SortingColumnName+" <= "+str(max)+" ORDER BY "+ SortingColumnName)
        
    else:

        curs.execute("CREATE TABLE "+thread_name+" AS SELECT * FROM "+InputTable+" WHERE "+SortingColumnName+" >= "+str(min)+" AND "+SortingColumnName+" <= "+str(max)+" ORDER BY "+ SortingColumnName)
         



def ParallelJoin (InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):
    
    curs = openconnection.cursor()
    	
    curs.execute("SELECT MAX("+str(Table1JoinColumn)+") ,  MIN("+str(Table1JoinColumn)+") FROM "+str(InputTable1))

    max1,min1 = curs.fetchone()

    curs.execute("SELECT MAX("+str(Table2JoinColumn)+") ,  MIN("+str(Table2JoinColumn)+") FROM "+str(InputTable2))

    max2,min2 = curs.fetchone()
  
    higher=max(max1,max2)

    lower=min(min1,min2)

    parts = float(higher - lower)/5.0

    curs.execute("CREATE TABLE IF NOT EXISTS "+ OutputTable +" AS SELECT * FROM " + InputTable1 + "," + InputTable2 + " WITH NO DATA")

    threads_list = ["thread1","thread2","thread3","thread4","thread5"]

    threads = []

    i = 0

    while i < 5:

        threads.append(Thread(target = threads_join, args = (threads_list[i], openconnection, InputTable1,InputTable2, Table1JoinColumn, Table2JoinColumn, lower+parts, lower)))

        threads[i].start()

        lower += parts

        i = i + 1

    for thread in threads:

        thread.join()

    for tname in threads_list:

        curs.execute("INSERT INTO "+OutputTable+" SELECT * FROM "+tname)

    openconnection.commit()    


def threads_join (thread_name, openconnection, InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, max, min):

    curs = openconnection.cursor()

    curs.execute("DROP TABLE IF EXISTS "+thread_name)

    if not thread_name == "thread1":

        curs.execute("CREATE TABLE " + thread_name +  " AS SELECT * FROM " + InputTable1 + " INNER JOIN " + InputTable2 + " ON " + InputTable1+"."+Table1JoinColumn + " = " + InputTable2+"." +Table2JoinColumn + " WHERE " +  InputTable1+"."+Table1JoinColumn + " > " + str(min) + " and " + InputTable1+"."+Table1JoinColumn + " <= " + str(max) + " and " + InputTable2+"."+Table2JoinColumn + " > " + str(min) + " and " + InputTable2+"."+Table2JoinColumn + " <= " + str(max))

    else:

        curs.execute("CREATE TABLE " + thread_name + " AS SELECT * FROM " +  InputTable1 + " INNER JOIN " +  InputTable2 + " ON " + InputTable1+"."+Table1JoinColumn + " = " + InputTable2 + "." + Table2JoinColumn + " WHERE " +  InputTable1 + "." + Table1JoinColumn +  " >= " + str(min) + " AND " + InputTable1+ "." + Table1JoinColumn + " <= " + str(max) + " and " + InputTable2+"." + Table2JoinColumn +  " >= " + str(min) + " AND " + InputTable2+ "."+Table2JoinColumn + " <= " + str(max))

       
    
