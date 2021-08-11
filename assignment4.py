#!/usr/bin/python2.7
#
# Assignment2 Interface
#

import psycopg2
import os
import sys
# Donot close the connection inside this file i.e. do not perform openconnection.close()
def RangeQuery(ratingsTableName, ratingMinValue, ratingMaxValue, openconnection):
	
    curs = openconnection.cursor()
    
    command = ''' SELECT * FROM RangeRatingsMetaData '''
    
    curs.execute(command)
    
    datarows = curs.fetchall()
    
    curs.execute("SELECT COUNT(*) FROM RangeRatingsMetaData ")
    
    parts = int(curs.fetchall()[0][0])
    
    writing =[] 
	
    part_inc=0
    

    while part_inc < parts:
    
        rows = datarows[part_inc]
        
        min_rating = rows[1]
        
        max_rating = rows[2]
        
        if((ratingMinValue <= max_rating)  and (ratingMaxValue >= min_rating)):
        
            curs.execute("SELECT * FROM RangeRatingsPart" + str(rows[0]) + " WHERE rating >= " + str(ratingMinValue) + " AND rating <= " + str(ratingMaxValue) + ";")
		    
            out=curs.fetchall()
            
	    for rqrange in out:

                writing.append(["RangeRatingsPart"+str(rows[0]),rqrange[0],rqrange[1],rqrange[2]])
	    
        part_inc=part_inc+1

    curs.execute("SELECT partitionnum FROM RoundRobinRatingsMetadata;")
    
    numberofpartitions = int(curs.fetchall()[0][0])
    
    rqp=0
    
    while rqp < numberofpartitions :
    
        curs.execute("SELECT * FROM RoundRobinRatingsPart" + str(rqp) + " WHERE rating >= " + str(ratingMinValue) + " AND rating <= " + str(ratingMaxValue) + ";")
        
        out = curs.fetchall()
		
        for rqrobin in out:
        
            writing.append(["RoundRobinRatingsPart"+str(rqp),rqrobin[0],rqrobin[1],rqrobin[2]])
            
        rqp=rqp+1
	
	    
    writeToFile("RangeQueryOut.txt", writing)
        



def PointQuery(ratingsTableName, ratingValue, openconnection):

    curs = openconnection.cursor()
    
    curs.execute("SELECT * FROM RangeRatingsMetaData")
    
    datarows = curs.fetchall()
    
    curs.execute("SELECT COUNT(*) FROM RangeRatingsMetaData")
    
    parts = int(curs.fetchall()[0][0])
    
    writing = []
    
    part_inc=0
    
    while part_inc < parts:
    
        rows = datarows[part_inc]
        
        min_rating = rows[1]
        
        max_rating = rows[2]
        
        if((ratingValue <= max_rating)  and (ratingValue >= min_rating)):
        
            curs.execute("SELECT * FROM RangeRatingsPart" + str(rows[0]) + " WHERE rating = " + str(ratingValue) + ";")
           
            out = curs.fetchall()
           
            for pqrange in out:
		    
                writing.append(["RangeRatingsPart"+str(rows[0]),pqrange[0],pqrange[1],pqrange[2]])
	
        part_inc = part_inc+1
    
    curs.execute("SELECT partitionnum FROM RoundRobinRatingsMetaData")
    
    numberofpartitions = int(curs.fetchall()[0][0])
    
    pqp=0
    
    while pqp < numberofpartitions :
    
        curs.execute("SELECT * FROM RoundRobinRatingsPart" + str(pqp) + " WHERE rating = " + str(ratingValue) + ";")
	
        out = curs.fetchall()
	    
        for pqrobin in out:
	        
            writing.append(["RoundRobinRatingsPart" + str(pqp),pqrobin[0], pqrobin[1], pqrobin[2]])
	
        pqp=pqp+1

	   
	   
    writeToFile("PointQueryOut.txt", writing)


def writeToFile(filename, rows):
    f = open(filename, 'w')
    for line in rows:
        f.write(','.join(str(s) for s in line))
        f.write('\n')
    f.close()
