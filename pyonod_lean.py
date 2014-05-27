#!/opt/local/bin/python

import select
import psycopg2
import psycopg2.extensions
import numpy as np
import time

dbname = 'sensordb'
host = '213.165.92.187'
user = 'node'
password = 'burger89]crew'

DSN = 'dbname=%s host=%s user=%s password=%s' % (dbname, host, user, password)


def moving_average(a, n=3) :
    ret = np.cumsum(a, dtype=float)
    ret[n:] = ret[n:] - ret[:-n]
    return ret[n - 1:] / n

def write_event(locId,event):
    curs.execute("SELECT m.\"reading\", m.\"sensorId\" FROM \"Measurements\" m, \"Sensors\" s WHERE m.\"sensorId\" = s.\"sensorId\" AND s.\"unitTypeId\"=17 AND s.\"locId\"=%s ORDER BY m.timestamp DESC LIMIT 1;" % locId )
    record  = curs.fetchone()
    if not record:
        print "No onlinePresence sensor in locId %s" % locId
        print "Creating one..."
        curs.execute("INSERT INTO \"Sensors\" (\"sensorId\",\"locId\",\"unitTypeId\",\"name\") VALUES (DEFAULT,%s,17,\'onlinePresence\') RETURNING \"sensorId\";" % locId)
        record = curs.fetchone()
        sensorId = record[0]
        lastEvent = 999;
    else:    
        lastEvent = record[0]
        sensorId = record[1]    
    if lastEvent != event:
        print "Inserting new event"
        sql = "INSERT INTO \"Measurements\" (\"reading\",\"timestamp\",\"sensorId\") VALUES (%s,%s,%s)"% (event,int(round(time.time()*1000)),sensorId)   
        curs.execute(sql)
    return

conn = psycopg2.connect(DSN)
conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

curs = conn.cursor()
curs.execute("LISTEN measurement;")

print "Waiting for notifications on channel 'measurement'"
while 1:
    if select.select([conn],[],[],5) == ([],[],[]):
        print "waiting..."
    else:
        conn.poll()
        while conn.notifies:
            notify = conn.notifies.pop()
            print "Got NOTIFY:",notify.channel, notify.payload
            locId,sensorId,unitTypeId,timestamp,reading = notify.payload.split(':')
            if unitTypeId == "3":
            	print "Got new CO2 measurement"
            	curs.execute("SELECT reading FROM \"Measurements\" WHERE \"sensorId\"="+sensorId+" ORDER BY timestamp DESC LIMIT 10")
            	rows = curs.fetchall()
            	measurements = []
            	for row in rows:
            		measurements.append(int(row[0]))
            	m = moving_average(measurements)
            	# Decide of levels are rising
            	if np.mean(np.diff(m)) > 0:
            		if max(m) - min(m) > 50:	 
           			    print "arrival in locId "+locId
                        write_event(locId,1)
            	else:
            		if max(m) - min(m) > 25:
            			print "departure in locId "+locId
                        write_event(locId,0)	