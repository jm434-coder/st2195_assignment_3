# -*- coding: utf-8 -*-
"""
Created on Mon Mar  3 19:22:50 2025

@author: joana
"""

#libraries
import os
import sqlite3
import pandas as pd

#creating database
try:
    os.remove('airline2.db')
except OSError:
    pass

conn = sqlite3.connect('airline2.db')


#adding tables
ontime_2000 = pd.read_csv("../files/2000.csv")
ontime_2001 = pd.read_csv("../files/2001.csv", encoding = 'ISO-8859-2')
ontime_2002 = pd.read_csv("../files/2002.csv", encoding = 'ISO-8859-2')
ontime_2003 = pd.read_csv("../files/2003.csv")
ontime_2004 = pd.read_csv("../files/2004.csv")
ontime_2005 = pd.read_csv("../files/2005.csv")
airports = pd.read_csv("../files/airports.csv")
carriers = pd.read_csv("../files/carriers.csv")
planes = pd.read_csv("../files/plane-data.csv")


ontime_2000.to_sql('ontime', con = conn, index = False, if_exists = 'append', chunksize = 100000)
ontime_2001.to_sql('ontime', con = conn, index = False, if_exists = 'append', chunksize = 100000)
ontime_2002.to_sql('ontime', con = conn, index = False, if_exists = 'append', chunksize = 100000)
ontime_2003.to_sql('ontime', con = conn, index = False, if_exists = 'append', chunksize = 100000)
ontime_2004.to_sql('ontime', con = conn, index = False, if_exists = 'append', chunksize = 100000)
ontime_2005.to_sql('ontime', con = conn, index = False, if_exists = 'append', chunksize = 100000)
airports.to_sql('airports', con = conn, index = False)
carriers.to_sql('carriers', con = conn, index = False)
planes.to_sql('planes', con = conn, index = False)


c = conn.cursor()


#Question 2
#Which of the following airplanes has the 
#lowest associated average departure delay (excluding cancelled and diverted flights)?

               
q2 = c.execute('''
               SELECT model, avg(DepDelay) as average_delay
               FROM ontime JOIN planes USING (TailNum)
               WHERE ontime.diverted = 0 AND ontime.cancelled = 0 AND model IN ('737-230', 'ERJ 190-100 IGW', 'A330-223', '737-282')
               GROUP BY model
               ORDER BY average_delay
''').fetchall()

pd.DataFrame(q2)

#answer: 737-282

#-------

#Question 3
#Which of the following cities has the highest number of inbound flights
#excluding cancelled flights

q3 = c.execute('''
               SELECT airports.city AS city, COUNT(*) AS count_flights
               FROM ontime JOIN airports ON ontime.dest = airports.iata
               WHERE ontime.cancelled = 0 AND city IN ('Chicago', 'Atlanta', 'New York', 'Houston')
               GROUP BY city
               ORDER BY count_flights DESC
''').fetchall()

pd.DataFrame(q3)

#answer: Chicago

#-------

#Question 4
#Which of the following companies has the highest number of cancelled flights?

q4 = c.execute('''
               SELECT Description, sum(cancelled) as number_cancelled_flights
               FROM ontime JOIN carriers ON ontime.UniqueCarrier = carriers.Code
               WHERE Description IN ('United Air Lines Inc.', 'American Airlines Inc.', 'Pinnacle Airlines Inc.', 'Delta Air Lines Inc.')
               GROUP BY Description
               ORDER BY number_cancelled_flights DESC
''').fetchall()

pd.DataFrame(q4)

#answer: Delta Air Lines Inc.

#-------

#Question 5
#Which of the following companies has the highest number of cancelled flights,
#relative to their number of total flights?

q5 = c.execute('''
               SELECT Description, (sum(cancelled)*1.0/count(*)) as cancellation_rate
               FROM ontime JOIN carriers ON ontime.UniqueCarrier = carriers.Code
               WHERE Description IN ('United Air Lines Inc.', 'American Airlines Inc.', 'Pinnacle Airlines Inc.', 'Delta Air Lines Inc.')
               GROUP BY Description
               HAVING count(*)>0
               ORDER BY cancellation_rate DESC
''').fetchall()

pd.DataFrame(q5)

#answer: United Air Lines Inc

#answers in csv
q2_df = pd.DataFrame(q2)
q2_df.to_csv('q2.csv', index=False)
q3_df = pd.DataFrame(q3)
q3_df.to_csv('q3.csv', index=False)
q4_df = pd.DataFrame(q4)
q4_df.to_csv('q4.csv', index=False)
q5_df = pd.DataFrame(q5)
q5_df.to_csv('q5.csv', index=False)


#close everything
c.close()
conn.close()
