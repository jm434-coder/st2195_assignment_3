#packages
library(DBI)
library(dplyr)


#creating database
if (file.exists("airline2.db")) 
  file.remove("airline2.db")

conn <- dbConnect(RSQLite::SQLite(), "airline2.db")

#adding the tables
ontime_2000 <- read.csv("../files/2000.csv", header = TRUE)
ontime_2001 <- read.csv("../files/2001.csv", header = TRUE)
ontime_2002 <- read.csv("../files/2002.csv", header = TRUE)
ontime_2003 <- read.csv("../files/2003.csv", header = TRUE)
ontime_2004 <- read.csv("../files/2004.csv", header = TRUE)
ontime_2005 <- read.csv("../files/2005.csv", header = TRUE)
airports <- read.csv("../files/airports.csv", header = TRUE)
carriers <- read.csv("../files/carriers.csv", header = TRUE)
planes <- read.csv("../files/plane-data.csv", header = TRUE)
combine_ontimes <- bind_rows(ontime_2000, ontime_2001, ontime_2002, ontime_2003, ontime_2004, ontime_2005)
dbWriteTable(conn, "ontime", combine_ontimes)
dbWriteTable(conn, "airports", airports)
dbWriteTable(conn, "carriers", carriers)
dbWriteTable(conn, "planes", planes)

#creating reference to tables for dyplr
ontime_db <- tbl(conn, "ontime")
airports_db <- tbl(conn, "airports")
carriers_db <- tbl(conn, "carriers")
planes_db <- tbl(conn, "planes")

#Question 2
#Which of the following airplanes has the 
#lowest associated average departure delay (excluding cancelled and diverted flights)?

#DBI
q2_DBI <- dbGetQuery(conn,
                      "SELECT model, avg(DepDelay) as average_delay
                      FROM ontime JOIN planes USING (TailNum)
                      WHERE ontime.diverted = 0 AND ontime.cancelled = 0 AND model IN ('737-230', 'ERJ 190-100 IGW', 'A330-223', '737-282')
                      GROUP BY model
                      ORDER BY average_delay")
q2_DBI

#Dyplr
q2_Dyplr <- inner_join(ontime_db, planes_db, by = c("TailNum" = "tailnum")) %>% 
  filter(Diverted == 0, Cancelled == 0, model %in% c('737-230', 'ERJ 190-100 IGW', 'A330-223', '737-282')) %>% 
  group_by(model) %>% 
  summarize(average_delay = mean(DepDelay, na.rm = TRUE)) %>% 
  arrange(average_delay)

q2_Dyplr

#answer: 737-282

#-------

#Question 3
#Which of the following cities has the highest number of inbound flights
#(excluding cancelled flights)

#DBI
q3_DBI <- dbGetQuery(conn,
                      "SELECT airports.city AS city, COUNT(*) AS count_flights
                      FROM ontime JOIN airports ON ontime.dest = airports.iata
                      WHERE ontime.cancelled = 0 AND city IN ('Chicago', 'Atlanta', 'New York', 'Houston')
                      GROUP BY city
                      ORDER BY count_flights DESC")
q3_DBI

#Dyplr
q3_Dyplr <- inner_join(ontime_db, airports_db, by = c("Dest" = "iata")) %>% 
  filter(Cancelled == 0, city %in% c('Chicago', 'Atlanta', 'New York', 'Houston')) %>% 
  group_by(city) %>% 
  summarize(count_flights = n()) %>% 
  arrange(desc(count_flights))

q3_Dyplr

#answer: Chicago
#doubt: Warning message: Closing open result set, pending rows 

#-------

#Question 4
#Which of the following companies has the highest number of cancelled flights?

#DBI
q4_DBI <- dbGetQuery(conn,
                      "SELECT Description, sum(cancelled) as number_cancelled_flights
                      FROM ontime JOIN carriers ON ontime.UniqueCarrier = carriers.Code
                      WHERE Description IN ('United Air Lines Inc.', 'American Airlines Inc.', 'Pinnacle Airlines Inc.', 'Delta Air Lines Inc.')
                      GROUP BY Description
                      ORDER BY number_cancelled_flights DESC")
q4_DBI

#Dyplr
q4_Dyplr <- inner_join(ontime_db, carriers_db, by = c("UniqueCarrier" = "Code")) %>% 
  filter(Description %in% c('United Air Lines Inc.', 'American Airlines Inc.', 'Pinnacle Airlines Inc.', 'Delta Air Lines Inc.')) %>% 
  group_by(Description) %>% 
  summarize(number_cancelled_flights = sum(Cancelled, na.rm = TRUE)) %>% 
  arrange(desc(number_cancelled_flights))

q4_Dyplr

#answer: Delta Air Lines Inc.
#doubt: i lost pinnacle?

#-------

#Question 5
#Which of the following companies has the highest number of cancelled flights,
#relative to their number of total flights?

#DBI
q5_DBI <- dbGetQuery(conn,
                      "SELECT Description, (sum(cancelled)*1.0/count(*)) as cancellation_rate
                      FROM ontime JOIN carriers ON ontime.UniqueCarrier = carriers.Code
                      WHERE Description IN ('United Air Lines Inc.', 'American Airlines Inc.', 'Pinnacle Airlines Inc.', 'Delta Air Lines Inc.')
                      GROUP BY Description
                      HAVING count(*)>0
                      ORDER BY cancellation_rate DESC")
q5_DBI

#Dyplr
q5_Dyplr <- inner_join(ontime_db, carriers_db, by = c("UniqueCarrier" = "Code")) %>% 
  filter(Description %in% c('United Air Lines Inc.', 'American Airlines Inc.', 'Pinnacle Airlines Inc.', 'Delta Air Lines Inc.')) %>% 
  group_by(Description) %>% 
  summarize(cancellation_rate = sum(Cancelled, na.rm = TRUE) * 1.0 / n()) %>%
  filter(n()>0) %>% 
  arrange(desc(cancellation_rate))

q5_Dyplr


#answer: United Air Lines Inc.
#doubt: i lost pinnacle?

#answers in csv

#DBI

write.csv(q2_DBI, "q2_DBI.csv", row.names = FALSE)
write.csv(q3_DBI, "q3_DBI.csv", row.names = FALSE)
write.csv(q4_DBI, "q4_DBI.csv", row.names = FALSE)
write.csv(q5_DBI, "q5_DBI.csv", row.names = FALSE)

#Dyplr
write.csv(q2_Dyplr, "q2_Dyplr.csv", row.names = FALSE)
write.csv(q3_Dyplr, "q3_Dyplr.csv", row.names = FALSE)
write.csv(q4_Dyplr, "q4_Dyplr.csv", row.names = FALSE)
write.csv(q5_Dyplr, "q5_Dyplr.csv", row.names = FALSE)

#Disconnect and clean environment
dbDisconnect(conn)
rm(list = ls())
