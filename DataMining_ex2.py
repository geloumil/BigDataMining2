#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Sat Mar 24 16:23:32 2018

@author: mscuser
"""

import psycopg2
import sys
from datetime import datetime
from datetime import timedelta
from datetime import date
import random
import numpy as np
import csv

now = datetime.now().date()
arrivals = ['11:00', '12:00', '13:00', '14:00', '15:00', '16:00', '23:00', '22:00']

csvfile2 = open('CSV_Database_Of_First_And_Last_Names/GreekCities.csv', 'r')
creader2 = csv.reader(csvfile2, delimiter=',', quotechar='"')

n = 6000     #number of rooms' details

try:
    con = psycopg2.connect("host=localhost user=postgres password=mscuser2017")
    cur = con.cursor()
    cur.execute("CREATE TABLE Room(Id_Room SERIAL PRIMARY KEY, Location varchar(40), Available_From_Date varchar(40), Available_Till_Date varchar(40), Area varchar(40), Price varchar(40), People_Capacity varchar(40),Plus_Person_Price varchar(40), Arrival_Time varchar(40), Single varchar(40), Sofa varchar(40), Double_Bed varchar(40), King_Size varchar(40), Pets varchar(40), Smoking varchar(40), Events varchar(40), Minimum_Days varchar(40), Air_Condition varchar(40), Heating varchar(40), WiFi varchar(40), TV varchar(40), Elevator varchar(40), Parking varchar(40), Oven varchar(40))")
    for j, row in zip(range(n), creader2):

        room = []
        room.extend(row)
        # Available_From_Date, Available_Till_Date

        diff = timedelta(days=random.randint(1, 30))
        diff2 = timedelta(days=random.randint(1, 100))

        from_date = now + diff
        till_date = from_date + diff2
        room.append(str(from_date))
        room.append(str(till_date))

        # Area

        if j%46 != 0:
            area = np.random.randint(20, 100)
        elif j%100 == 2:
            area = " "
        else:
            area = 0
        room.append(area)

        # Price

        if j%46 != 0:
            price = np.random.randint(0, 500)
        elif j%10 == 2:
            price = " "
        else:
            price = 10000
        room.append(price)

        # People_Capacity
        if j%46 != 0:
            capacity = np.random.randint(0, 10)
        elif j%100 == 2:
            capacity = " "
        else:
            capacity = 100
        room.append(capacity)

        # Plus_Person_Price

        if j%46 != 0:
            plus_price = np.random.randint(0, 500)
        elif j%10 == 2:
            plus_price = " "
        else:
            plus_price = 10000
        room.append(plus_price)

        # Arrival_Time

        arrival_time = arrivals[random.randint(0, len(arrivals) - 1)]
        room.append(str(arrival_time))
        
        # Beds

        for i in range(1,5):        #gia ta bed types
            if i%200 != 0:
                beds = np.random.binomial(n=1, p= 0.5)
            elif i%26 == 0:
                beds = " "
            else:
                beds = '20'
            room.append(beds)

        # Rules

        for i in range(1,4):        #gia ta Pets, Smoking, Events
            if i%200 != 0:
                pse = np.random.binomial(n=1, p= 0.5)
            elif i%26 == 0:
                pse =' '
            else:
                pse = '2'
            room.append(pse)
        min_days= np.random.randint(0, 11)      #gia to Minimum_Days
        room.append(min_days)

        for i in range(1,8):      
            if i%200 != 0:
                facilities = np.random.binomial(n=1, p= 0.5)
            elif i%26 == 0:
                facilities = ' '
            else:
                facilities = '2'
            room.append(facilities)
        cur.execute("INSERT into Room (Location, Available_From_Date, Available_Till_Date, Area, Price, People_Capacity, Plus_Person_Price , Arrival_Time, Single , Sofa , Double_Bed , King_Size, Pets, Smoking, Events, Minimum_Days, Air_Condition, Heating, WiFi, TV, Elevator, Parking, Oven) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", room)
        con.commit()
except psycopg2.DatabaseError, e:
        if con:
            con.rollback()

        print 'Error %s' %e
        sys.exit(1)
        
        
csvfile = open('CSV_Database_Of_First_And_Last_Names/PersonData.csv', 'r') 
creader = csv.reader(csvfile, delimiter=',', quotechar='"')
m = 88799         #number of persons' details in csv
cur.execute("CREATE TABLE Person(Id_Person SERIAL PRIMARY KEY, Name varchar(40), Surname varchar(40), Password varchar(40), Username varchar(40), Email varchar(40), Date_of_Birth varchar(40), Age varchar(40), Days_of_Reservation varchar(40))")
counter = 0
for j, row in zip(range(m), creader):
    person = []
    person.extend(row)
    year = random.randint(1930, 2017)
    month = random.randint(1, 12)
    if month in [1, 3, 5, 7, 8, 10, 12]:
        day = random.randint(1, 31)
    elif month == 2:
        day = random.randint(1, 28)
    else:
        day = random.randint(1, 30)

    birth_date = date(year, month, day)
    person.append(str(birth_date))
    #Age according to random birth date
    age = 2018 - year
    person.append(age)
    
    reservation = random.randint(1, 20)
    person.append(reservation)
    cur.execute("INSERT into Person (Name, Surname, Password , Username, Email, Date_of_Birth, Age, Days_of_Reservation) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)", person)
    con.commit()
    counter = counter + 1

cur.execute("CREATE TABLE Reservations(Person_id integer, Room_id integer)")
con.commit()

reservation_lst = [(np.random.randint(0, 88000),np.random.randint(0, 6000)) for i in range(1000000)]     #create 1M list of (id_person, id_room)
for i in range (len(reservation_lst)):        
    cur.execute("INSERT INTO Reservations(Person_id, Room_id) VALUES (%s, %s)", (reservation_lst[i][0],reservation_lst[i][1]))
    con.commit()
    
query = """SELECT * FROM Reservations"""

outputquery = "COPY ({0}) TO STDOUT WITH CSV HEADER".format(query)

with open('reservations', 'w') as f:
    cur.copy_expert(outputquery, f)