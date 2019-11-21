from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import csv

auth_provider = PlainTextAuthProvider(username='studentbdonn', password='SAO201907')
cluster = Cluster(contact_points=['ser-info-03.ec-nantes.fr'], auth_provider=auth_provider)
session = cluster.connect('merigold')

csvfile = open("dataCassandra.csv","r")
try:
	reader = csv.reader(csvfile,delimiter=";")
	for row in reader:
		session.execute("INSERT INTO camion(camion_id, heure, gps_x, gps_y) VALUES (\'" + row[0] + "\', \'" + row[3] + "\', " + row[1] + ", " + row[2] +");")
finally:
	csvfile.close()

rows = session.execute("SELECT * FROM camion")
for user_row in rows:
    print (user_row)

csvfile = open("dataCassandra.csv","r")
try:
	reader = csv.reader(csvfile,delimiter=";")
	for row in reader:
		session.execute("INSERT INTO position(heure, camion_id, gps_x, gps_y) VALUES (\'" + row[3] + "\', \'" + row[0] + "\', " + row[1] + ", " + row[2] +");")
finally:
	csvfile.close()

rows = session.execute("SELECT * FROM position")
for user_row in rows:
    print (user_row)


