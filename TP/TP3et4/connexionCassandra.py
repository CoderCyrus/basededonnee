#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Oct 21 08:07:27 2019

@author: muruowang
"""
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

#%% connexion à la base
auth_provider = PlainTextAuthProvider(username='studentbdonn', password='SAO201907')
cluster = Cluster(contact_points=['ser-info-03.ec-nantes.fr'], auth_provider=auth_provider)
session = cluster.connect('merigold')

#%% lister les données de tous les camion
rows = session.execute("SELECT * FROM camion group by camion_id")
for user_row in rows:
    print (user_row)  
    
#%% lister les données de tous les camion et nombre
rows = session.execute("SELECT camion_id, count(*) FROM camion group by camion_id")
for user_row in rows:
    print (user_row) 
    
#%% lister les données d’un camion donnée
data =  {"camion" :'AC-543-AG'}
rows = session.execute("SELECT * FROM camion where camion_id = %(camion)s",data)
computer = 0
for user_row in rows:
    print(user_row)  
    computer +=1
print("Nombre de ligne " +str(computer))

#%% vérifier que vous avez bien accès aux données des GPS sur une plage horaire donnée
data = {"heure_debut" : '2019-07-01 05:59:11' , "heure_fin" :   '2019-07-01 10:59:11'}
# CQL filtering
rows = session.execute("SELECT * FROM position where heure >  %(heure_debut)s and heure < %(heure_fin)s allow filtering",data)
for user_row in rows:
    print(user_row)  
    
    
#%% vérifier que vous avez bien accès aux données des GPS sur une plage horaire donnée
data = {"heure_debut" : '2019-07-01 05:59:11' , "heure_fin" :   '2019-07-01 10:59:11'}
# CQL filtering
rows = session.execute("SELECT * FROM position where heure >  %(heure_debut)s and heure < %(heure_fin)s allow filtering",data)
for user_row in rows:
    print(user_row)  
    
 
#%% fermeture le serve
#session.shutdown()
#cluster.shutdown()
