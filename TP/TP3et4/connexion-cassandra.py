#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Oct 21 08:04:16 2019

@author: Benjamin
"""
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

#%% Connexion à la base
auth_provider = PlainTextAuthProvider(username='studentbdonn', password='SAO201907')
cluster = Cluster(contact_points=['ser-info-03.ec-nantes.fr'], auth_provider=auth_provider)
session = cluster.connect('merigold')

#%% Lister tous les camions et le nombre de données dans la base
rows = session.execute("SELECT camion_id, COUNT(*) FROM camion GROUP BY camion_id")
for camion in rows:
    print(camion)

#%% Lister les données de tous les camions
rows = session.execute("SELECT * FROM camion")
for user_row in rows:
    print (user_row)  

#%% Lister les données d'un camion donné
data = {"camion_id": 'AD-671-KA'}
rows = session.execute("SELECT * FROM camion WHERE camion_id = %(camion_id)s ORDER BY heure", data)
compteur = 0
for camion_row in rows:
    print(camion_row)
    compteur += 1
print("Nombre de lignes : " + str(compteur))

#%% Données GPS entre 8h00 et 8h30 le 2019-07-01
data = {"heure_debut": "2019-07-01 08:00:00", "heure_fin": "2019-07-01 08:30:00"}
rows = session.execute("SELECT * FROM position WHERE heure >= %(heure_debut)s \
                       AND heure <= %(heure_fin)s ALLOW FILTERING", data)
for row_heure in rows:
    print(row_heure)


#%% Fermeture de la session et du cluster
session.shutdown()
cluster.shutdown()









