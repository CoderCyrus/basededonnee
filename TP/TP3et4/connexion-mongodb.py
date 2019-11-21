#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Oct 14 10:46:39 2019

@author: Benjamin
"""
#%%
import pprint
from pymongo import MongoClient

client = MongoClient('localhost', 27017)
db = client.merigold

#%% Nombre de fiches
compteur = 0
for collection_name in db.list_collection_names():
    collection = db.get_collection(collection_name)
    compteur += collection.count_documents({}) # Document vide pour tout sélectionner
print(f'Nombre de fiches : {compteur}')

#%% Fiche de Jacques WEBER
collection = db.get_collection('Alice Dumond')
fiche = collection.find_one({'Nom-Prenom': 'Jacques WEBER'})
pprint.pprint(fiche)

#%% Fiche d’entreprise "Primeur & co"
collection = db.get_collection('Pascal Lelievre')
fiche = collection.find_one({'Societe': 'Primeur & co'})
pprint.pprint(fiche)

#%% Nombre de fiches de chaque collaborateur
for collection_name in db.list_collection_names():
    collection = db.get_collection(collection_name)
    nb_fiches = collection.count_documents({}) 
    print(f'{collection_name} a {nb_fiches} fiches')