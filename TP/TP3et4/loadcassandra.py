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
    
#%% lister les données d’un camion donné 
    data =  {"camion" :'AC-543-AG'}
    rows = session.execute("SELECT * FROM camion where camion_id = %(camion)s",data)
    for user_row in rows:
        print(user_row)  

#%% vérifier que vous avez bien accès aux données des GPS sur une plage horaire donnée
data = {"camion" :'AC-543-AG'}
date = {"heure" : '(2019, 7, 1, 0, 4, 52)'}
rows = session.execute("SELECT * FROM camion where camion_id = %(camion)s",data)
for user_row in rows:
        print(user_row)  
    
 
#%% fermeture le serve
#session.shutdown()
#cluster.shutdown()



