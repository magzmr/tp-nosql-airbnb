"""
TP NoSQL
Prénom : Magui

Nom : Azmirly

"""


import pymongo
import pandas as pd

URI = 'mongodb+srv://mongo_user:tbSzgpDPccljy8yo@cluster-but-sd.bo2es.mongodb.net/?retryWrites=true&w=majority&appName=cluster-but-sd'
client = pymongo.MongoClient(URI)
db = client.tp

# output the name of the collections in the database
print("Collections: ", db.list_collection_names())

#1. Combien de logements sont dans la base de données ?
print("Nb de logements dans la BDD: ", db.airbnb.count_documents({}))

#2. Quel est le prix moyen par ville ? Trier les villes par prix décroissant.

q2 = db.airbnb.aggregate([
    {"$group": {"_id": "$address.market", 
                "prix_moyen": {"$avg": "$price"}}},
    {"$sort": {"prix_moyen": -1}}
]) 
    
print("Prix moyen par ville (prix décroissant): ", pd.DataFrame(list(q2)))

#3. Afficher la liste de tous les différents équipements qui existent.

q3 = db.airbnb.distinct("amenities")
print("Liste des différents équipements: ", pd.DataFrame(list(q3)))

#4. Combien de propriétés incluent le Wifi dans les équipements ?

print("Nb de propriétés incluant le Wifi dans les équipements: ",db.airbnb.count_documents({"amenities":"Wifi"}))

#5. Afficher le nom de tous les logements ainsi que le nombre de chambres et de lits qu'ils contiennent (ne pas afficher l'ID)

q5 = db.airbnb.find({}, {"name": 1, "bedrooms": 1, "beds": 1, "_id": 0})
for logement in q5:
    print("Nom de tous les logements ainsi que le nombre de chambres et de lits qu'ils contiennent: ", pd.DataFrame(list(q5)))

#6. Afficher le nom et le prix des logements situés à Porto.

q6 = db.airbnb.find({"address.market": "Porto"}, {"name": 1, "price": 1, "_id": 0})
for logement in q6:
    print("Nom et prix des logements situés à Porto: ", pd.DataFrame(list(q6)))

#7. Quels sont les 5 hôtes les plus populaires (ceux dont les propriétés ont reçu le plus de commentaires) ?

q7 = db.airbnb.aggregate([
    {"$project": {"name": 1, 
                  "nb_reviews": {"$size":{ "$ifNull": ["$reviews", []]}}}},
    {"$sort": {"nb_reviews": -1}},
    {"$limit": 5}
])
for host in q7:
    print("Top 5 hôtes ayant recu le plus de commentaires: ", pd.DataFrame(list(q7)))
    
#8. Quelles sont les 6 villes ayant le plus de logements disponibles à la location ?
    
q8 = db.airbnb.aggregate([
    {"$group": {"_id": "$address.market", "nb_logements": {"$sum": 1}}},
    {"$sort": {"nb_logements": -1}},
    {"$limit": 6}
])

print("Les 6 villes ayant eu le plus de logements disponibles à la location: ", pd.DataFrame(list(q8)))

#9. Combien de propriétés acceptent plus de 4 invités et ont une caution de moins de 300€ ?   

print("Nb de propriétés acceptant plus de 4 invités et ayant une caution de moins de 300€: ",db.airbnb.count_documents({
    "accommodates": {"$gt": 4},
    "security_deposit": {"$lt": 300}
}))

#10. Donner les 20 utilisateurs qui ont fait le plus de commentaires (afficher seulement l'ID et le nom de l'utilisateur).

q10 = db.airbnb.aggregate([
    {"$unwind": "$reviews"},
    {"$group": {"_id": "$reviews.reviewer_id", "nom_utilisateur": {"$first": "$reviews.reviewer_name"}, "nb_avis": {"$sum": 1}}},
    {"$sort": {"nb_avis": -1}},
    {"$limit": 20}
])

print("Les 20 utilisateurs ayant fait le plus de commentaires: ", pd.DataFrame(list(q10)))

#11. Parmi les logements à Sydney, quel est la note moyenne des visiteurs ?

q11 = db.airbnb.aggregate([
    {"$match": {"address.market": "Sydney"}},
    {"$group": {"_id": None, "note_moyenne": {"$avg": "$review_scores.review_scores_rating"}}}
])

print("Note moyenne des visiteurs parmi les logements à Sydney: ", pd.DataFrame(list(q11)))

#12. (bonus) Afficher les logements qui contiennent le mot "park" dans leur nom

q12 = db.airbnb.find(
    {"name": {"$regex": "park", "$options": "i"}},
    {"name": 1, "_id": 0}
)

print("Logements contenant le mot 'park' dans leur nom: ", pd.DataFrame(list(q12)))

#13. (bonus) Afficher le nom des logements ayant une lattitude comprise entre 36,1 et 40,6 dont le prix est entre 100 et 200 euros.

q13 = db.airbnb.find(
    {
        "location.coordinates.1": {"$gte": 36.1, "$lte": 40.6},
        "price": {"$gte": 100, "$lte": 200}
    },
    {"name": 1, "_id": 0})

print("Nom des logements ayant une lattitude comprise entre 36,1 et 40,6 dont le prix est entre 100 et 200 euros: ", pd.DataFrame(list(q13)))






































    