#!/usr/bin/python
# -*- coding: utf-8 -*-

import client

db = client.Pfft("127.0.0.1", "user")

def example():
  # List out current triples
  db.triples("", "", "")

  # Apply geo inference rule
  db.inference("geo")

  # list triples again, now with latitude and logitude
  db.triples("", "", "")

  # Get all the names of users
  db.triples("", "foaf:name", "")

  # Get all the addresses of users
  db.triples("", "address", "")

  # Get users with id and name
  db.query([["?id", "foaf:name", "?name"]])

  # Get users with id name and address
  db.query([["?id", "foaf:name", "?name"], ["?id", "address", "?address"]])

  # Get all the users who know user 1
  db.query([["?id", "foaf:name", "?name"], ["?id", "address", "?address"], ["?id", "foaf:knows", "_:1"]])

  # Add a random user who you know nothing about and make them friends with 
  db.add([["_:5", "foaf:knows", "_:1"], ["_:5", "foaf:name", "wilson"]])

  # Query friends of _:1 again
  db.query([["?id", "foaf:name", "?name"], ["?id", "foaf:knows", "_:1"]])

def categories():
  db.add([
    ["category:type", "rdf:type", "owl:TransitiveProperty"], 

    ["category:1", "rdf:type", "category:type"], 
    ["category:1", "rdfs:label", "Politics"], 

    ["category:2", "rdfs:subClassOf", "category:1"], 
    ["category:2", "rdfs:label", "US Politics"], 
    ["category:3", "rdfs:subClassOf", "category:1"], 
    ["category:3", "rdfs:label", "Russian Politics"], 

    ["category:4", "rdf:type", "category:type"],
    ["category:4", "rdfs:label", "Sports"],
    ["category:5", "rdfs:subClassOf", "category:4"],
    ["category:5", "rdfs:label", "US Sports"],
    ["category:6", "rdfs:subClassOf", "category:4"],
    ["category:6", "rdfs:label", "British Sports"],
    ["category:7", "rdfs:subClassOf", "category:5"],
    ["category:7", "rdfs:label", "Football"],
    ["category:8", "rdfs:subClassOf", "category:5"],
    ["category:8", "rdfs:label", "Baseball"],
    ["category:9", "rdfs:subClassOf", "category:6"],
    ["category:9", "rdfs:label", "Cricket"],
  ])

def items():
  db.add([
    #["item:type", "rdf:type", "owl:TransitiveProperty"],

    ["item:1", "rdf:type", "item:type"],
    ["item:1", "item:category", "category:7"],
    ["item:1", "dc:title", "The Bears are losers"],
    ["item:1", "dc:description", "They lost yet again, now for the 100th time."],
    ["item:1", "dc:author", "Charles Knowsallot"],

    ["item:2", "rdf:type", "item:type"],
    ["item:2", "item:category", "category:3"],
    ["item:2", "item:data", "data:1"],
    ["item:2", "item:data", "data:2"],
    ["data:1", "dc:language", "en"],
    ["data:1", "dc:title", "Putin kills a bear"],
    ["data:1", "dc:description", "With his bear hands Putin punches and eats a bear alive."],

    ["data:2", "dc:language", "ru"],
    ["data:2", "dc:author", "Vlarry Romanov"],
    ["data:2", "dc:title", "Путин убийство несут"],
    ["data:2", "dc:description", "С его голыми руками Путин зубил и съест все живы"],
  ])

def users():
  db.add([
    ["foaf:Person", "rdf:type", "owl:SymmetricProperty"],

    ["user:1", "user:loggedIn", True],
    ["user:1", "foaf:mbox", "a@a.com"],
    ["user:1", "foaf:name", "Larry Lipshitz"],
    ["user:1", "user:facebook", "fb:1"],
    ["user:1", "user:pref", "_:1"],
    ["user:1", "rdf:type", "foaf:Person"],
    ["user:1", "address", "25 Lusk Street, San Francisco, CA 94107"],
    ["_:1", "item:id", "item:1"],
    ["_:1", "item:val", 1],
    ["user:1", "user:pref", "_:2"],
    ["_:2", "item:id", "item:2"],
    ["_:2", "item:val", 0],
    ["fb:1", "fb:email", "a@facebook.com"],
    ["fb:1", "fb:like", "resource1"],
    ["fb:1", "fb:like", "resource2"],
    ["fb:1", "fb:token", "someaccesstoken"],

    ["user:2", "rdf:type", "foaf:Person"],
    ["user:2", "foaf:knows", "user:1"],
    ["user:2", "address", "500 Howard Street, San Francisco, CA 94107"],
  ])

if __name__ == '__main__':
  pass
  # Define taxonomy of categories with owl:transitive
  #reco.categories()

  # Get all categories and their parents
  #reco.db.query([["?id", "rdfs:subClassOf", "?parentid"], ["?parentid", "rdfs:label", "?parentlabel"], ["?id", "rdfs:label", "?label"]])

  # Create items
  #reco.items()

  # Get all items in football and item data
  #reco.db.query([["?itemid", "item:category", "category:7"], ["?itemid", "?key", "?val"]])

  # Get all US Sports
  #reco.db.query([["?catid", "rdfs:subClassOf", "category:5"], ["?itemid", "item:category", "?catid"], ["?itemid", "?key", "?val"]])

  # Get russian politics in english
  #reco.db.query([["?catid", "rdfs:subClassOf", "category:1"], ["?itemid", "item:category", "?catid"], ["?itemid", "item:data", "?dataid"], ["?dataid", "dc:language", "en"], ["?dataid", "?key", "?val"]])

  # Get russian politics in russian
  #reco.db.query([["?catid", "rdfs:subClassOf", "category:1"], ["?itemid", "item:category", "?catid"], ["?itemid", "item:data", "?dataid"], ["?dataid", "dc:language", "ru"], ["?dataid", "?key", "?val"]])

  # Add users
  #reco.users()

  # get users preferences
  #reco.db.query([["user:1", "user:pref", "?prefid"], ["?prefid", "item:val", "?val"], ["?prefid", "item:id", "?itemid"]])
