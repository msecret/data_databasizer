#!/usr/bin/python
# Copyright  Marco Segreto 2014

"""
Takes a json file and a mongo database collection and inserts the json data
as documents.

Use:
json_to_mongo {json file} {collection} '{mongo connection client uri}'

"""

import sys
import json

import pymongo
from pymongo import MongoClient

def read_file(arg_file):
  """Read file, parse it as json and return it

  Only works with json valid files.

  """
  with open(arg_file) as file:
    json_data = json.load(file)

  return json_data

def setup_connection(uri):
  """Setup connection to database with host, database name and possible port.

  """
  client = MongoClient(uri)
  db = client.get_default_database()
  return db

def write_to_db(db, data, db_collection):
  """Write data to the db and return ids, or errors if there were any

  """
  print db
  collection = db[db_collection]
  if collection is None:
    return None, Exception("Collection was not given.")
  ids = []
  try:
    for doc in data:
      ids.append(collection.insert(doc))
  except Exception as e:
    return None, e

  return ids, None

def parse_args(args):
  """Parse all args, check that there are enough and return them in named
  dict.

  """
  ret_args = {}
  num_args  = len(sys.argv)
  if num_args < 2:
    sys.exit("Not enough arguments, need at least file and collection.")

  ret_args['file'] = args[1]
  ret_args['db_collection'] = args[2]
  ret_args['db_connect'] = args[3]

  return ret_args

def main():
  args = sys.argv
  args = parse_args(args)

  file_data = read_file(args['file'])

  db = setup_connection(args['db_connect'])

  success, failure = write_to_db(db, file_data, args['db_collection'])
  print success, failure

if __name__ == "__main__":
   main()
