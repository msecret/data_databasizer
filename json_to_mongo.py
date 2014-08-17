#!/usr/bin/python
# Copyright  Marco Segreto 2014

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

def setup_connection(host, dbase, port=27017):
  """Setup connection to database with host, database name and possible port.

  """
  client = MongoClient(host, port)
  db = client[dbase]
  return db

def write_to_db(db, data):
  """Write data to the db and return ids, or errors if there were any

  """
  collection = db['comments']
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
    sys.exit("Not enough arguments")

  ret_args['file'] = args[1]
  ret_args['db_host'] = args[2]
  ret_args['db_dbase'] = args[3]
  if num_args > 4:
    ret_args['db_port'] = str(args[4])

  return ret_args

def main():
  args = sys.argv
  args = parse_args(args)

  file_data = read_file(args['file'])

  db = setup_connection(args['db_host'], args['db_dbase'])

  success, failure = write_to_db(db, file_data)
  print success, failure

if __name__ == "__main__":
   main()
