data_databasizer
================

Parses, formats data and inserts into a database.


json_to_mongo
=============

Takes a json file and inserts the data into a mongodb collection.

Does not support mongo authentication. Uses insert, so will insert duplicate
data.

Use
---
```bash
python json_to_mongo {json file} {collection} '{mongo connection client uri}'
```

Example
-------
```bash
python json_to_mongo.py test.json comments 'mongodb://localhost:27017/fcc_comments'
```
