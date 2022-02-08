#!/bin/bash

COLLECTION=$1

python code/dump_to_redis.py --mongo-uri="mongodb://reader:MongoDb-e7ae3f6950b744840b1d4@medical00-4.tau.ac.il:80/?authSource=admin&authMechanism=SCRAM-SHA-256&readPreference=primary&appname=MongoDB%20Compass&ssl=false" --db-name=brca_omics --collection=$COLLECTION
