#!/usr/bin/env python3

import argparse
import os
import sys

import boto3
import json

def get_entity_sizes(bucket, prefix):
    s3 = boto3.client("s3")
    prefix = f"{prefix}social_network/csv/raw/composite-merged-fk/dynamic/"
    more = True
    token = None
    sizes = {}
    while more:
        resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, **({'ContinuationToken': token} if token else {}))
        for obj in resp["Contents"]:
            splits = obj["Key"][len(prefix):].split("/", 1)
            if len(splits) > 1:
                entity, rest = splits
                if rest.endswith(".csv"):
                    total, c, m = sizes.get(entity, [0, 0, 0])
                    sizes[entity] = [total + obj["Size"], c + 1, max(m, obj["Size"])]
        more = False
        if 'NextContinuationToken' in resp.keys():
            token = resp['NextContinuationToken']
            more = True
    with open(f"sizes.json", "w") as f:
        json.dump(sizes, f)

get_entity_sizes("ldbc-datagen-sf10k-debug", "sf1000/runs/20210412_091530/")
