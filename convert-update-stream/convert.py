import argparse
import sys
import os
import re
import time
import duckdb


parser = argparse.ArgumentParser()
parser.add_argument('--input_dir', required=True)
parser.add_argument('--output_dir', required=True)
args = parser.parse_args()

con = duckdb.connect(database='snb.duckdb')

# todo: support arguments

with open("schema.sql") as f:
    schema_def = f.read()
    con.execute(schema_def)

data_root_path = args.input_dir
output_path = args.output_dir
data_path = f"{data_root_path}/graphs/csv/bi/composite-merged-fk/inserts/dynamic"

for entity in ["Comment", "Comment_hasTag_Tag", "Forum", "Forum_hasMember_Person", "Forum_hasTag_Tag", "Person", "Person_hasInterest_Tag", "Person_knows_Person", "Person_likes_Comment", "Person_likes_Post", "Person_studyAt_University", "Person_workAt_Company", "Post", "Post_hasTag_Tag"]:
    print(f"===== {entity} =====")
    entity_dir = f"{data_path}/{entity}"
    print(f"--> {entity_dir}")
    for batch in [batch for batch in os.listdir(f"{entity_dir}/") if os.path.isdir(f"{entity_dir}/{batch}")]:
        for csv_file in [f for f in os.listdir(f"{entity_dir}/{batch}") if f.startswith("part-") and f.endswith(".csv")]:
            csv_path = f"{entity_dir}/{batch}/{csv_file}"
            print(csv_path)
            con.execute(f"COPY {entity} FROM '{csv_path}' (DELIMITER '|', HEADER, TIMESTAMPFORMAT '%Y-%m-%dT%H:%M:%S.%g+00:00');")

with open("convert.sql") as f:
    convert_script = f.read()
    con.execute(convert_script)

for entity in [
        "Comment",                # INS7
        "Forum",                  # INS4
        "Forum_hasMember_Person", # INS5
        "Person",                 # INS1
        "Person_knows_Person",    # INS8
        "Person_likes_Comment",   # INS3
        "Person_likes_Post",      # INS2
        "Post"                    # INS6
    ]:
    con.execute(f"""
        COPY
            (SELECT strftime(creationDate::timestamp, '%Y-%m-%dT%H:%M:%S.%g+00:00') AS creationDate, * EXCLUDE creationDate FROM {entity}_Update)
            TO '{output_path}/{entity}.csv'
            (DELIMITER '|', HEADER)
        """)
