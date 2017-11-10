#!/usr/bin/env python3

## Re-key the dataset, i.e., replace the ids with surrogate (global) ids

import glob
import re

def get_id(line, index):
    return int(line.split('|')[index])

prefix = "social_network/"
postfix = "_0_0.csv"
vertex_labels = ["comment", "forum", "organisation", "person", "place", "post", "tagclass", "tag"]

global_id = 0

for vertex_label in vertex_labels:
    print("(:" + vertex_label + ")")

    filename = prefix + vertex_label + postfix

    with open(filename, encoding="utf-8") as file:
        lines = file.readlines()

    # throw away header
    lines = lines[1:]

    old_ids_list = [int(line.split('|')[0]) for line in lines]
    old_ids_set = set(old_ids_list)

    mapping_to_global_id = {}
    for old_id in old_ids_set:
        global_id = global_id + 1
        mapping_to_global_id[old_id] = global_id

    # re-key vertex file (ID)
    with open(filename, encoding="utf-8") as file:
        lines = file.readlines()

        with open(filename, "w") as out_file:
            # header
            out_file.write(lines[0])
            # content
            lines = lines[1:]
            for old_line in lines:
                old_id = get_id(old_line, 0)
                new_id = mapping_to_global_id[old_id]

                # regex to get the first cell with the start id
                regex = re.compile("^" + str(old_id) + "\|")
                new_line = regex.sub(str(new_id) + "|", old_line)
                out_file.write(new_line)


    # re-key edge files with the same source vertex label (START_ID)
    for filename in glob.glob(prefix + vertex_label + "_*" + postfix):
        print("-> " + filename)
        with open(filename, encoding="utf-8") as file:
            lines = file.readlines()

            with open(filename, "w") as out_file:
                # header
                out_file.write(lines[0])
                # content
                lines = lines[1:]
                for old_line in lines:
                    old_id = get_id(old_line, 0)
                    new_id = mapping_to_global_id[old_id]
                    regex = re.compile("^" + str(old_id) + "\|")
                    new_line = regex.sub(str(new_id) + "|", old_line)
                    out_file.write(new_line)

    # re-key edge files with the same target vertex label (END_ID)
    for filename in glob.glob(prefix + "*_" + vertex_label + postfix):
        print("<- " + filename)
        with open(filename, encoding="utf-8") as file:
            lines = file.readlines()

            with open(filename, "w") as out_file:
                # header
                out_file.write(lines[0])
                # content
                lines = lines[1:]
                for old_line in lines:
                    old_id = get_id(old_line, 1)
                    new_id = mapping_to_global_id[old_id]
                    # regex to get the second cell with the end id
                    regex = re.compile("^(\d+)\|" + str(old_id))
                    new_line = regex.sub("\\1|" + str(new_id), old_line)
                    out_file.write(new_line)