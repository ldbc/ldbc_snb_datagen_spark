#!/usr/bin/env python3

import glob
import re
from tempfile import NamedTemporaryFile

prefix="social_network/"
postfix="_0_0.csv"
vertex_labels = ["comment", "forum", "organisation", "person", "place", "post", "tagclass", "tag"]

new_id = 0

for vertex_label in vertex_labels:
    print(vertex_label)

    filename = prefix + vertex_label + postfix

    with open(filename, encoding="utf-8") as file:
        my_list = file.readlines()

    # trim newlines
    my_list = [x.strip() for x in my_list]
    # throw away header
    my_list = my_list[1:]

    old_ids_list = [int(e.split('|')[0]) for e in my_list]
    old_ids_set = set(old_ids_list)

    old_new_id_mapping = {}
    for old_id in old_ids_set:
        old_new_id_mapping[old_id] = new_id
        new_id = new_id + 1

    with open(filename, encoding="utf-8") as in_file:
        csv_data = in_file.read()

    print(old_new_id_mapping)
    for old_id, new_id in old_new_id_mapping.items():
        csv_data = csv_data.replace(str(old_id), str(new_id))

    with open(filename, "w") as out_file:
        out_file.write(csv_data)



# for vertex_label in ${vertex_labels}; do
# echo == == == == == == == == == == == == == == == == == == == == == == == == ==
# echo ${vertex_label}
# echo == == == == == == == == == == == == == == == == == == == == == == == == ==
#
# vertex_file =${vertex_label}$postfix
# tail - n + 2 ${vertex_file} | cut - d
# '|' - f
# 1 | sort - u > current_ids.txt
#
# echo
# vertex
# type: ${vertex_file}
# echo
# outgoing
# edge
# types: ${vertex_label}
# _ *${postfix}
# echo
# incoming
# edge
# types: *_${vertex_label}${postfix}
#
# while read line; do
# # come up with a new id
# ((id + +))
# # replace the old id in the vertex file
# sed - i
# "s/$line|/$id|/" ${vertex_file}
# # replace the old source ids in the edge file
# for e in ${vertex_label}_ * ${postfix}; do
# sed - i
# "s/$line|/$id|/" $e
# done
# # replace the old target ids in the egde file
# for e in ${vertex_label}_ * ${postfix}; do
# sed - i
# "s/|$line|/|$id|/" $e
# done
# done < current_ids.txt
# done
