# Factor files

## Activity factors

The `*activityFactors.txt` file consists of 5 sections. The first 4 contains Countries, TagClasses, Tags, and first names. These are preceded by a line with a single number _k_ defining how many entries are next. The next _k_ lines contain those entries.

The last 4 lines in the file are numbers representing years/months:

```
startMonth
startYear
minWorkFrom
maxWorkFrom
```

The TagClass factors had redundant information as they contained lines such as `$tagclass,$tagclass,number`. This has been fixed in commit `8479292185eaaf16c0228e261efd47e5a3433643`.

## Person factors

The `*personFactors.txt` file consists of lines with 83 fields. A line expanded looks like this:

```py
26388279072769, # id                  [1]
John,           # name                [2]
123,            # numFriends          [3]
54,             # numPosts            [4]
158,            # numLikes            [5]
463,            # numTagsOfMessages   [6]
444,            # numForums           [7]
1,              # numWorkPlaces       [8]
273,            # numComments         [9]
0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,13,35,54,45,48,43,61,28,0,0,0,  # numMessagesPerMonth (37 entries) [10-46]
0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,209,20,30,28,34,30,38,42,13,0,0,0 # numForumsPerMonth (37 entries)   [47-83]
```
