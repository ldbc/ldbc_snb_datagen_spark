#!/usr/bin/env python2

import sys
import discoverparams
import readfactors
import random
import json
import os
import codecs
import calendar
import time
from datetime import date,datetime,timedelta
from timeparameters import *
from calendar import timegm

START_DATE=datetime.strptime("2010-01-01", "%Y-%m-%d")
END_DATE=datetime.strptime("2013-01-01", "%Y-%m-%d")

def format_date(date):
   return int(time.mktime(date.timetuple())*1000)

# class ParamsWriter:
#    def __init__(self, name, num_params):
#       self.files = []
#       for i in range(0, num_params):
#          self.files.append(codecs.open("params/"+name+"."+str(i+1)+".params", "w",encoding="utf-8"))

#    def append(self, params):
#       for i, param in enumerate(params):
#          self.files[i].write(param+"\n")

class ParamsWriter:
   def __init__(self, outdir, name, param_names):
      self.file = codecs.open(outdir+"/"+name+"_param.txt", "w",encoding="utf-8")
      for i in range(0,len(param_names)):
         if i>0:
            self.file.write("|")
         self.file.write(param_names[i])
      self.file.write("\n")

   def append(self, params):
      for i, param in enumerate(params):
         if i>0:
            self.file.write("|")
         self.file.write(param)
      self.file.write("\n")


def post_date_right_open_range_params(sample, lower_bound, upper_bound):
   results = []
   for ix in range(0, len(sample)):
      start_offset = sample[ix][0]
      count_sum = 0
      for offset, count in sample[ix:]:
         count_sum += count
      if count_sum > lower_bound and count_sum < upper_bound:
         results.append([start_offset, count_sum])
   return results

def post_date_range_params(sample, lower_bound, upper_bound):
   results = []
   for ix in range(0, len(sample)):
      start_offset = sample[ix][0]
      count_sum = 0
      for offset, count in sample[ix:]:
         count_sum += count
         if count_sum > lower_bound and count_sum < upper_bound:
            results.append([[start_offset, offset], count_sum])
   return results

def post_month_params(sample, lower_bound, upper_bound):
   results = []
   for ix in range(0, len(sample)/4):
      start_ix = ix*4
      count_sum = 0
      for offset, count in sample[start_ix:start_ix+4]:
         count_sum += count
      if count_sum > lower_bound and count_sum < upper_bound:
         start_day = sample[start_ix][0]
         end_day = sample[start_ix+4][0]
         results.append([[start_day, end_day], count_sum])
   return results

# def post_three_month_params(sample, lower_bound, upper_bound):
#    results = []
#    for ix in range(0, len(sample)/12):
#       start_ix = ix*12
#       count_sum = 0
#       for offset, count in sample[start_ix:start_ix+12]:
#          count_sum += count
#       if count_sum > lower_bound and count_sum < upper_bound:
#          start_day = sample[start_ix][0]
#          end_day = sample[start_ix+12][0]
#          results.append([[start_day, end_day], count_sum])
#    return results

def key_params(sample, lower_bound, upper_bound):
   results = []
   for key, count in sample:
      if count > lower_bound and count < upper_bound:
         results.append([key, count])
   return results

def serializes_q1(outdir, post_weeks):
   writer = ParamsWriter(outdir, "q1", ["date"])
   for week, count in post_weeks:
      writer.append([str(week)])

def serializes_q2(outdir, countries, post_day_ranges):
   writer = ParamsWriter(outdir, "q2", ["date1", "date2", "country1", "country2"])
   for day_range, count_post in post_day_ranges:
      for ix in range(0,len(countries)):
         country_1, count_1 = countries[ix]
         for country_2, count_2 in countries[ix+1:]:
            writer.append([str(day_range[0]),str(day_range[1]),country_1,country_2])

def serializes_q3(outdir, post_months):
   writer = ParamsWriter(outdir, "q3", ["year", "month"] )
   # TODO year, month

def serializes_q4(outdir, tagclasses, countries):
   writer = ParamsWriter(outdir, "q4", ["tagClass", "country"])
   for tag, count_a in tagclasses:
      for country, count_b in countries:
         writer.append([tag,country])

def serializes_q5(outdir, countries):
   writer = ParamsWriter(outdir, "q5", ["country"])
   for country, count in countries:
      writer.append([country])


def serializes_q6(outdir, tags):
   writer = ParamsWriter(outdir, "q6", ["tag"])
   for tag, count in tags:
      writer.append([tag])

def serializes_q7(outdir, tags):
   writer = ParamsWriter(outdir, "q7", ["tag"])
   for tag, count in tags:
      writer.append([tag])

def serializes_q8(outdir, tags):
   writer = ParamsWriter(outdir, "q8", ["tag"])
   for tag, count in tags:
      writer.append([tag])

def serializes_q9(outdir, tagclasses):
   writer = ParamsWriter(outdir, "q9", ["tagClass1", "tagClass2", "threshold"])
   for ix in range(0,len(tagclasses)):
      tag_class_a, count_a = tagclasses[ix]
      for tag_class_b, count_b in tagclasses[ix+1:]:
         writer.append([tag_class_a, tag_class_b, str(200)])

def serializes_q10(outdir, tags, post_weeks):
   writer = ParamsWriter(outdir, "q10", ["tag", "date"])
   for tag, count in tags:
      for week, count in post_weeks:
         writer.append([tag, str(week)])

def serializes_q11(outdir, countries, bad_words):
   writer = ParamsWriter(outdir, "q11", ["country", "blacklist"])
   random.seed(1988+2)
   for country, count in countries:
      num_words = random.randint(1,min(len(bad_words),4));
      random.shuffle(bad_words)
      blacklist = bad_words[0:num_words]
      writer.append([country,";".join(blacklist)])

      num_words = random.randint(1,min(len(bad_words),10));
      random.shuffle(bad_words)
      blacklist = bad_words[0:num_words]
      writer.append([country,";".join(blacklist)])

      num_words = random.randint(1,min(len(bad_words),7));
      random.shuffle(bad_words)
      blacklist = bad_words[0:num_words]
      writer.append([country,";".join(blacklist)])

def serializes_q12(outdir, post_weeks):
   writer = ParamsWriter(outdir, "q12", ["date", "likeThreshold"])
   for week, count in post_weeks:
      writer.append([str(week),str(400)])

def serializes_q13(outdir, countries):
   writer = ParamsWriter(outdir, "q13", ["country"])
   for country, count in countries:
      writer.append([country])

def serializes_q14(outdir, creationdates):
   writer = ParamsWriter(outdir, "q14", ["begin", "end"])
   for creation, count in creationdates:
      writer.append([str(creation[0]),str(creation[1])])

def serializes_q15(outdir, countries):
   writer = ParamsWriter(outdir, "q15", ["country"])
   for country, count in countries:
      writer.append([country])

def serializes_q16(outdir, persons, tagclasses, countries):
   writer = ParamsWriter(outdir, "q16", ["person", "tag", "country", "minPathDistance", "maxPathDistance"])
   random.seed(1988+2)
   for tag, count_a in tagclasses:
      for country, count_b in countries:
         writer.append([str(persons[random.randint(0,len(persons))]), tag, country])
         # TODO minPathDistance and maxPathDistance are missing

def serializes_q17(outdir, countries):
   writer = ParamsWriter(outdir, "q17", ["country"])
   for country, count in countries:
      writer.append([country])

def serializes_q18(outdir, post_weeks):
   writer = ParamsWriter(outdir, "q18", ["date", "lengthThreshold", "languages"])
   for week, count in post_weeks:
      writer.append([str(week)])
      # TODO lengthThreshold and languages are missing

def serializes_q19(outdir, tagclasses):
   PERS_DATE=datetime.strptime("1989-1-1", "%Y-%m-%d")
   writer = ParamsWriter(outdir, "q19", ["date", "tagClass1", "tagClass2"])
   for ix in range(0,len(tagclasses)):
      tag_class_a, count_a = tagclasses[ix]
      for tag_class_b, count_b in tagclasses[ix+1:]:
         writer.append([str(format_date(PERS_DATE)),tag_class_a, tag_class_b])

def serializes_q20(outdir, tagclasses):
   writer = ParamsWriter(outdir, "q20", ["tagClasses"]) # TODO tagclasses
   for tagclass, count in tagclasses:
      writer.append([tagclass])

def serializes_q21(outdir, countries):
   writer = ParamsWriter(outdir, "q21", ["country", "endDate"])
   for country, count in countries:
      writer.append([country,str(format_date(END_DATE))])

def serializes_q22(outdir, countries):
   writer = ParamsWriter(outdir, "q22", ["country1", "country2"])
   for ix in range(0,len(countries)):
      country_a, count_a = countries[ix]
      for country_b, count_b in countries[ix+1:]:
         writer.append([country_a, country_b])

def serializes_q23(outdir, countries):
   writer = ParamsWriter(outdir, "q23", ["country"])
   for country, count in countries:
      writer.append([country])

def serializes_q24(outdir, tagclasses):
   writer = ParamsWriter(outdir, "q24", ["tagClass"])
   for tagclass, count in tagclasses:
      writer.append([tagclass])

def serializes_q25(outdir):
   writer = ParamsWriter(outdir, "q25", ["person1Id", "person2Id", "startDate", "endDate"])
   # TODO

def add_months(sourcedate,months):
   month = sourcedate.month - 1 + months
   year = int(sourcedate.year + month / 12 )
   month = month % 12 + 1
   day = min(sourcedate.day,calendar.monthrange(year,month)[1])
   return sourcedate.replace(year, month, day)

def convert_posts_histo(histogram):
   week_posts = []
   month = 0
   while (histogram.existParam(month)):
      monthTotal = histogram.getValue(month, "p")
      baseDate=add_months(START_DATE,month)
      week_posts.append([format_date(baseDate), monthTotal/4])
      week_posts.append([format_date(baseDate+timedelta(days=7)), monthTotal/4])
      week_posts.append([format_date(baseDate+timedelta(days=14)), monthTotal/4])
      week_posts.append([format_date(baseDate+timedelta(days=21)), monthTotal/4])
      month = month + 1
   return week_posts

def main(argv=None):
   if argv is None:
      argv = sys.argv

   if len(argv) < 3:
      print "arguments: <input dir> <output dir>"
      return 1

   indir = argv[1]+"/"
   outdir = argv[2]+"/"
   activityFactorFiles=[]
   personFactorFiles=[]
   friendsFiles = []

   for file in os.listdir(indir):
      if file.endswith("activityFactors.txt"):
         activityFactorFiles.append(indir+file)
      if file.endswith("personFactors.txt"):
         personFactorFiles.append(indir+file)
      if file.startswith("m0friendList"):
         friendsFiles.append(indir+file)

   # read precomputed counts from files   
   (personFactors, countryFactors, tagFactors, tagClassFactors, nameFactors, givenNames,  ts, postsHisto) = \
      readfactors.load(personFactorFiles,activityFactorFiles, friendsFiles)
   week_posts = convert_posts_histo(postsHisto)

   persons = []
   for key, _ in personFactors.values.iteritems():
      persons.append(key)
   random.seed(1988)
   random.shuffle(persons)

   country_sample = []
   for key, value in countryFactors.values.iteritems():
      country_sample.append([key, value.getValue("p")])
   country_sample.sort(key=lambda x: x[1], reverse=True)

   tagclass_posts = tagClassFactors
   tagclass_posts.sort(key=lambda x: x[1], reverse=True)

   tag_posts = tagFactors
   tag_posts.sort(key=lambda x: x[1], reverse=True)

   total_posts = 0
   for day, count in tag_posts:
      total_posts += count

   person_sum = 0
   for country, count in country_sample:
      person_sum += count

   post_lower_threshold = 0.1*total_posts*0.9
   post_upper_threshold = 0.1*total_posts*1.1
   post_day_ranges = post_date_range_params(week_posts, post_lower_threshold, post_upper_threshold)
   
   bad_words = ['Augustine','William','James','with','Henry','Robert','from','Pope','Hippo','album','David','has','one','also','Green','which','that']
   #post_lower_threshold = (total_posts/(week_posts[len(week_posts)-1][0]/7/4))*0.8
   #post_upper_threshold = (total_posts/(week_posts[len(week_posts)-1][0]/7/4))*1.2
   non_empty_weeks=len(week_posts)
   for ix in range(0,len(week_posts)):
      if week_posts[ix][1]==0:
         non_empty_weeks-= 1

   post_lower_threshold = (total_posts/(non_empty_weeks/4))*0.8
   post_upper_threshold = (total_posts/(non_empty_weeks/4))*1.2
   post_months = post_month_params(week_posts, post_lower_threshold, post_upper_threshold)

   serializes_q2 (outdir, key_params(country_sample, total_posts/200, total_posts/100), post_day_ranges) # TODO determine constants
   serializes_q3 (outdir, post_months)
   serializes_q14(outdir, post_month_params(week_posts, post_lower_threshold*2, post_upper_threshold*2))

   serializes_q1 (outdir, post_date_right_open_range_params(week_posts, 0.3*total_posts, 0.6*total_posts))
   serializes_q12(outdir, post_date_right_open_range_params(week_posts, 0.3*total_posts, 0.6*total_posts))
   serializes_q18(outdir, post_date_right_open_range_params(week_posts, 0.3*total_posts, 0.6*total_posts))
   serializes_q10(outdir, key_params(tag_posts, total_posts/900, total_posts/600), post_date_right_open_range_params(week_posts, 0.3*total_posts, 0.6*total_posts))

   serializes_q4 (outdir, key_params(tagclass_posts, total_posts/20, total_posts/10), key_params(country_sample, total_posts/120, total_posts/70))
   serializes_q5 (outdir, key_params(country_sample, total_posts/200, total_posts/100))
   serializes_q6 (outdir, key_params(tag_posts, total_posts/1300, total_posts/900))
   serializes_q7 (outdir, key_params(tag_posts, total_posts/900, total_posts/600))
   serializes_q8 (outdir, key_params(tag_posts, total_posts/600, total_posts/300))
   serializes_q9 (outdir, key_params(tagclass_posts, 6000, 25000))
   serializes_q13(outdir, key_params(country_sample, total_posts/200, total_posts/100))
   serializes_q15(outdir, key_params(country_sample, total_posts/200, total_posts/100))
   serializes_q16(outdir, persons, key_params(tagclass_posts, total_posts/30, total_posts/10), key_params(country_sample, total_posts/80, total_posts/20))
   serializes_q17(outdir, key_params(country_sample, total_posts/200, total_posts/100))
   serializes_q19(outdir, key_params(tagclass_posts, total_posts/60, total_posts/10))
   serializes_q21(outdir, key_params(country_sample, total_posts/200, total_posts/100))
   serializes_q22(outdir, key_params(country_sample, total_posts/120, total_posts/40))
   serializes_q23(outdir, key_params(country_sample, total_posts/200, total_posts/100))
   serializes_q24(outdir, key_params(tagclass_posts, total_posts/140, total_posts/5))

   # TODO: Refine
   serializes_q20(outdir, key_params(tagclass_posts, total_posts/20, total_posts/2))
   serializes_q11(outdir, key_params(country_sample, total_posts/80, total_posts/20), bad_words)

   # TODO: implement
   #serializes_q25(outdir, ...)

if __name__ == "__main__":
   sys.exit(main())
