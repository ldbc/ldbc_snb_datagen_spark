import sys
import discoverparams
import readfactors
import random
import json
import os
import codecs
from datetime import date
from timeparameters import *
from calendar import timegm


# class ParamsWriter:
#    def __init__(self, name, num_params):
#       self.files = []
#       for i in range(0, num_params):
#          self.files.append(codecs.open("params/"+name+"."+str(i+1)+".params", "w",encoding="utf-8"))

#    def append(self, params, counts):
#       for i, param in enumerate(params):
#          self.files[i].write(param+"\n")

class ParamsWriter:
   def __init__(self, name, num_params):
      self.file = codecs.open("substitution_parameters/"+name+"_param.txt", "w",encoding="utf-8")
      for i in range(0,num_params):
         if i>0:
            self.file.write("|")
         self.file.write("Param"+str(i))
      self.file.write("\n")

   def append(self, params, counts):
      for i, param in enumerate(params):
         if i>0:
            self.file.write("|")
         self.file.write(param)
      self.file.write("\n")


def country_sets_params(sample, lower_bound, upper_bound, max_depth, start = 0):
   if max_depth == 0:
      return []

   results = []
   ix = start
   for country, count in sample[start:]:
      if count < (lower_bound / (max_depth + 1)):
         continue
      if count < lower_bound:
         others = country_sets_params(sample, lower_bound-count, upper_bound-count, max_depth - 1, ix + 1)
         for other_countries, other_count in others:
            combined_count = count + other_count
            if combined_count > lower_bound and combined_count < upper_bound:
               other_countries.append(country)
               results.append([other_countries, combined_count])
      if count > lower_bound and count < upper_bound:
         results.append([[country], count])
      ix = ix + 1
   return results

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

def post_three_month_params(sample, lower_bound, upper_bound):
   results = []
   for ix in range(0, len(sample)/12):
      start_ix = ix*12
      count_sum = 0
      for offset, count in sample[start_ix:start_ix+12]:
         count_sum += count
      if count_sum > lower_bound and count_sum < upper_bound:
         start_day = sample[start_ix][0]
         end_day = sample[start_ix+12][0]
         results.append([[start_day, end_day], count_sum])
   return results


def key_params(sample, lower_bound, upper_bound):
   results = []
   for key, count in sample:
      if count > lower_bound and count < upper_bound:
         results.append([key, count])
   return results

def serialize_q1(post_weeks):
   f1 = open('params/q1.1.params', 'w+')
   fcounts = open('params/q1.counts.params', 'w+')
   for week, count in post_weeks:
      f1.write(str(week)+"\n")
      fcounts.write(str(count)+"\n")

def serialize_q2(country_sets, post_day_ranges):
   # Generate Q2 params
   f1 = open('params/q2.1.params', 'w+')
   f2 = open('params/q2.2.params', 'w+')
   f3 = open('params/q2.3.params', 'w+')
   fcounts = open('params/q2.counts.params', 'w+')
   random.seed(1988+2)
   for country_set, count_country in country_sets:
      for day_range, count_post in post_day_ranges:
         if random.randint(0,len(country_sets) + len(post_day_ranges)) == 0:
            f1.write(str(day_range[0])+"\n")
            f2.write(str(day_range[1])+"\n")
            f3.write("ctry_name = '"+"' or ctry_name = '".join(country_set)+"'\n")
            fcounts.write(str(count_post)+"|"+str(count_country)+"\n")

def serialize_q3(post_months):
   # Generate Q2 params
   f1 = open('params/q3.1.params', 'w+')
   f2 = open('params/q3.2.params', 'w+')
   fcounts = open('params/q3.counts.params', 'w+')
   for ix in range(0,len(post_months)):
      week_range_a, count_a = post_months[ix]
      for week_range_b, count_b in post_months[ix+1:]:
         f1.write(str(week_range_a[0])+"\n")
         f2.write(str(week_range_b[0])+"\n")
         fcounts.write(str(count_a)+"|"+str(count_b)+"\n")

def serialize_q4(tagclasses, countries):
   writer = ParamsWriter("q4", 2)
   for tag, count_a in tagclasses:
      for country, count_b in countries:
         writer.append([tag,country], [count_a,count_b])

def serialize_q5(countries):
   writer = ParamsWriter("q5", 1)
   for country, count in countries:
      writer.append([country], [count])


def serialize_q6(tags):
   writer = ParamsWriter("q6", 1)
   for tag, count in tags:
      writer.append([tag], [count])

def serialize_q7(tags):
   writer = ParamsWriter("q7", 1)
   for tag, count in tags:
      writer.append([tag], [count])

def serialize_q8(tags):
   writer = ParamsWriter("q8", 1)
   for tag, count in tags:
      writer.append([tag], [count])

def serialize_q9(tagclasses):
   writer = ParamsWriter("q9", 2)
   for ix in range(0,len(tagclasses)):
      tag_class_a, count_a = tagclasses[ix]
      for tag_class_b, count_b in tagclasses[ix+1:]:
         writer.append([tag_class_a, tag_class_b], [count_a, count_b])

def serialize_q10(tags):
   writer = ParamsWriter("q10", 1)
   for tag, count in tags:
      writer.append([tag], [count])

def serialize_q12(post_weeks):
   f1 = open('params/q12.1.params', 'w+')
   fcounts = open('params/q12.counts.params', 'w+')
   for week, count in post_weeks:
      f1.write(str(week)+"\n")
      fcounts.write(str(count)+"\n")

def serialize_q13(countries):
   writer = ParamsWriter("q13", 1)
   for country, count in countries:
      writer.append([country], [count])

def serialize_q14(creationdates):
   f1 = open('params/q14.1.params', 'w+')
   fcounts = open('params/q14.counts.params', 'w+')
   for creation, count in creationdates:
      f1.write(str(creation[0])+"\n")
      fcounts.write(str(count)+"\n")

def serialize_q15(countries):
   writer = ParamsWriter("q15", 1)
   for country, count in countries:
      writer.append([country], [count])

def serialize_q16(tagclasses, countries):
   writer = ParamsWriter("q16", 2)
   for tag, count_a in tagclasses:
      for country, count_b in countries:
         writer.append([tag, country], [count_a, count_b])

def serialize_q17(countries):
   writer = ParamsWriter("q17", 1)
   for country, count in countries:
      writer.append([country], [count])

def serialize_q18(post_weeks):
   f1 = open('params/q18.1.params', 'w+')
   fcounts = open('params/q18.counts.params', 'w+')
   for week, count in post_weeks:
      f1.write(str(week)+"\n")

def serialize_q19(tagclasses):
   writer = ParamsWriter("q19", 2)
   for ix in range(0,len(tagclasses)):
      tag_class_a, count_a = tagclasses[ix]
      for tag_class_b, count_b in tagclasses[ix+1:]:
         writer.append([tag_class_a, tag_class_b], [count_a, count_b])

def serialize_q21(countries):
   writer = ParamsWriter("q21", 1)
   for country, count in countries:
      writer.append([country], [count])

def serialize_q22(countries):
   writer = ParamsWriter("q22", 2)
   for ix in range(0,len(countries)):
      country_a, count_a = countries[ix]
      for country_b, count_b in countries[ix+1:]:
         writer.append([country_a, country_b], [count_a, count_b])

def serialize_q23(countries):
   writer = ParamsWriter("q23", 1)
   for country, count in countries:
      writer.append([country], [count])

def serialize_q24(tagclasses):
   writer = ParamsWriter("q24", 1)
   for tagclass, count in tagclasses:
      writer.append([tagclass], [count])

def main(argv=None):
   if argv is None:
      argv = sys.argv

   if len(argv) < 3:
      print "arguments: <input dir> <output>"
      return 1

   indir = argv[1]+"/"
   factorFiles=[]
   friendsFiles = []
   outdir = argv[2]+"/"

   for file in os.listdir(indir):
      if file.endswith("factors.txt"):
         factorFiles.append(indir+file)
      if file.startswith("m0friendList"):
         friendsFiles.append(indir+file)

   # read precomputed counts from files   
   (personFactors, countryFactors, tagFactors, tagClassFactors, nameFactors, givenNames,  ts) = readfactors.load(factorFiles, friendsFiles)

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
   
   serialize_q4(key_params(tagclass_posts, total_posts/20, total_posts/10), key_params(country_sample, total_posts/120, total_posts/70))
   serialize_q5(key_params(country_sample, total_posts/200, total_posts/100))
   serialize_q6(key_params(tag_posts, total_posts/1300, total_posts/900))
   serialize_q7(key_params(tag_posts, total_posts/900, total_posts/600))
   serialize_q8(key_params(tag_posts, total_posts/600, total_posts/300))
   serialize_q9(key_params(tagclass_posts, 6000, 25000))
   serialize_q10(key_params(tag_posts, total_posts/900, total_posts/600))
   # serialize_q12(post_date_right_open_range_params(week_posts, 0.3*total_posts, 0.6*total_posts))
   serialize_q13(key_params(country_sample, total_posts/200, total_posts/100))
   # serialize_q14(post_month_params(week_posts, post_lower_threshold*2, post_upper_threshold*2))
   serialize_q15(key_params(country_sample, total_posts/200, total_posts/100))
   serialize_q16(key_params(tagclass_posts, total_posts/30, total_posts/10), key_params(country_sample, total_posts/110, total_posts/70))
   serialize_q17(key_params(country_sample, total_posts/200, total_posts/100))
   # serialize_q18(post_date_right_open_range_params(week_posts, 0.3*total_posts, 0.6*total_posts))
   serialize_q19(key_params(tagclass_posts, total_posts/60, total_posts/10))
   serialize_q21(key_params(country_sample, total_posts/200, total_posts/100))
   serialize_q22(key_params(country_sample, total_posts/120, total_posts/40))
   serialize_q23(key_params(country_sample, total_posts/200, total_posts/100))
   serialize_q24(key_params(tagclass_posts, total_posts/140, total_posts/5))

if __name__ == "__main__":
   sys.exit(main())
