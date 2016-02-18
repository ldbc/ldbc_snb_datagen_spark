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

START_DATE=datetime.strptime("2010-01-01","%Y-%m-%d")
END_DATE=datetime.strptime("2013-01-01","%Y-%m-%d")

def format_date(date):
   return int(time.mktime(date.timetuple())*1000)

# class ParamsWriter:
#    def __init__(self, name, num_params):
#       self.files = []
#       for i in range(0, num_params):
#          self.files.append(codecs.open("params/"+name+"."+str(i+1)+".params", "w",encoding="utf-8"))

#    def append(self, params, counts):
#       for i, param in enumerate(params):
#          self.files[i].write(param+"\n")

class ParamsWriter:
   def __init__(self, name, param_names):
      self.file = codecs.open("substitution_parameters/"+name+"_param.txt", "w",encoding="utf-8")
      for i in range(0,len(param_names)):
         if i>0:
            self.file.write("|")
         self.file.write(param_names[i])
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

def serialize_q1(post_weeks):
   writer = ParamsWriter("q1", ["date"])
   for week, count in post_weeks:
      writer.append([str(week)], [count])

def serialize_q2(country_sets, post_day_ranges):
   writer = ParamsWriter("q2", ["date1","date2","countries","endDate","messageThreshold"])
   random.seed(1988+2)
   for country_set, count_country in country_sets:
      for day_range, count_post in post_day_ranges:
         if random.randint(0,len(country_sets) + len(post_day_ranges)) == 0:
            writer.append([str(day_range[0]), str(day_range[1]), ";".join(country_set), str(format_date(END_DATE)),str(20)], [count_post,count_post,count_country,333])

def serialize_q3(post_months):
   writer = ParamsWriter("q3", ["range1Start","range1End","range2Start","range2End"])
   for ix in range(0,len(post_months)):
      week_range_a, count_a = post_months[ix]
      for week_range_b, count_b in post_months[ix+1:]:
         writer.append([str(week_range_a[0]),str(week_range_a[1]),str(week_range_b[0]),str(week_range_b[1])], [count_a,count_b])

def serialize_q4(tagclasses, countries):
   writer = ParamsWriter("q4", ["tagClass","country"])
   for tag, count_a in tagclasses:
      for country, count_b in countries:
         writer.append([tag,country], [count_a,count_b])

def serialize_q5(countries):
   writer = ParamsWriter("q5", ["country"])
   for country, count in countries:
      writer.append([country], [count])


def serialize_q6(tags):
   writer = ParamsWriter("q6", ["tag"])
   for tag, count in tags:
      writer.append([tag], [count])

def serialize_q7(tags):
   writer = ParamsWriter("q7", ["tag"])
   for tag, count in tags:
      writer.append([tag], [count])

def serialize_q8(tags):
   writer = ParamsWriter("q8", ["tag"])
   for tag, count in tags:
      writer.append([tag], [count])

def serialize_q9(tagclasses):
   writer = ParamsWriter("q9", ["tagClass1", "tagClass2", "threshold"])
   for ix in range(0,len(tagclasses)):
      tag_class_a, count_a = tagclasses[ix]
      for tag_class_b, count_b in tagclasses[ix+1:]:
         writer.append([tag_class_a, tag_class_b, str(200)], [count_a, count_b])

def serialize_q10(tags):
   writer = ParamsWriter("q10", ["tag"])
   for tag, count in tags:
      writer.append([tag], [count])

def serialize_q11(countries, bad_words):
   writer = ParamsWriter("q11", ["country", "blacklist"])
   random.seed(1988+2)
   for country, count in countries:
      num_words = random.randint(1,min(len(bad_words),4));
      random.shuffle(bad_words)
      blacklist = bad_words[0:num_words]
      writer.append([country,";".join(blacklist)], [count])

      num_words = random.randint(1,min(len(bad_words),10));
      random.shuffle(bad_words)
      blacklist = bad_words[0:num_words]
      writer.append([country,";".join(blacklist)], [count])

      num_words = random.randint(1,min(len(bad_words),7));
      random.shuffle(bad_words)
      blacklist = bad_words[0:num_words]
      writer.append([country,";".join(blacklist)], [count])

def serialize_q12(post_weeks):
   writer = ParamsWriter("q12", ["creationDate", "likeCount"])
   for week, count in post_weeks:
      writer.append([str(week),str(400)], [count])

def serialize_q13(countries):
   writer = ParamsWriter("q13", ["country"])
   for country, count in countries:
      writer.append([country], [count])

def serialize_q14(creationdates):
   writer = ParamsWriter("q14", ["begin","end"])
   for creation, count in creationdates:
      writer.append([str(creation[0]),str(creation[1])], [count])

def serialize_q15(countries):
   writer = ParamsWriter("q15", ["country"])
   for country, count in countries:
      writer.append([country], [count])

def serialize_q16(persons, tagclasses, countries):
   writer = ParamsWriter("q16", ["person","tag","country"])
   random.seed(1988+2)
   for tag, count_a in tagclasses:
      for country, count_b in countries:
         writer.append([str(persons[random.randint(0,len(persons))]), tag, country], [0, count_a, count_b])

def serialize_q17(countries):
   writer = ParamsWriter("q17", ["country"])
   for country, count in countries:
      writer.append([country], [count])

def serialize_q18(post_weeks):
   writer = ParamsWriter("q18", ["creationDate"])
   for week, count in post_weeks:
      writer.append([str(week)], [count])

def serialize_q19(tagclasses):
   PERS_DATE=datetime.strptime("1989-1-1","%Y-%m-%d")
   writer = ParamsWriter("q19", ["date","tagClass1","tagClass2"])
   for ix in range(0,len(tagclasses)):
      tag_class_a, count_a = tagclasses[ix]
      for tag_class_b, count_b in tagclasses[ix+1:]:
         writer.append([str(format_date(PERS_DATE)),tag_class_a, tag_class_b], [count_a, count_b])

def serialize_q20(tagclasses):
   writer = ParamsWriter("q20", ["tagclass"])
   for tagclass, count in tagclasses:
      writer.append([tagclass], [count])

def serialize_q21(countries):
   writer = ParamsWriter("q21", ["country","endDate"])
   for country, count in countries:
      writer.append([country,str(format_date(END_DATE))], [count])

def serialize_q22(countries):
   writer = ParamsWriter("q22", ["country1","country2"])
   for ix in range(0,len(countries)):
      country_a, count_a = countries[ix]
      for country_b, count_b in countries[ix+1:]:
         writer.append([country_a, country_b], [count_a, count_b])

def serialize_q23(countries):
   writer = ParamsWriter("q23", ["country"])
   for country, count in countries:
      writer.append([country], [count])

def serialize_q24(tagclasses):
   writer = ParamsWriter("q24", ["tagClass"])
   for tagclass, count in tagclasses:
      writer.append([tagclass], [count])

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
      print "arguments: <input dir> <output>"
      return 1

   indir = argv[1]+"/"
   activityFactorFiles=[]
   personFactorFiles=[]
   friendsFiles = []
   outdir = argv[2]+"/"

   for file in os.listdir(indir):
      if file.endswith("activityFactors.txt"):
         activityFactorFiles.append(indir+file)
      if file.endswith("personFactors.txt"):
         personFactorFiles.append(indir+file)
      if file.startswith("m0friendList"):
         friendsFiles.append(indir+file)

   # read precomputed counts from files   
   (personFactors, countryFactors, tagFactors, tagClassFactors, nameFactors, givenNames,  ts, postsHisto) = readfactors.load(personFactorFiles,activityFactorFiles, friendsFiles)
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

   country_lower_threshold = 0.1*total_posts*0.9
   country_upper_threshold = 0.1*total_posts*1.1
   country_sets = country_sets_params(country_sample, country_lower_threshold, country_upper_threshold, 4)

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

   serialize_q2(country_sets, post_day_ranges)
   serialize_q3(post_months)
   serialize_q14(post_month_params(week_posts, post_lower_threshold*2, post_upper_threshold*2))

   serialize_q1(post_date_right_open_range_params(week_posts, 0.3*total_posts, 0.6*total_posts))
   serialize_q12(post_date_right_open_range_params(week_posts, 0.3*total_posts, 0.6*total_posts))
   serialize_q18(post_date_right_open_range_params(week_posts, 0.3*total_posts, 0.6*total_posts))

   serialize_q4(key_params(tagclass_posts, total_posts/20, total_posts/10), key_params(country_sample, total_posts/120, total_posts/70))
   serialize_q5(key_params(country_sample, total_posts/200, total_posts/100))
   serialize_q6(key_params(tag_posts, total_posts/1300, total_posts/900))
   serialize_q7(key_params(tag_posts, total_posts/900, total_posts/600))
   serialize_q8(key_params(tag_posts, total_posts/600, total_posts/300))
   serialize_q9(key_params(tagclass_posts, 6000, 25000))
   serialize_q10(key_params(tag_posts, total_posts/900, total_posts/600))
   serialize_q13(key_params(country_sample, total_posts/200, total_posts/100))
   serialize_q15(key_params(country_sample, total_posts/200, total_posts/100))
   serialize_q16(persons, key_params(tagclass_posts, total_posts/30, total_posts/10), key_params(country_sample, total_posts/80, total_posts/20))
   serialize_q17(key_params(country_sample, total_posts/200, total_posts/100))
   serialize_q19(key_params(tagclass_posts, total_posts/60, total_posts/10))
   serialize_q21(key_params(country_sample, total_posts/200, total_posts/100))
   serialize_q22(key_params(country_sample, total_posts/120, total_posts/40))
   serialize_q23(key_params(country_sample, total_posts/200, total_posts/100))
   serialize_q24(key_params(tagclass_posts, total_posts/140, total_posts/5))

   # TODO: Refine
   serialize_q20(key_params(tagclass_posts, total_posts/20, total_posts/2))
   serialize_q11(key_params(country_sample, total_posts/80, total_posts/20), bad_words)

if __name__ == "__main__":
   sys.exit(main())
