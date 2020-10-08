# libraries
library(tidyverse)
library(lubridate)

# insert operation type mapping
# 1 - PERSON
# 2 - LIKE_POST
# 3 - LIKE_COMMENT
# 4 - FORUM
# 6 - POST
# 7 - COMMENT
# 8 - FRIENDSHIP
# TODO: delete operation type mapping

#### load data ####
dg_home = Sys.getenv("LDBC_SNB_DATAGEN_HOME")
person = paste0(dg_home,"/social_network/updateStream_0_0_person_trimmed.csv")
activity = paste0(dg_home,"/social_network/updateStream_0_0_forum_trimmed.csv")
person = read_delim(person,"|",col_names = c("timestamp","type"))
activity = read_delim(activity,"|",col_names = c("timestamp","type"))
# TODO: load all csv files 

#### preprocess ####
person = person %>% mutate(timestamp = as_datetime(timestamp/1000))
activity = activity %>% mutate(timestamp = as_datetime(timestamp/1000))
updates = bind_rows(person, activity)
types = c("PERSON","LIKE_POST","LIKE_COMMENT","FORUM","FORUM_MEMBERSHIP","POST","COMMENT","FRIENDSHIP")
types = tolower(types)
# TODO: sort by timestamp and group in batches  

#### analysis ####
operations_per_type = updates %>% count(type) %>% mutate(type = types)
operations_per_day = updates %>% mutate(timestamp = date(timestamp)) %>% group_by(type) %>% count(timestamp) %>% do({
   p <- ggplot(., aes(x = timestamp, y = n)) + geom_line()
   ggsave(p, filename = paste0("./figs/ops_per_day_type_", unique(.$type),  ".pdf"), width = 4, height = 4, units = "in")
   invisible(.)
   })
# TODO: add labels and some colour to plots
# TODO: operation counts per various batch sizes 
# TODO: ratio of inserts to deletes in each batch 
