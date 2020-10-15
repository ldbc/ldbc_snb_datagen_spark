# libraries
library(tidyverse)
library(lubridate)


#### load data ####
ldbc = paste0(Sys.getenv("LDBC_SNB_DATAGEN_HOME"),"/out/social_network/dynamic/")
ldbc = "/Users/jackwaudby/Documents/ldbc/ldbc_snb_datagen/out/social_network/dynamic/"
post = paste0(ldbc,"post_0_0_trimmed.csv")
post = read_delim(post,"|")

#### preprocess ####
types = c("PERSON","LIKE_POST","LIKE_COMMENT","FORUM","FORUM_MEMBERSHIP","POST","COMMENT","FRIENDSHIP")
types = tolower(types)


#### analysis ####
ss = 1262304000
se = 1356998400
bl = as_datetime(ss + ((se-ss)*0.9))
new_bl = as_datetime(se) - days(7*16) # possible better bulk load date
weeks = (as_datetime(se) - new_bl) / as.duration(weeks(1))

## inserts 
post %>% filter(creationDate > new_bl) %>% count() 
insert_posts_per_day = post %>% filter(creationDate > new_bl) %>% mutate(creationDate = date(creationDate)) %>% count(creationDate)
p_line <- ggplot(insert_posts_per_day , aes(x = creationDate, y=n)) + geom_line() 

## deletes 
# number of explicit deletes across simulation period
post %>% count(explicitlyDeleted) 
# number of explicit deletes after bulk load threshold
post %>% filter(explicitlyDeleted == T, deletionDate > new_bl)
# number of explicit deletes after bulk load threshold per day
posts_per_day = post %>% filter(explicitlyDeleted == T, deletionDate > new_bl) %>% mutate(deletionDate = date(deletionDate)) %>% count(deletionDate)

p_line <- ggplot(posts_per_day , aes(x = deletionDate, y=n)) + geom_line()

(p_hist <- ggplot(posts_per_day, aes(x=n)) + geom_histogram())


# operations_per_type = updates %>% count(type) %>% mutate(type = types)
# operations_per_day = updates %>% mutate(timestamp = date(timestamp)) %>% group_by(type) %>% count(timestamp) %>% do({
#    p <- ggplot(., aes(x = timestamp, y = n)) + geom_line()
#    ggsave(p, filename = paste0("./figs/ops_per_day_type_", unique(.$type),  ".pdf"), width = 4, height = 4, units = "in")
#    invisible(.)
#    })
# TODO: add labels and some colour to plots
# TODO: operation counts per various batch sizes 
# TODO: ratio of inserts to deletes in each batch 
