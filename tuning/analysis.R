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
ss = 1262304000 # simulation start 
se = 1356998400 # simulation end 
bl = as_datetime(ss + ((se-ss)*0.9)) # current bulk load threshold
new_bl =  as_datetime("2012-09-04") # proposed bulk load date threshold
n_batches = (as_datetime(se) - new_bl) / as.duration(weeks(1)) # 17 week-sized batches 
new_pc = (se-1346716800)/(se-ss) # new bulk load percentage

n_inserts = post %>% filter(creationDate > new_bl) %>% count() # no. inserts: 29094
n_deletes = post %>% filter(deletionDate > new_bl) %>% filter(explicitlyDeleted == T) %>% count() # no. deletes: 261

n_insert_delete = post %>% filter(creationDate > new_bl) %>% filter(explicitlyDeleted == T) %>% count() # no. inserted and deleted: 118
n_insert_not_delete = post %>% filter(creationDate > new_bl) %>% filter(explicitlyDeleted == F) %>% count() # no. inserted only: 28976
n_not_insert_delete = post %>% filter(creationDate <= new_bl && deletionDate > new_bl) %>% filter(explicitlyDeleted == T) %>% count()  # no. deleted only: 0

# operation count per batch 
op_types = c("Insert", "Delete")
i = post %>% filter(creationDate > new_bl) %>% select(creationDate) %>% rename(timestamp = creationDate) %>% add_column(op_type="Insert") %>% mutate(op_type = factor(op_type,levels = op_types)) 
d = post %>% filter(deletionDate > new_bl) %>% filter(explicitlyDeleted == T) %>% select(deletionDate) %>% rename(timestamp = deletionDate) %>% add_column(op_type="Delete") %>% mutate(op_type = factor(op_type,levels = op_types))
o = bind_rows(i,d) %>% arrange(timestamp)
 
rb_i = rep(0,17)
rb_d = rep(0,17)
for (i in 1:17) {
  rb = o %>% filter(timestamp >= new_bl + as.duration(weeks(i-1)) & timestamp < new_bl + as.duration(weeks(i)))
  total = as.numeric(rb %>% count())
  rb_i[i]  = as.numeric(rb %>% filter(op_type == "Insert") %>% count())
  rb_d[i] = total - rb_i[i]
}

rb_i/(rb_i + rb_d)


# plots
new_posts_per_day = post %>% filter(creationDate > new_bl) %>% mutate(creationDate = date(creationDate)) %>% count(creationDate)
deleted_posts_per_day = post %>% filter(explicitlyDeleted == T, deletionDate > new_bl) %>% mutate(deletionDate = date(deletionDate)) %>% count(deletionDate)

ggplot(new_posts_per_day , aes(x = creationDate, y=n)) + geom_line() 
ggplot(deleted_posts_per_day , aes(x = deletionDate, y=n)) + geom_line()

(p_hist <- ggplot(posts_per_day, aes(x=n)) + geom_histogram())


# operations_per_type = updates %>% count(type) %>% mutate(type = types)
# operations_per_day = updates %>% mutate(timestamp = date(timestamp)) %>% group_by(type) %>% count(timestamp) %>% do({
#    p <- ggplot(., aes(x = timestamp, y = n)) + geom_line()
#    ggsave(p, filename = paste0("./figs/ops_per_day_type_", unique(.$type),  ".pdf"), width = 4, height = 4, units = "in")
#    invisible(.)
#    })
# TODO: add labels and some colour to plots
