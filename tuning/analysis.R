# libraries
suppressMessages(library(tidyverse))
suppressMessages(library(lubridate))

options(digits=4)

#### load data ####
cat("loading data...\n")
ldbc = paste0(Sys.getenv("LDBC_SNB_DATAGEN_HOME"),"/out/social_network/dynamic/")
# ldbc = "/Users/jackwaudby/Documents/ldbc/ldbc_snb_datagen/out/social_network/dynamic/"
person = paste0(ldbc,"person_0_0_trimmed.csv")
forum = paste0(ldbc,"forum_0_0_trimmed.csv")
post = paste0(ldbc,"post_0_0_trimmed.csv")
comment = paste0(ldbc,"comment_0_0_trimmed.csv")
person_likes_post = paste0(ldbc,"person_likes_post_0_0_trimmed.csv")
person_likes_comment = paste0(ldbc,"person_likes_comment_0_0_trimmed.csv")
forum_hasMember_person = paste0(ldbc,"forum_hasMember_person_0_0_trimmed.csv")
person_knows_person = paste0(ldbc,"person_knows_person_0_0_trimmed.csv")

person = suppressMessages(read_delim(person,"|"))
forum = suppressMessages(read_delim(forum,"|"))
post = suppressMessages(read_delim(post,"|"))
comment = suppressMessages(read_delim(comment,"|"))
person_likes_post = suppressMessages(read_delim(person_likes_post,"|"))
person_likes_comment = suppressMessages(read_delim(person_likes_comment,"|"))
forum_hasMember_person = suppressMessages(read_delim(forum_hasMember_person,"|"))
person_knows_person = suppressMessages(read_delim(person_knows_person,"|"))
cat("data loaded!\n")

#### preprocess ####
op_types = c("Insert_Person","Insert_Like_Post","Insert_Like_Comment","Insert_Forum",
          "Insert_Forum_Membership","Insert_Post","Insert_Comment","Insert_Friendship",
          "Delete_Person","Delete_Like_Post","Delete_Like_Comment","Delete_Forum",
          "Delete_Forum_Membership","Delete_Post","Delete_Comment","Delete_Friendship")

#### analysis ####
# ss = 1262304000 # simulation start 
# se = 1356998400 # simulation end 
# bl = as_datetime(ss + ((se-ss)*0.9)) # current bulk load threshold
# new_bl =  as_datetime("2012-09-04") # proposed bulk load date threshold
# n_batches = (as_datetime(se) - new_bl) / as.duration(weeks(1)) # 17 week-sized batches 
# new_pc = (se-1346716800)/(se-ss) # new bulk load percentage
# 
# n_inserts = post %>% filter(creationDate > new_bl) %>% count() # no. inserts: 29094
# n_deletes = post %>% filter(deletionDate > new_bl) %>% filter(explicitlyDeleted == T) %>% count() # no. deletes: 261
# 
# n_insert_delete = post %>% filter(creationDate > new_bl) %>% filter(explicitlyDeleted == T) %>% count() # no. inserted and deleted: 118
# n_insert_not_delete = post %>% filter(creationDate > new_bl) %>% filter(explicitlyDeleted == F) %>% count() # no. inserted only: 28976
# n_not_insert_delete = post %>% filter(creationDate <= new_bl && deletionDate > new_bl) %>% filter(explicitlyDeleted == T) %>% count()  # no. deleted only: 0

# operation count per batch 
# op_types = c("Insert", "Delete")
# i = post %>% filter(creationDate > new_bl) %>% select(creationDate) %>% rename(timestamp = creationDate) %>% add_column(op_type="Insert") %>% mutate(op_type = factor(op_type,levels = op_types)) 
# d = post %>% filter(deletionDate > new_bl) %>% filter(explicitlyDeleted == T) %>% select(deletionDate) %>% rename(timestamp = deletionDate) %>% add_column(op_type="Delete") %>% mutate(op_type = factor(op_type,levels = op_types))
# o = bind_rows(i,d) %>% arrange(timestamp)
 
batch_op_cnt <- function(dat,type) {

  new_bl = as_datetime("2012-09-04")
  op_types = c(paste0("Insert-",type), paste0("Delete-",type))
  i = dat %>% filter(creationDate > new_bl) %>% select(creationDate) %>% rename(timestamp = creationDate) %>% add_column(op_type=op_types[1]) %>% mutate(op_type = factor(op_type,levels = op_types)) 
  d = dat %>% filter(deletionDate > new_bl) %>% filter(explicitlyDeleted == T) %>% select(deletionDate) %>% rename(timestamp = deletionDate) %>% add_column(op_type=op_types[2]) %>% mutate(op_type = factor(op_type,levels = op_types))
  o = bind_rows(i,d) %>% arrange(timestamp)
  
  rb_i = rep(0,17)
  rb_d = rep(0,17)
  
  for (i in 1:17) {
    rb = o %>% filter(timestamp >= new_bl + as.duration(weeks(i-1)), timestamp < new_bl + as.duration(weeks(i)))
    total = as.numeric(rb %>% count())
    rb_i[i]  = as.numeric(rb %>% filter(op_type == op_types[1]) %>% count())
    rb_d[i] = total - rb_i[i]
  }
  
  res = tibble(
    `i`=rb_i,
    `d`=rb_d
  )
  
  return(res)
}


pe = batch_op_cnt(person,"person") %>% rename(c("i-pers" = "i", "d-pers" ="d"))
po = batch_op_cnt(post,"post") %>% rename(c("i-post" = "i", "d-post" ="d"))
c = batch_op_cnt(comment,"comment") %>% rename(c("i-comm" = "i", "d-comm" ="d"))
f = batch_op_cnt(forum,"forum") %>% rename(c("i-forum" = "i", "d-forum" ="d"))
k = batch_op_cnt(person_knows_person,"knows") %>% rename(c("i-knows" = "i", "d-knows" ="d"))
pl = batch_op_cnt(person_likes_post,"likes-p") %>% rename(c("i-likes-p" = "i", "d-likes-p" ="d"))
cl = batch_op_cnt(person_likes_comment,"likes-c") %>% rename(c("i-likes-c" = "i", "d-likes-c" ="d"))
fm = batch_op_cnt(forum_hasMember_person,"memb") %>% rename(c("i-memb" = "i", "d-memb" ="d"))
res = tibble(`rb`=seq(1,17))

res = bind_cols(res,pe,po,c,f,k,pl,cl,fm)

total_i = res %>% select(starts_with("i-")) %>% rowSums()
total_d = res %>% select(starts_with("d-")) %>% rowSums()
pc = total_i/(total_i + total_d)

inserts = tibble(`inserts` = total_i)
deletes = tibble(`deletes` = total_d)
pc = tibble(`pc` = pc)

res = bind_cols(res, inserts, deletes,pc)
print(res%>% data.frame,row.names = FALSE)


# plots
# new_posts_per_day = post %>% filter(creationDate > new_bl) %>% mutate(creationDate = date(creationDate)) %>% count(creationDate)
# deleted_posts_per_day = post %>% filter(explicitlyDeleted == T, deletionDate > new_bl) %>% mutate(deletionDate = date(deletionDate)) %>% count(deletionDate)
# 
# ggplot(new_posts_per_day , aes(x = creationDate, y=n)) + geom_line() 
# ggplot(deleted_posts_per_day , aes(x = deletionDate, y=n)) + geom_line()
# 
# (p_hist <- ggplot(posts_per_day, aes(x=n)) + geom_histogram())


# operations_per_type = updates %>% count(type) %>% mutate(type = types)
# operations_per_day = updates %>% mutate(timestamp = date(timestamp)) %>% group_by(type) %>% count(timestamp) %>% do({
#    p <- ggplot(., aes(x = timestamp, y = n)) + geom_line()
#    ggsave(p, filename = paste0("./figs/ops_per_day_type_", unique(.$type),  ".pdf"), width = 4, height = 4, units = "in")
#    invisible(.)
#    })
# TODO: add labels and some colour to plots
