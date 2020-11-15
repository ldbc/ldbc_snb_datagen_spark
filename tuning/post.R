# libraries
suppressMessages(library(tidyverse))
suppressMessages(library(lubridate))
options(digits=4)

ldbc = "/Users/jackwaudby/Documents/ldbc/ldbc_snb_datagen/out/social_network/dynamic/"
f = paste0(ldbc,"forum_0_0_trimmed.csv")
p = paste0(ldbc,"post_0_0_trimmed.csv")

forum = suppressMessages(read_delim(f,"|"))
post = suppressMessages(read_delim(p,"|"))

post = post %>% rename(forumId = Forum.id,postId = id)
post = post %>% select(postId,explicitlyDeleted,forumId)

forum = forum %>% rename(forumId = id)
forum = forum %>% select(forumId,type)

joint = merge(post,forum,by="forumId")

wall = joint %>% filter(type == "WALL")

(p_wposts = mean(joint$type == "WALL"))
(p_gposts = mean(joint$type == "GROUP"))
(p_photos = mean(joint$type == "ALBUM"))

(d_photos = mean(joint$explicitlyDeleted[joint$type == "ALBUM"])*100)
(d_w_posts = mean(joint$explicitlyDeleted[joint$type == "WALL"])*100)
(d_g_posts = mean(joint$explicitlyDeleted[joint$type == "GROUP"])*100)
(d_overall = mean(joint$explicitlyDeleted)*100)

