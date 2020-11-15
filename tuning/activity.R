# Posts - uniform posts (wall/group), flashmob posts (wall/group), photo (album)
# Comments - on uniform/flashmob posts in wall and group
# Likes - on uniform/flashmob posts in wall and group, photos in album, and comments in wall and group.
# 50% people are "deleters"  

(tweets = 67200000)
(undeleted = tweets*0.976)
(deleted = tweets*0.024)

## Posts ##
(ud_posts = undeleted * (0.514 + 0.035))
(d_posts = deleted * (0.556 + 0.06))
(perc_d_posts = round(d_posts/(ud_posts + d_posts),3) * 100)

# uniform posts in groups and walls, 
# flashmob events, i.e., spikes in posts. 
# uniform photos in albums
# TARGET: 2.7% posts deleted across the simulation period. 
# Adjusted target: 5.4% as only 50% people can delete posts. 

# uniform/flashmob posts in walls and groups have unif(0,20) comments 
# this is used to determine a probability of deletion 
# photos in album use flat probability 
mapping = c(18,15.2,14.2,10,10,10,10,5,5,5,1,1,1,1,1,1,1,1,1,1,1)/100
mean(mapping) # 0.054
plot(mapping) 

ggplot(data = data.frame(x = 0:20,
                         y = mapping,
                         yend = rep(0,21)),
       aes(x = x, y = y, xend = x, yend = yend)) +
  geom_point() +
  geom_segment() +
  scale_x_continuous(name="comments",
                
                     limits = c(-0.5, 20.5)) +
  scale_y_continuous(name="probability",
                     limits = c(0.0,0.2)) +
  theme_bw() +
  ggtitle("comment-probability post deleted mapping\n") +
  theme(plot.title = element_text(hjust = 0.5),
        text = element_text(size = 12))
  
ldbc = "/Users/jackwaudby/Documents/ldbc/ldbc_snb_datagen/tuning/figs/"
ggsave(paste0(ldbc,"comment.pdf"))

n = 11000  #sf1
is_deleted = rep(F,n)
postComms = rep(0,n)
for (i in 1:n) {
  postComms[i] = sample(1:21,1,replace = T) - 1
  if (runif(1) < 0.5) {
     if(runif(1) < mapping[postComms[i] + 1]) {
      # if(runif(1) < 0.052) { # photos
       is_deleted [i] = T
    }
  }
}
(prop_deleted = sum(is_deleted )/n) # ~0.027
hist(postComms[which(is_deleted ==T)]) #TODO: ggplot

## Comments ##
# Photos in albums do not have comments.
(ud_comments = undeleted * 0.264)
(d_comments = deleted * 0.194)
(perc_d_comments = round(d_comments/(ud_comments + d_comments),3) * 100)
# TARGET: 1.8% comment deleted across the simulation period. 

## Likes ##
## Happen on posts, photos, and comments in all forum types.
(ud_likes = undeleted * 0.188)
(d_likes = deleted * 0.191)
(perc_d_likes = round(d_likes/(ud_likes + d_likes),3) * 100)
# TARGET: 2.4% likes deleted across the simulation period. 

## Forum ##
# Only groups and albums can be removed 
# TARGET: 1% forums deleted across the simulation period. 

## Memberships ##
# Only memberships in groups can be removed 
# TARGET: 5% memberships deleted across the simulation period. 

## Temporal Distribution ##
# used by posts/photos, comments, and likes
minutes = c(0,0.5,1,5,10,20,30,40,60,120,300,1440,2880,4320,5760,7200,8460,10080)
p = c(0,.17,.22,.38,.46,.54,.586,.61,.652,.752,.812,.891,.949,.973,.986,.994,0.998,1)
cbind(minutes,p)
# TODO: change absolute path 
ldbc = "/Users/jackwaudby/Documents/ldbc/ldbc_snb_datagen/src/main/resources/dictionaries/"
write.table(p, file = paste0(ldbc,"powerLawActivityDeleteDate.txt"), row.names = FALSE, col.names = FALSE)

powerLawActivityDelete = function() {
  r = runif(1)
  for (i in 1:length(p)) {
   if (r < p[i]) {
     lower = minutes[i-1]
     upper = minutes[i]
     x = lower + (upper-lower) * runif(1)
     break
    }
  }
  return(x)
}

draw = rep(0,10000)
for (i in 1:length(draw)) {
  draw[i] = powerLawActivityDelete()
}
mean(draw)/60
sqrt(var(draw))/60
plot(minutes,p,type="l") 

ggplot(data = data.frame(x = minutes,y = p),
       aes(x = x, y = y)) +
  geom_line() +
  scale_x_continuous(name="x (minutes)",
                     limits = c(0, 10100)) +
  scale_y_continuous(name="F(x)",
                     limits = c(0.0,1.0)) +
  theme_bw() +
  ggtitle("cdf post deletion since creation\n") +
  theme(plot.title = element_text(hjust = 0.5),
        text = element_text(size = 12))

ldbc = "/Users/jackwaudby/Documents/ldbc/ldbc_snb_datagen/tuning/figs/"
ggsave(paste0(ldbc,"post_temporal.pdf"))

dev.off()
