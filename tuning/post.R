# target 0.015
prob = c(rep(8,5),rep(2,5),rep(1,10))/100
posts = 100000
c = rep(0,posts)
deleted = rep(F,posts)
for (i in 1:posts) {
  c[i] = sample(1:20,1,replace = T)
  if(0.5*prob[c[i]] > runif(1)) {
    deleted[i] = T
  }
}
sum(deleted == T)/posts

hist(c[which(deleted==T)])

# scale mins 
minutes = c(0.5,1,5,10,20,30,40,60,120,300,1440,2880,4320,5760,7200,8460,10080)
p = c(.17,.22,.38,.46,.54,.586,.61,.652,.752,.812,.891,.949,.973,.986,.994,0.998,1)


plot(minutes,p,type="l")
