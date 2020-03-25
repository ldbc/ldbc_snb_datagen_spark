# social capital - geometrically distributed 
hist(rgeom(1000,0.04)) 
plot(1:1000,sort(rgeom(1000,0.04),decreasing = T),xlim=c(0,1000)) #  pdf
plot(1:1000,cumsum(dgeom(1:1000,0.04)),type="b",xlim=c(0,1000)) # prob leaving cdf 

# fb degree distribution 
draws_10000 = read.csv2("fb_degree_10000.csv",header =F,sep=",", stringsAsFactors= FALSE) 
hist(as.numeric(draws_10000))
mean(as.numeric(draws_10000))



left = 0
people = 1000
prob = c()
degree = c() 
leavers = c()
for (i in 1:people) {
  degree[i] = rgeom(1,0.04) # generate degree
  prob[i] = pexp(degree[i], 0.1) # calc prob of leaving
  if(runif(1)>prob[i]){
    leavers[i] = degree[i]
    left = left + 1
    if(left == people * 0.07) {
      break
    }
  }
}
left
(leavers = leavers[!is.na(leavers)])
hist(leavers)


# person deletion date pmf
fx <- function(x,a,b,c,del) {
  r = c()
  lim = min(x-del,b)
  if( x < c ) {
    if (lim > 0) {
      for (i in 1:lim) {
        r[i] = 1/(b-a+1) * 1/(c - (i + del) + 1)
      }
      res = sum(r)
    } else {
      res = 0
      }
    } else {
      res = 0
  }
  return(res)
}


p = c()
for (i in 1:100) {
  p[i] = fx(i,a=1,b=30,c=100,del=1)
}


dates = seq(2010,2019.9,by=0.1)

png(filename = "pmf.png")
plot(dates,p,ylab="prob",xlab="date",pch=20,main="Person deletion date pmf")

png(filename = "cdf.png")
plot(dates,cumsum(p),xlab="x(date)",ylab="P(x)",pch=20,main="Person deletion date cdf")

png(filename="cdf-res.png")
plot(dates[1:30],cumsum(p[1:30]),xlab="x(date)",ylab="P(x)",pch=20,main="Person deletion date cdf")
dev.off()

abline(v=2012.9,h=cumsum(p)[30])
(cumsum(p)[30]) # 16.2%

# confirm with real person deletion dates 
pers = read.csv2("social_network/dynamic/person_0_0.csv",sep = "|")
dd = pers$deletionDate
sum(as.Date(dd) < as.Date("2013-01-01"))/11000 # 16.6%


