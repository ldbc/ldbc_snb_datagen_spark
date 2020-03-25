require(ggplot2)
require(ggthemes)

draws_10000 = read.csv2("fb_degree_10000.csv",header =F,sep=",", stringsAsFactors= FALSE) 
draws_27000 = read.csv2("fb_degree_27000.csv",header =F,sep=",", stringsAsFactors= FALSE) 
draws_73000 = read.csv2("fb_degree_73000.csv",header =F,sep=",", stringsAsFactors= FALSE) 
draws_182000 = read.csv2("fb_degree_182000.csv",header =F,sep=",", stringsAsFactors= FALSE) 
draws_499000 = read.csv2("fb_degree_499000.csv",header =F,sep=",", stringsAsFactors= FALSE) 
draws_1250000 = read.csv2("fb_degree_1250000.csv",header =F,sep=",", stringsAsFactors= FALSE) 
draws_3600000 = read.csv2("fb_degree_3600000.csv",header =F,sep=",", stringsAsFactors= FALSE) 


draws_10000 = sort(as.numeric(draws_10000[1,]))
plot(count(draws_10000)[,1],count(draws_10000)[,2]/10000)

draws_10000 = density(draws_10000)
df_10000 = data.frame(x=draws_10000$x,y=draws_10000$y,SF=rep(1,length(draws_10000$x)))

draws_27000 = sort(as.numeric(draws_27000[1,]))
draws_27000 = density(draws_27000)
df_27000 = data.frame(x=draws_27000$x,y=draws_27000$y,SF=rep(3,length(draws_27000$x)))

draws_73000  = sort(as.numeric(draws_73000[1,]))
draws_73000  = density(draws_73000)
df_73000  = data.frame(x=draws_73000$x,y=draws_73000$y,SF=rep(10,length(draws_73000$x)))

draws_499000 = sort(as.numeric(draws_499000[1,]))
draws_499000 = density(draws_499000)
df_499000 = data.frame(x=draws_499000$x,y=draws_499000$y,SF=rep(100,length(draws_499000$x)))

draws_182000 = sort(as.numeric(draws_182000[1,]))
draws_182000 = density(draws_182000)
df_182000 = data.frame(x=draws_182000$x,y=draws_182000$y,SF=rep(30,length(draws_182000$x)))

draws_1250000 = sort(as.numeric(draws_1250000[1,]))
draws_1250000 = density(draws_1250000)
df_1250000 = data.frame(x=draws_1250000$x,y=draws_1250000$y,SF=rep(300,length(draws_1250000$x)))

draws_3600000 = sort(as.numeric(draws_3600000[1,]))
draws_3600000 = density(draws_3600000)
df_3600000 = data.frame(x=draws_3600000$x,y=draws_3600000$y,SF=rep(1000,length(draws_3600000$x)))

df = rbind(df_10000,df_27000)
df = rbind(df,df_73000)
df = rbind(df,df_499000)
df = rbind(df,df_1250000)
df = rbind(df,df_3600000)

df$SF = as.factor(df$SF)

pdf(file="fb_degree_plot.pdf")

ggplot(data=df,aes(x,y,colour = SF)) +
  geom_line(aes(group=SF))+
  labs(title="Facebook Degree Distribution",x = "degree", y="Density") +  
  theme_few() +
  scale_x_continuous(limits = c(-100, 1000))

dev.off()


## Code for PMF
plot(count(draws_10000)[,1],count(draws_10000)[,2]/10000)



ggplot(data = data.frame(x = count(draws_1250000)[,1],
                         y = count(draws_1250000)[,2]/10000),
       aes(x = x, y = y)) +
  geom_point() 


