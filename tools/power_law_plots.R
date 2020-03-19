require(ggplot2)
require(ggthemes)

draws = read.csv2("power_law_cdf.csv",header = F,sep=",", stringsAsFactors= FALSE) 
x = sort(as.numeric(draws[1,]))*24
y = (1:length(x)/length(x))
df = data.frame(x,y)

png(filename="comments_power_law.png")

ggplot(data=df,aes(x,y)) +
  geom_line() +
  labs(title="Empirical Inverse Power Law Cumulative Distribution",x = "x (hours)", y="F(x)") +  
  theme_few()

dev.off()










