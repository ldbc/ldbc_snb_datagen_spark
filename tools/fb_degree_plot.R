require(ggplot2)
require(ggthemes)

draws = read.csv2("fb_degree.csv",header =F,sep=",", stringsAsFactors= FALSE) 
x = sort(as.numeric(draws[1,]))
y = density(x)
df = data.frame(x=y$x,y=y$y)

pdf(file="fb_degree_plot.pdf")

ggplot(data=df,aes(x,y)) +
  geom_line() +
  labs(title="Facebook Degree Distribution",x = "degree", y="Density") +  
  theme_few()

dev.off()

