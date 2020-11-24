# Importing the libraries
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import PolynomialFeatures

# Importing the dataset
datas = pd.read_csv('data.csv')
datas

X = datas.iloc[:, 0:1].values
y = datas.iloc[:, 1].values

poly = PolynomialFeatures(degree = 1)
X_poly = poly.fit_transform(X)

poly.fit(X_poly, y)
lin2 = LinearRegression()
lin2.fit(X_poly, y)

lin = LinearRegression()
lin.fit(X, y)

plt.scatter(X, y, color = 'blue')
plt.plot(X, lin.predict(X), color = 'red')
plt.title('Linear Regression')
plt.xlabel('SF')
plt.ylabel('numPersons')

plt.show()


plt.scatter(X, y, color = 'blue')
plt.plot(X, lin2.predict(poly.fit_transform(X)), color = 'red')
plt.title('Polynomial Regression')
plt.xlabel('SF')
plt.ylabel('numPersons')
plt.show()

data2 = pd.read_csv('data2.csv')
newX = data2.iloc[:, 0:1].values
newX

lin.predict(newX)
lin2.predict(poly.fit_transform(newX))
