"""
FILE: sf-prediction.py
DATE: 24 February 2022

DESC: Predict the number of nodes for different scale factors.
      The known values for each scale factor are:
     _____________________________________________________________________
    | Scale Factor    |    1  |   3   |   10  |   30   |  100   |  300    |
    ----------------------------------------------------------------------
    | Nr. of Persons  | 10620 | 25870 | 70800 | 175950 | 487700 | 1230500 |
    ----------------------------------------------------------------------

     ___________________________________________________________
    | Scale Factor    |   1000  |  3000   |  10000   |  30000   |
    -------------------------------------------------------------
    | Nr. of Persons  | 3505000 | 9300000 | 27200000 | 77000000 |
    ------------------------------------------------------------  

    The values are approximated using a polynomial with degree 3 for scale
    factors untill 3000 with the nr. of persons and scale factors scaled 
    with natural logarithm. For scale factors larger than 3000, a 5th degree
    polynomial is used without log-scale.
"""

import numpy as np

def approximate_small_sf(sf, num_persons, sf_new):
    """
    :param: sf (List):          List of scale factors
    :param: num_persons (List): List of number of persons aligned with the
                                scale factor list
    :param: sf_new (List):      The scale factors to predict

    :return: List of predicted number of persons
    """
    coeffs = np.polyfit(np.log(sf), np.log(num_persons), deg=3)
    poly = np.poly1d(coeffs)
    yfit = lambda x: np.exp(poly(np.log(x)))
    return np.array(yfit(sf_new))

def approximate_large_sf(sf, num_persons, sf_new):
    """
    :param: sf (List):          List of scale factors
    :param: num_persons (List): List of number of persons aligned with the
                                scale factor list
    :param: sf_new (List):      The scale factors to predict

    :return: List of predicted number of persons
    """
    coeffs = np.polyfit(sf, num_persons, deg=5)
    poly = np.poly1d(coeffs)
    return np.array(poly(sf_new))

if __name__ == "__main__":
    sf = [1, 3, 10, 30, 100, 300, 1000, 3000, 10000, 30000]
    num_persons = [
        10620, 25870, 70800, 175950, 487700, 1230500, 3505000, 9300000,
        27200000, 77000000
    ]
    sf_new = [1, 3, 10, 30, 100, 300, 1000, 3000, 10000, 30000, 100000]

    predicted_large_sf = approximate_large_sf(sf, num_persons, sf_new)
    predicted_small_sf = approximate_small_sf(sf, num_persons, sf_new)

    print("Number of Persons")
    print(num_persons)
    print("Polyfit Large")
    print(predicted_large_sf)
    print("Polyfit Small")
    print(predicted_small_sf)

    print("Error Polyfit Large")
    print(np.abs(predicted_large_sf[:10] - num_persons))
    print("Error Polyfit Small")
    print(np.abs(predicted_small_sf[:10] - num_persons))
