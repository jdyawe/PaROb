import numpy as np

a = np.zeros(2,)
print(a)
print((a==0).all())
a[1] = 1
print(a)
print((a==0).all())
