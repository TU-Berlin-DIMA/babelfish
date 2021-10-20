import time

import numpy as np


def udf(x, y, n):
    m = (len(x) * np.sum(x * y) - np.sum(x) * np.sum(y)) / (len(x) * np.sum(x * x) - np.sum(x) * np.sum(x))
    b = (np.sum(y) - m * np.sum(x)) / len(x)
    return m * 42 + b

def benchmark(xBuffer, yBuffer):
    last = 0
    for n in range(1, 10000):
        last = udf(xBuffer, yBuffer, n)
    return last


x = np.arange(100)
y = np.arange(100, 200)
for n in range(1, 100):
    print("Run benchmark in iteration: " + str(n))
    start = time.time()
    print(benchmark(x, y))
    end = time.time()
    print(end - start)
