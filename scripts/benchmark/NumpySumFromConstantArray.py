import numpy as np
import time

def udf(buffer):
    x = np.arange(1000)
    return x.sum()

def benchmark(buffer):
    sum = 0
    for n in range(1, 10000):
        sum = sum + udf(buffer)
    return sum

array =np.arange(100)
buffer =array.tobytes()
for n in range(1, 100):
    print("Run benchmark in iteration: " + str(n))
    start = time.time()
    print(benchmark(buffer))
    end = time.time()
    print(end - start)
