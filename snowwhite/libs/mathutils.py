import numpy as np


def gaussian(x, mu, sigma):
    A = 1.0 / (sigma * np.sqrt(2.0 * np.pi))
    exponent = 0.5 * (((x - mu) / sigma)**2)
    return A * np.exp(-exponent)


class CentralStats():

    def __init__(self, n, _sum, _sumsq):
        self.n = n
        self._sum = _sum
        self._sumsq = _sumsq

    def add(self, x):
        self.n = self.n + 1
        self._sum = self._sum + x
        self._sumsq = self._sumsq + (x * x)

    def get_stats(self):
        mean = self._sum / self.n
        variance = (self._sumsq / self.n) - (mean * mean)
        std = np.sqrt(variance)
        error = std / np.sqrt(self.n)
        return mean, variance, std, error

    def get_sums(self):
        return self.n, self._sum, self._sumsq

    def reset(self):
        self.n = 0
        self._sum = 0.0
        self._sumsq = 0.0


class RecursiveStats:

    def __init__(self, n=0, mean=0.0, error=0.0):
        self.__n = n
        self.__mean = mean
        self.__var = n * (error**2)
        self.__error = error

    @staticmethod
    def recursive_mean(prevmean, x, n):
        return prevmean + (x - prevmean) / n

    def add(self, x):
        self.__n = self.__n + 1
        self.__mean = self.recursive_mean(self.__mean, x, self.__n)
        self.__var = self.recursive_mean(self.__var, (x - self.__mean)**2,
                                         self.__n)
        self.__error = np.sqrt(self.__var / self.__n)

    def __iadd__(self, other):
        self.add(other)
        return self

    @property
    def stats(self):
        return self.__n, self.__mean, self.__error

    @property
    def n(self):
        return self.__n

    @property
    def mean(self):
        return self.__mean

    @property
    def error(self):
        return self.__error

    def reset(self):
        self.__n = 0
        self.__mean = 0.0
        self.__var = 0.0
        self.__error = 0.0


def timediffseconds(dt0, dt1):
    return (dt1 - dt0) / np.timedelta64(1, 's')
