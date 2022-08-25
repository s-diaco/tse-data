import jdatetime
import datetime
import numpy as np


def test_jdatetime():
    time1 = jdatetime.datetime.now()
    time2 = jdatetime.date.fromgregorian(date=datetime.datetime.strptime("20200405", '%Y%m%d'))
    print(time1)
    print(time2)
    print(np.array(list({1, 3, 5}))[1])


if __name__ == '__main__':
    test_jdatetime()
