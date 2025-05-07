from datetime import datetime, timedelta
# import pytz
from threading import Timer


class RepeatTimer(Timer):

    def run(self):
        while not self.finished.wait(self.remaining_time()):
            self.function(*self.args, **self.kwargs)

    def remaining_time(self):  # interval en segundos
        dt_now = datetime.now()
        tm_now = dt_now.timestamp()
        itime = self.interval
        tm_run = ((tm_now // itime + (tm_now % itime > 0)) * itime)
        return tm_run - tm_now


def nextevent(interval, dt_now):  # interval en segundos
    # dt_now = datetime.utcnow()

    tm_now = dt_now.timestamp()
    itime = interval
    tm_run = ((tm_now // itime + (tm_now % itime > 0)) * itime)
    dt_run = datetime.utcfromtimestamp(tm_run)

    dt_prev = dt_run - timedelta(seconds=interval)

    return dt_prev, dt_run


# TESTING

# import time
# def display(msg):
#     print(msg + ' ' + datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'))

# print('Threading started')
# timer = RepeatTimer(10, display, ['Execute'])
# timer.start()
# time.sleep(50)  # It gets suspended for the given number of seconds
# print('Threading finishing')
# timer.cancel()
