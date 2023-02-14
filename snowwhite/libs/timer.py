from datetime import datetime
from threading import Timer


class RepeatTimer(Timer):

    def run(self):
        while not self.finished.wait(self.remainingtime()):
            self.function(*self.args, **self.kwargs)

    def remainingtime(self):   # interval en segundos
        dt_now = datetime.now()
        tm_now = dt_now.timestamp()
        itime = self.interval
        tm_run = ((tm_now//itime + (tm_now % itime > 0))
                  * itime)
        return tm_run-tm_now

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
