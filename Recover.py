#!/usr/bin/python
# -*- encoding: utf-8 -*-

import contextlib
import mmap
import time
import config


def caltime(func):
    def inner(*args, **kw):
        now_time = time.time()
        func(*args, **kw)
        costtime = time.time()
        print("cost time:", costtime - now_time, "over!!!")

    return inner


@caltime
def recover():
    f = open(config.conf["filename"], 'r')
    with contextlib.closing(mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)) as m:  # ACCESS_WRITE, ACCESS_READ
        while True:
            line = m.readline().strip()
            dict1 = eval(line)
            for key in dict1:
                val = dict1[key]
                print(key, val)

            if m.tell() == m.size():
                break


if __name__ == '__main__':
    recover()
