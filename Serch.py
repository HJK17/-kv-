#!/usr/bin/python
# -*- encoding: utf-8 -*-

import mmap
import contextlib
import sys
import time

import config
from Recover import caltime


def wo():
    value = input("plz query content (input 'exit' finish): ")
    if value == "exit":
        print('over!!!')
        sys.exit()
    elif len(value) == 0:
        print("None")
    else:
        value = value.encode()
        wo1(value)


@caltime
def wo1(value):
    count = 0
    f = open(config.conf["filename"], 'r')
    with contextlib.closing(mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)) as m:
        while True:
            line = m.readline().strip()
            if line.find(value) >= 0:
                dict1 = eval(line.decode())
                for key in dict1:
                    vl = dict1[key]
                    value1 = str(value.decode())
                    vl1 = str(vl)
                    if value1 in vl1:
                        print("val->", vl1)
                        count += 1
            elif m.tell() == m.size():
                break
        print("count->", count)


if __name__ == '__main__':
    while True:
        wo()
