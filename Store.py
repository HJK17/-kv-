#!/usr/bin/python
# -*- encoding: utf-8 -*-

import copy
import os
import random
from multiprocessing import Process

import config

info_dict1 = {}
info_dict2 = {}


def crenum(info_dict1):
    j = 0
    while True:
        i = random.randint(0, 99999999)
        info_dict = {j: i}  # 自定义字典数据
        info_dict1.update(info_dict)
        j += 1
        print(info_dict)

        # time.sleep(0.001)
        if len(info_dict1) >= config.conf["var_aof_buf"]:
            aof_buf(info_dict1)


def aof_buf(info_dict1):
    """数据缓存区"""
    info_dict2 = copy.deepcopy(info_dict1)
    WriteInfo(info_dict1, info_dict2)


"""保存kv值，aof"""


def WriteInfo(info_dict1, info_dict2):
    # 显示数据类型
    with open(config.conf["filename"], 'a+') as f:
        f.write(str(info_dict2) + "\n")
        f.flush()
        info_dict1.clear()

    try:
        file_name = config.conf["filename"]
        file_stats = os.stat(file_name)
        file_size = file_stats.st_size / (1024 * 1024)
        if file_size >= config.conf["aof_rewrite_min_size"]:
            print("over 10M", file_size)
            newpro(info_dict2)
    except ValueError:
        pass


def aof_rewrite(info_dict2):  # 创建新的AOF文件
    lists = os.listdir(config.conf["dirname"])
    lists.sort(key=lambda x: os.path.getmtime(config.conf["dirname"] + '/' + x))
    if len(lists) >= 2:
        file_old = os.path.join(config.conf["dirname"], lists[0])  # 找到旧aof文件
        path = file_old  # 文件路径
        if os.path.exists(path):  # 如果文件存在
            os.remove(path)
        else:
            pass  # 则返回文件不存在

    with open(config.conf["filename"], 'w+') as dumpf:
        dumpf.write(str(info_dict2) + "\n")
        dumpf.flush()
        print('全部重写完毕')


def newpro(info_dict2):
    ar = Process(target=aof_rewrite, args=(info_dict2,))
    ar.start()


if __name__ == '__main__':
    if not os.path.exists(config.conf["dirname"]):
        os.makedirs(config.conf["dirname"])
    print("start")
    p1 = Process(target=crenum, args=(info_dict1,))
    p1.start()
