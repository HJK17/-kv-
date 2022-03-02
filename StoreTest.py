#!/usr/bin/python
# -*- encoding: utf-8 -*-

from multiprocessing import Process, Manager
import os
import random
import time
import copy
import config


def crenum(info_dict1):
    j = 0
    while True:
        i = random.randint(0, 99999999)
        # j = random.randint(0, 99999999)
        info_dict = {j: i}  # 自定义字典数据
        info_dict1.update(info_dict)
        # print(info_dict)
        j += 1
        # time.sleep(0.001)

        if len(info_dict1) >= config.conf["var_aof_buf"]:
            aof_buf(info_dict1)
            time.sleep(1)


def aof_buf(info_dict1):
    print(">>aof-->")
    """数据缓存区"""
    info_dict2 = copy.deepcopy(info_dict1)
    WriteInfo(info_dict1,info_dict2)
    info_dict1.clear()
    return


"""保存kv值，aof"""


def WriteInfo(info_dict1,info_dict2):
    # 显示数据类型
    with open(config.conf["filename"], 'a+') as f:
        f.write(str(info_dict2) + "\n")
        f.flush()
        info_dict1.clear()

    """检查aof文件大小"""
    try:
        file_name = config.conf["filename"]
        file_stats = os.stat(file_name)
        file_size = file_stats.st_size / (1024 * 1024)
        if file_size >= config.conf["aof_rewrite_min_size"]:
            print("over 10M", file_size)
            newpro(info_dict2)
    except ValueError:
        pass


"""创建新的AOF文件，但是这个函数会进行大量的写入操作，所以调用这个函数的线程将被长时间的阻塞"""


def aof_rewrite(info_dict2):
    """不追加，直接全部改写"""
    lists = os.listdir('./aoffile')
    lists.sort(key=lambda x: os.path.getmtime('./aoffile' + '/' + x))
    """删除旧文件"""
    if len(lists) >= 2:
        file_old = os.path.join('./aoffile/', lists[0])  # 找到旧aof文件
        path = file_old  # 文件路径
        if os.path.exists(path):  # 如果文件存在
            os.remove(path)
            print(">>>删除的文件")
        else:
            print('no such file')  # 则返回文件不存在

    with open(config.conf["filename"], 'w+') as dumpf:
        dumpf.write(str(info_dict2) + "\n")
        dumpf.flush()
        print('全部重写完毕')


def newpro(info_dict2):
    ar = Process(target=aof_rewrite, args=(info_dict2,))
    ar.start()


if __name__ == '__main__':
    print(">>启动随机数生成")
    info_dict1 = dict()
    info_dict2 = dict()
    p1 = Process(target=crenum, args=(info_dict1,))
    p1.start()
