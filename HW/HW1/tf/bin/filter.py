#!/usr/bin/env python

"""
Filter top-50 follower countings
"""

import struct

from pydoop.mapreduce.pipes import run_task, Factory
from pydoop.mapreduce.api import Mapper, Reducer

class FilterMapper(Mapper):

    def map(self, context):
        dst, cnt = context.key, context.value
        cnt = struct.unpack(">i", cnt)[0]
        context.emit(0, (dst, cnt))

class FilterReducer(Reducer):

    def reduce(self, context):
        dic = {}
        lst = []
        for pair in list(context.values):
            dst, cnt = pair[0], pair[1]
            dic[dst] = cnt
        lst = sorted(dic.items(), key=lambda t:t[1], reverse=True);

        for i in range(50):
            context.emit(lst[i][0], lst[i][1])


if __name__ == "__main__":
    factory = Factory(FilterMapper, FilterReducer)
    run_task(factory)
