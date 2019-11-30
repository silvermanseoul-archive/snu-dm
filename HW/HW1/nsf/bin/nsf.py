#!/usr/bin/env python

"""
Impelements non-symmetric fellow-shop
Input : directed graph
    e.g.) "3   4" indicates that person 3 has 4 followers.
Output : (node, node)
    e.g.) "1 2" node 1 => node 2 is non-symmetric
"""
# DOCS_INCLUDE_START
import pydoop.mapreduce.api as api
import pydoop.mapreduce.pipes as pipes


class Mapper(api.Mapper):

    def map(self, context):
        src = context.value.split('\t')[0]
        dst = context.value.split('\t')[1]
        if int(src)>int(dst):
            src, dst = dst, src
        context.emit((src, dst), 1)


class Reducer(api.Reducer):

    def reduce(self, context):
        cnt = sum(context.values)
        if cnt == 1:
            context.emit(context.key[0], context.key[1])



FACTORY = pipes.Factory(Mapper, reducer_class=Reducer)


def main():
    pipes.run_task(FACTORY)


if __name__ == "__main__":
    main()
