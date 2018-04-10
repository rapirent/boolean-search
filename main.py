# -*- coding: utf-8 -*-
from threading import Thread, Lock
import sys
import csv
import re

RE_CJK = re.compile(r'[\u4e00-\ufaff]+', re.UNICODE)
RE_ENG = re.compile(r'[a-zA-Z]+')
RE_ALL = re.compile(r'[a-zA-Z\u4e00-\ufaff]+', re.UNICODE)

THREAD_NUM = 2


def index_parallel(source, start, step, index):
    for i in range(start, len(source), 2):
        cjk_strings = RE_CJK.findall(source[i][1])
        eng_strings = RE_ENG.findall(source[i][1])
        split_string = set()
        for string in cjk_strings:
            split_string.update([string[i:i+2] for i in range(0, len(string))])
            split_string.update([string[i:i+3] for i in range(0, len(string))])
        if eng_strings:
            split_string.update(eng_strings)
        index[i % THREAD_NUM].append(split_string)


def split_line(filename):
    csvfile = open(filename, 'r', newline='')
    sourcereader = list(csv.reader(csvfile, delimiter=','))
    index = [list(), list()]
    threads = list()
    for i in range(0, THREAD_NUM):
        s = Thread(target=index_parallel, args=(sourcereader, i, THREAD_NUM, index))
        s.start()
        threads.append(s)
    for thread in threads:
        thread.join()

    csvfile.close()

    return index


def or_search(print_list, queries, index_string, thread_index):
    for (i, search_line) in enumerate(index_string):
        if queries & search_line:
            print_list.append(i*THREAD_NUM+thread_index+1)


def and_search(print_list, queries, index_string, thread_index):
    for (i, search_line) in enumerate(index_string):
        if queries < search_line:
            print_list.append(i*THREAD_NUM+thread_index+1)


def not_search(print_list, in_element, notin_element, index_string, thread_index):
    for (i, search_line) in enumerate(index_string):
        if (in_element in search_line
            and not notin_element < search_line):
            print_list.append(i*THREAD_NUM+thread_index+1)

if __name__ == '__main__':

    import argparse
    # python main.py --source source.csv --query query.txt --output output.txt
    parser = argparse.ArgumentParser()
    parser.add_argument('--source',
                        default='source.csv',
                        help='input source data file name')
    parser.add_argument('--query',
                        default='query.txt',
                        help='query file name')
    parser.add_argument('--output',
                        default='output.txt',
                        help='output file name')
    args = parser.parse_args()

    index_string = split_line(args.source)

    with open(args.output, 'w') as o, open(args.query, 'r') as q:
        for query_line in q:
            query_line = re.sub('\n', '', query_line)
            # with Manager() as manager:
            print_list = list()
            threads = list()
            if 'or' in query_line:
                queries = set(re.split(' or ', query_line))
                for i in range(0, THREAD_NUM):
                    s = Thread(target=or_search,
                                args=(print_list, queries, index_string[i], i))
                    s.start()
                    threads.append(s)
          
            elif 'and' in query_line:
                queries = set(re.split(' and ', query_line))
                for i in range(0, THREAD_NUM):
                    s = Thread(target=and_search,
                                args=(print_list, queries, index_string[i], i))
                    s.start()
                    threads.append(s)
            elif 'not' in query_line:
                queries = re.split(' not ', query_line)
                in_element = queries[0]
                notin_element = set(queries[1:])
                for i in range(0, THREAD_NUM):
                    s = Thread(target=not_search,
                                args=(print_list, in_element, notin_element, index_string[i], i))
                    s.start()
                    threads.append(s)

            for thread in threads:
                thread.join()

            if not print_list:
                print('0', file=o)
            else:
                print_list.sort()
                print(','.join(map(str, print_list)), file=o)
