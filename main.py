# -*- coding: utf-8 -*-
from multiprocessing import Process, Manager
import sys
import csv
import re

RE_CJK = re.compile(r'[\u4e00-\ufaff]+', re.UNICODE)
RE_ENG = re.compile(r'[a-zA-Z]+')
RE_ALL = re.compile(r'[a-zA-Z\u4e00-\ufaff]+', re.UNICODE)


class Trie():
    pass

def split_line(filename):
    csvfile = open(filename, 'r', newline='')
    sourcereader = csv.reader(csvfile, delimiter=',')
    index_2gram = list()
    index_3gram = list()
    index_english = list()
    for line in sourcereader:
        cjk_strings = RE_CJK.findall(line[1])
        eng_strings = RE_ENG.findall(line[1])
        split_string_2gram = set()
        split_string_3gram = set()
        for string in cjk_strings:
            split_string_2gram.update([string[i:i+2] for i in range(0, len(string))])
            split_string_3gram.update([string[i:i+3] for i in range(0, len(string))])

        index_2gram.append(split_string_2gram)
        index_3gram.append(split_string_3gram)
        index_english.append(set(eng_strings))
    csvfile.close()

    return [index_2gram, index_3gram, index_english]


def or_search(print_list, queries, index_string):
    for i, search_line in enumerate(index_string):
        if (i+1) not in print_list and queries & search_line:
            print_list.append(i+1)


def and_search(print_list, queries, index_string):
    for i, search_line in enumerate(index_string):
        if (i+1) not in print_list and queries < search_line:
            print_list.append(i+1)


def not_search(print_list, in_element, notin_element, index_string):
    for i, search_line in enumerate(index_string):
        if ((i+1) not in print_list
            and in_element in search_line
            and not notin_element < search_line):
            print_list.append(i+1)


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
            print_list = Manager().list()
            processes = []
            if 'or' in query_line:
                queries = set(re.split(' or ', query_line))
                for i in range(0,2):
                    p = Process(target=or_search, args=(print_list, queries, index_string[i]))
                    p.start()
                    processes.append(p)

            elif 'and' in query_line:
                queries = set(re.split(' and ', query_line))
                for i in range(0,2):
                    p = Process(target=and_search, args=(print_list, queries, index_string[i]))
                    p.start()
                    processes.append(p)
            elif 'not' in query_line:
                queries = re.split(' not ', query_line)
                in_element = queries[0]
                notin_element = set(queries[1:])
                for i in range(0,2):
                    p = Process(target=not_search, args=(print_list, in_element, notin_element, index_string[i]))
                    p.start()
                    processes.append(p)

            for process in processes:
                process.join()

            if not print_list:
                print('0', file=o)
            else:
                print(','.join(map(str, print_list)), file=o)
