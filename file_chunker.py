#!/usr/bin/env python 

import os
import time
import argparse

class Constant(object):
    """Holds the constants used by pretty much all the functions
    in the script.
    """
    #static variable
    INFINITY = pow(2, 40)                           # 1 TB

    def __init__(self):
        # On my PC, this shows the best results after I checked from 8Kb to 2Mb
        self.CHUNK_SIZE = 1 * 1024 * 1024           # 1 MB

        # added at the time of parsing
        # self.BIGFILE_NAME = ""
        # self.COPIEDFILE_NAME = ""

        self.MAX_SPLIT_SIZE = 100 * 1024 * 1024     # 100 MB
        self.CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
        self.ANALYSE = False

def main(constants, command):
    """invokes the split or join method based on the passed command.
    """
    start = time.clock()

    if command == "join":
        remake_file(const)
    elif command == "split":
        split(const)
    else:
        print 'use either "split" or "join" as the command'
        exit(1)

    end = time.clock()
    if constants.ANALYSE: print "time taken: ", (end-start)


def split(constants):
    """public function that splits the files into the required number of pieces.
    Data to analyse the number of files to split into is taken from constants.
    """
    big_file = open(constants.BIGFILE_NAME, "rb")

    for i, start_byte in enumerate(_get_bytes_for_splitting(big_file, constants.MAX_SPLIT_SIZE)):
        copied_file = open(".".join([constants.COPIEDFILE_NAME, str(i)]),
                                "wb")
        print "Start breaking into part:", copied_file.name
        _copy_in_chunks(constants, big_file, copied_file, start_byte, start_byte + constants.MAX_SPLIT_SIZE)
        copied_file.close()

    big_file.close()


def _minute_copy(source_file, des_file, size=Constant.INFINITY):
    """A (hopefully) module private function that copies chunks of data.
    It returns whether there was anything copied or not.
    """
    chunk = source_file.read(size)
    des_file.write(chunk)
    return bool(chunk)

def _copy_in_chunks(constants, source_file, des_file, from_byte=0, to_byte= Constant.INFINITY):
    """Is called while copying files. This ensures that the complete file is not kept
    in the memory while being transferred. A module-private function.
    """
    size = constants.CHUNK_SIZE
    source_file.seek(from_byte)
    while True:
        # This brings down the copy transfer time of a 140 Mb file with
        # 1 Mb from 1.3 second to 0.13 seconds.
        if (source_file.tell() + constants.CHUNK_SIZE) >= to_byte:
            size = to_byte - source_file.tell()
            _minute_copy(source_file, des_file, size)
            break

        if not _minute_copy(source_file, des_file, size): break

    des_file.flush()


def _get_bytes_for_splitting(source_file, max_split_size):
    """Returns the bytes from which the source file must be split
    in order to get files of sizes that the user wants. A module-private function.
    """
    size = os.stat(source_file.name).st_size
    list_of_starting_bytes = [0]
    start = 0
    while(start < size):
        start += max_split_size
        list_of_starting_bytes.append(start)

    return list_of_starting_bytes[:-1]


def remake_file(constants):
    """This function joins the splitted files into the original file.
    """
    file_names = [x for x in os.listdir(constants.CURRENT_DIR) if 'part' in x]

    # _ = [(x.split('.')[:-1], x.split('.')[-1]) for x in file_names]
    # _.sort(key = lambda (x,y): int(y))
    # file_names = ['.'.join(['.'.join(x), str(y)]) for (x,y) in _]
    file_names.sort(key=lambda x: int(x.split('.')[-1]))

    final_file = open(file_names[0].split('.')[0], "wb")

    for part_name in file_names:
        print "Start joining", part_name
        part_file = open(part_name, "rb")
        _copy_in_chunks(constants, part_file, final_file)
        part_file.close()

    final_file.close()

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="""This tiny tool is useful when you want to split a\
                                         humongously big file into smaller ones and then reattach them together.""")
    parser.add_argument("command", help='Use the word "split" or "join" to tell the program what to do.')
    parser.add_argument("file", help="""
        Specify the original file name of the big file that you want to split / originally split using this program.
        Please make sure that the name is correct else there might be undefined behavior""")
    parser.add_argument('-t', help='Prints the time taken for the operation to complete', required=False, action='store_true')
    parser.add_argument("-S", "--split-size", help="Specify the maximum file size of the splitted files in bytes", required=False)
    parser.add_argument('-C','--chunk-size', help='The chunk size of the data copied/read in a single operation', required=False)
    args = parser.parse_args()

    const = Constant()

    if args.split_size: const.MAX_SPLIT_SIZE = int(args.split_size)
    if args.chunk_size: const.CHUNK_SIZE = int(args.chunk_size)
    if args.t: const.ANALYSE = True
    const.BIGFILE_NAME = args.file
    const.COPIEDFILE_NAME = ".".join([const.BIGFILE_NAME, "part"])

    main(const, args.command)