#!/usr/bin/env python3
from scripts import *
import sys

class VefsDependencyChecker(DependencyChecker):
    def __init__(self, vefs_option):
        super().__init__(['unvme:ve'], conf, vefs_option)
        self.vefs_option = vefs_option

    def filter_files(self, list_of_files):
        return [f for f in list_of_files if f != './vefs_autogen_conf.h']

    def process(self):
        shell.check_call("./build.sh '{}'".format(self.vefs_option))

conf = {
    #"force_test": True,
}

if __name__ == "__main__":
    VefsDependencyChecker(sys.argv[1]).check_and_process()
