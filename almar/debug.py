#!/usr/bin/env python
# -*- coding: utf-8 -*-

# filename   : debug.py
# created at : 2012-12-20 12:50:11
# author     : Jianing Yang <jianingy.yang AT gmail DOT com>

__author__ = 'Jianing Yang <jianingy.yang AT gmail DOT com>'

__all__ = ['debug_out', 'warn_out', 'error_out', 'out']

import sys


def debug_out(s):
    print >>sys.stderr, "DEBUG:", s


def warn_out(s):
    print >>sys.stderr, s


def error_out(s):
    print >>sys.stderr, s


def out(s):
    print >>sys.stderr, s


def fatal_out(s):
    print >>sys.stderr, "FATAL:", s
    sys.exit(111)


def pretty_out(s):
    import pprint
    pp = pprint.PrettyPrinter(indent=4)
    pp.pprint(s)
