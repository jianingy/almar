#!/usr/bin/env python
# -*- coding: utf-8 -*-

# filename   : debug.py
# created at : 2012-12-20 12:50:11
# author     : Jianing Yang <jianingy.yang AT gmail DOT com>

__author__ = 'Jianing Yang <jianingy.yang AT gmail DOT com>'

__all__ = ['debug_out', 'warn_out', 'error_out', 'out']

from twisted.python import log
from logging import DEBUG, WARNING, ERROR, CRITICAL
from sys import exit


def debug_out(s):
    log.msg(s, level=DEBUG)


def warn_out(s):
    log.msg(s, level=WARNING)


def error_out(s):
    log.msg(s, level=ERROR)


def out(s):
    log.msg(s)


def fatal_out(s):
    log.msg(s, level=CRITICAL)
    exit(111)


def pretty_out(s):
    import pprint
    pp = pprint.PrettyPrinter(indent=4)
    pp.pprint(s)
