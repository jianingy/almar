#!/usr/bin/env python
# -*- coding: utf-8 -*-

# filename   : util.py
# created at : 2013-01-15 16:44:48
# author     : Jianing Yang <jianingy.yang AT gmail DOT com>

from __future__ import print_function

__author__ = 'Jianing Yang <jianingy.yang AT gmail DOT com>'


def quote(s):
    return s.replace("'", "\\'").replace('"', '\\"')


def banner(s, x='*', w=78):
    print(x * w)
    print(' ' * ((w - len(s[:w])) / 2) + s)
    print(x * w)
