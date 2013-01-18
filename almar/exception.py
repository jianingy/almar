#!/usr/bin/env python
# -*- coding: utf-8 -*-

# filename   : exception.py
# created at : 2013-01-09 11:13:09
# author     : Jianing Yang <jianingy.yang AT gmail DOT com>

__author__ = 'Jianing Yang <jianingy.yang AT gmail DOT com>'


class AlmarErrorBase(Exception):
    pass


class ModelNameError(AlmarErrorBase):
    pass


class MalformedIncomingData(AlmarErrorBase):
    pass


class ModelNotExistError(AlmarErrorBase):
    pass


class KeyNotDefinedError(AlmarErrorBase):
    pass


class MissingFieldError(AlmarErrorBase):
    pass


class ConstraintViolationError(AlmarErrorBase):
    pass


class SearchGrammerError(AlmarErrorBase):
    pass


# ERROR CODE

INVALID_INPUT = 4001

CONSTRAINT_VIOLATION = 5001
