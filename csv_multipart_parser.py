# -*- coding: utf-8 -*-
from __future__ import absolute_import, unicode_literals

import simplejson as json

from django.conf import settings
from django.http.multipartparser import MultiPartParser
from django.utils import six
from rest_framework.exceptions import ParseError
from rest_framework.parsers import BaseParser
from typing import IO, List, Union

from pandas import read_csv


def read(stream: IO,
         columns_to_filter: List = None,
         as_dict: bool = True,
         strip_and_falsify_value: bool = False,
         lower_case_columns: bool = False) -> List[Union[List, dict]]:
    """
    Read the file/io stream and return the parsed results.
    :param stream: Any file like object that implements `read`
    :param columns_to_filter: The columns to take from here
    :param as_dict: return each row as a list of values or dict
    :return: list of lists or a list of dicts

    >>> from io import StringIO
    >>> sio = StringIO('h1,h2,h3\na,b,false\nd,e,f\ng,h,False')

    >>> read(sio, as_dict=True, strip_and_falsify_value=True)
    [{'h1': 'a', 'h2': 'b', 'h3': False},
     {'h1': 'd', 'h2': 'e', 'h3': 'f'},
     {'h1': 'g', 'h2': 'h', 'h3': False}]

    >>> read(sio, as_dict=True, strip_and_falsify_value=False)
    [{'h1': 'a', 'h2': 'b', 'h3': 'false'},
     {'h1': 'd', 'h2': 'e', 'h3': 'f'},
     {'h1': 'g', 'h2': 'h', 'h3': 'False'}]

    >>> read(sio, as_dict=False, strip_and_falsify_value=True)
    [['a', 'b', False], ['d', 'e', 'f'], ['g', 'h', False]]

    >>> read(sio, as_dict=False, strip_and_falsify_value=False)
    [['a', 'b', 'false'], ['d', 'e', 'f'], ['g', 'h', 'False']]

    """

    data_frame = read_csv(stream, skipinitialspace=True)

    # does some magic to override pandas converting int(id) to floats
    data_frame = data_frame.dropna(axis=0, how='all')  # drop rows where all na
    for column in data_frame.columns.values:
        try:
            if column.endswith("_id"):
                data_frame.loc[:, column] = data_frame[column].astype(int)
        except Exception:
            pass

    def attempt_strip_and_lower(x: str):
        x = x.strip()
        if lower_case_columns:
            x = x.lower()
        return x

    data_frame.columns = data_frame.columns.map(attempt_strip_and_lower)  # strip the column names of spaces

    if lower_case_columns and columns_to_filter:
        columns_to_filter = [i.lower() for i in columns_to_filter]

    if columns_to_filter:
        data_frame = data_frame[columns_to_filter]

    def attempt_strip_and_falsify(x: str):
        """
        If value is a string, then strip whitespace.
        After that, if the value is 'false' (case insensitive) then convert to Python bool False, else dont change.
        """
        if isinstance(x, str):
            x = x.strip()
            if x.lower() == 'false':
                x = False
        return x

    data_frame = data_frame.dropna(how='all')  # filter out row if all required columns are null

    # TODO move `attempt_strip_and_falsify` to a decorator like @transform_strip_and_falsify
    # which will do the final transform. Also change it to column type conversion

    # return as records
    if not as_dict:
        res = data_frame.values.tolist()
        if not strip_and_falsify_value:
            return res

        ret_rows = []
        for row in res:
            ret_rows.append([attempt_strip_and_falsify(i) for i in row])

        return ret_rows

    # return as dictionary
    res = data_frame.to_dict('records')
    if not strip_and_falsify_value:
        return res

    ret_rows = []
    for d in res:
        new_d = {}
        for k, v in d.items():
            new_d[k] = attempt_strip_and_falsify(v)
        ret_rows.append(new_d)
    return ret_rows


class CSVMultiPartParser(BaseParser):
    """
    A naive raw file upload parser.
    """
    media_type = 'multipart/form-data'  # Accept anything

    def parse(cls, stream, media_type=None, parser_context=None):
        parser_context = parser_context or {}
        request = parser_context['request']
        encoding = parser_context.get('encoding', settings.DEFAULT_CHARSET)
        meta = request.META.copy()
        meta['CONTENT_TYPE'] = media_type
        upload_handlers = request.upload_handlers

        try:
            parser = MultiPartParser(meta, stream, upload_handlers, encoding)
            _, files = parser.parse()

            # extract data from uploaded files
            data = {}
            for philename, phile in files.items():
                data[philename] = read(stream=phile,
                                       columns_to_filter=getattr(request, 'csv_filter_columns', None),
                                       as_dict=getattr(request, 'csv_with_keys', False),
                                       strip_and_falsify_value=True, )
            return data
        except Exception as exc:
            raise ParseError('Multipart form parse error - %s' % six.text_type(exc))
