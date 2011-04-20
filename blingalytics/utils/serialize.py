from datetime import date, datetime
from decimal import Decimal
import itertools
import time


def encode(value):
    encoder = encodings.get(type(value))
    if encoder is None:
        raise ValueError('Can\'t encode type: %s' % type(value))
    return encoder(value)

def decode(value):
    decoder = decodings.get(value[:1])
    if decoder is None:
        raise ValueError('Can\'t decode value of unknown type: %s' % value)
    return decoder(value[2:])

def encode_dict(value):
    return dict(itertools.starmap(
        lambda k, v: (k, encode(v)),
        value.iteritems()
    ))

def decode_dict(value):
    return dict(itertools.starmap(
        lambda k, v: (k, decode(v)),
        value.iteritems()
    ))

encodings = {
    type(None): lambda value: 'None',
    int: lambda value: 'i_' + str(value),
    long: lambda value: 'i_' + str(value),
    float: lambda value: 'f_' + str(value),
    bool: lambda value: 'b_' + str(int(value)),
    Decimal: lambda value: 'd_' + str(value),
    str: lambda value: 'u_' + _escape(value.encode('base-64')),
    unicode: lambda value: 'u_' + _escape(value.encode('utf-8').encode('base-64')),
    datetime: lambda value: 't_%i.%06i'%(time.mktime(value.timetuple()), value.microsecond),
    date: lambda value: 'a_%i'%(time.mktime(value.timetuple())),
    tuple: lambda value: 'l_' + '_'.join(map(lambda a: _escape(encode(a)), value)),
    list: lambda value: 'l_' + '_'.join(map(lambda a: _escape(encode(a)), value)),
    dict: lambda value: 'h_' + '_'.join(map(lambda a: '%s:%s' % (_escape(encode(a[0])), _escape(encode(a[1]))), value.items())),
}

decodings = {
    'N': lambda value: None,
    'i': int,
    'f': float,
    'b': lambda value: bool(int(value)),
    'd': Decimal,
    'u': lambda value: _unescape(value.decode('base-64').decode('utf-8')),
    't': lambda value: datetime.fromtimestamp(float(value)),
    'a': lambda value: date.fromtimestamp(float(value)),
    'l': lambda value: map(decode, value.split('_')),
    'h': lambda value: dict(map(lambda a: map(decode, map(_unescape, a.split(':'))), value.split('_'))),
}

def _escape(value):
    return value.replace('|', '||').replace('\n', '|n').replace('_', '|u')

def _unescape(value):
    return value.replace('||', '|').replace('|n', '\n').replace('|u', '_')
