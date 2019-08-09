from builtins import str
from builtins import map
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
        iter(value.items())
    ))

def decode_dict(value):
    return dict(itertools.starmap(
        lambda k, v: (k, decode(v)),
        iter(value.items())
    ))

encodings = {
    type(None): lambda value: 'None',
    int: lambda value: 'i_' + str(value),
    int: lambda value: 'i_' + str(value),
    float: lambda value: 'f_' + str(value),
    bool: lambda value: 'b_' + str(int(value)),
    Decimal: lambda value: 'd_' + str(value),
    str: lambda value: 'u_' + _escape(value.encode('base-64')),
    str: lambda value: 'u_' + _escape(value.encode('utf-8').encode('base-64')),
    datetime: lambda value: 't_%i.%06i'%(time.mktime(value.timetuple()), value.microsecond),
    date: lambda value: 'a_%i'%(time.mktime(value.timetuple())),
    tuple: lambda value: 'l_' + '_'.join([_escape(encode(a)) for a in value]),
    list: lambda value: 'l_' + '_'.join([_escape(encode(a)) for a in value]),
    dict: lambda value: 'h_' + '_'.join(['%s:%s' % (_escape(encode(a[0])), _escape(encode(a[1]))) for a in list(value.items())]),
}

decodings = {
    'N': lambda value: None,
    'i': int,
    'f': float,
    'b': lambda value: bool(int(value)),
    'd': Decimal,
    'u': lambda value: _unescape(value).decode('base-64').decode('utf-8'),
    't': lambda value: datetime.fromtimestamp(float(value)),
    'a': lambda value: date.fromtimestamp(float(value)),
    'l': lambda value: list(map(decode, list(map(_unescape, value.split('_'))))),
    'h': lambda value: dict([list(map(decode, list(map(_unescape, a.split(':'))))) for a in value.split('_')]),
}

def _escape(value):
    return value.replace('|', '||').replace('\n', '|n').replace('_', '|u')

def _unescape(value):
    return value.replace('||', '|').replace('|n', '\n').replace('|u', '_')
