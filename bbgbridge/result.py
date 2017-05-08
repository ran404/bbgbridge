import json
from collections import OrderedDict
from os import path

from bbgbridge.converters import convert_to_frame
from bbgbridge.util import CustomJSONEncoder


class BloombergRequestResult(object):
    def __init__(self, result, request, meta=None, converter=None):
        if meta is None:
            meta = {}
        self.result = result
        self.request = request
        self.meta = meta
        self.converter = converter

    @classmethod
    def from_dict(cls, data):
        return cls(data['result'], data.get('request'), data.get('meta'), data.get('converter'))

    @classmethod
    def from_json_file(cls, data_file):
        with open(data_file) as f:
            return cls.from_dict(json.load(f))

    def with_df_converter(self, converter):
        return BloombergRequestResult(self.result, self.request, self.meta, converter=converter)

    def to_json(self, indent=2, separators=(',', ': '), **kwargs):
        return json.dumps(self.to_dict(), cls=CustomJSONEncoder, indent=indent, separators=separators, **kwargs)

    def to_json_file(self, outfile, indent=2, separators=(',', ': '), **kwargs):
        with open(path.expanduser(outfile), 'w') as fp:
            json.dump(self.to_dict(), fp, cls=CustomJSONEncoder, indent=indent, separators=separators, **kwargs)

    def to_dataframe(self, converter=None):
        return convert_to_frame(self, converter or self.converter)

    def to_dict(self):
        return OrderedDict([
            ('meta', self.meta),
            ('converter', self.converter),
            ('request', self.request),
            ('result', self.result),
        ])

    def __repr__(self):
        return 'BloombergRequestResult for request: ' + str(self.request)[:500] + ' ...'
