import collections
import pandas as pd

from bbgbridge.util import merge_dicts, is_string


def _possible_security_error(security_error):
    return {
        'error_category': security_error['securityError'].get('category') if security_error else None,
        'error_message': security_error['securityError'].get('message') if security_error else None
    }


def _price_generator(result, raise_on_missing):
    security_data = result.get('securityData')
    if security_data:
        for x in security_data['fieldData']:
            yield merge_dicts(x['fieldData'], {'symbol': security_data['security']})
    elif raise_on_missing:
        raise RuntimeError('Bad data point detected: ' + str(result))


def _refdata_generator(result):
    return (
        merge_dicts(
            y['securityData']['fieldData']['fieldData'],
            {'symbol': y['securityData']['security']},
            _possible_security_error(y['securityData'].get('securityError'))
        )
        for z in result
        for y in z
    )


def _bulkresult_generator(result):
    for z in result:
        for y in z:
            symbol_dict = {'symbol': y['securityData']['security']}
            field_data = y['securityData']['fieldData']['fieldData']
            for k, v in field_data:
                if not is_string(v) and isinstance(v, collections.Iterable):
                    for bv in v:
                        yield merge_dicts(symbol_dict, bv[k])


def _intraday_bar_generator(result, symbol, raise_on_missing):
    bar_data = result.get('barData')
    if bar_data:
        for x in bar_data['barTickData']:
            yield merge_dicts(x['barTickData'], {'symbol': symbol})
    elif raise_on_missing:
        raise RuntimeError('Bad data point detected: ' + str(result))


# ============ Bloomberg result parsing functions ============


def price_to_frame(bbg_result, raise_on_missing=True):
    return pd.DataFrame(list(x for y in bbg_result.result for x in _price_generator(y, raise_on_missing)))


def refdata_to_frame(bbg_result):
    refdata = pd.DataFrame(list(_refdata_generator(bbg_result.result)))
    desired_columns = bbg_result.request['ReferenceDataRequest']['fields']
    available_columns = [c for c in desired_columns if c in refdata.columns]
    extra_columns = [c for c in refdata.columns if c not in desired_columns]
    return refdata[extra_columns + available_columns]


def intraday_bar_to_frame(bbg_result, raise_on_missing=True):
    symbol = bbg_result.request['IntradayBarRequest']['security']
    return pd.DataFrame(list(x for y in bbg_result.result for x in _intraday_bar_generator(y, symbol, raise_on_missing)))


def bulk_data_to_frame(bbg_result):
    return pd.DataFrame(list(_bulkresult_generator(x) for x in bbg_result.result))


def symbol_lookup_dataframe(res):
    return pd.DataFrame(
        [x['results']
         for y in res.result
         for x in y['InstrumentListResponse']['results']])


frame_converters = {
    'price': price_to_frame,
    'price_ignore_missing': lambda x: price_to_frame(x, raise_on_missing=False),
    'intraday_bar': intraday_bar_to_frame,
    'intraday_bar_ignore_missing': lambda x: intraday_bar_to_frame(x, raise_on_missing=False),
    'refdata': refdata_to_frame,
    'bulk_data': bulk_data_to_frame
}


def convert_to_frame(bbg_result, converter):
    """
    Read a bloomberg result object and converts it to a dataframe.
    The converter can one of predefined strings, or a function
    """
    if converter is None:
        raise ValueError("I need to know the converter in order to convert to DataFrame")

    converter_func = frame_converters.get(converter, converter)
    if is_string(converter_func):
        raise ValueError('converter must be one of {}, or a function, but was: {}'.format(sorted(frame_converters.keys()), converter_func))
    return converter_func(bbg_result)
