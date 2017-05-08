import pandas as pd
from blpapi import Event, Session

from .parsing import parse_message
from .result import BloombergRequestResult
from .util import (
    as_list,
    date_bloomberg_string,
    dedupe,
    to_timestamp,
    is_string,
    merge_dicts
)


def create_bloomberg_connection():
    return BloombergBridge()


def update_meta(meta, **additional):
    return merge_dicts(meta or {}, additional)


class BloombergBridge(object):
    def __init__(self):
        self.session = Session()
        self.refdata_service = None
        self.instrument_service = None
        self._init_session()

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.stop()

    def _init_session(self):
        if not self.session.start():
            raise RuntimeError('Failed to start session.')

        if not self.session.openService('//blp/refdata'):
            raise RuntimeError('Failed to open //blp/refdata')

        if not self.session.openService('//blp/instruments'):
            raise RuntimeError('Failed to open //blp/instruments')

        self.refdata_service = self.session.getService('//blp/refdata')
        self.instrument_service = self.session.getService('//blp/instruments')

    def stop(self):
        self.session.stop()

    def request_historical_data(self,
                                symbols,
                                fields,
                                start_date='1900-01-01',
                                end_date=None,
                                *,
                                periodicity_adjustment='ACTUAL',
                                periodicity_selection='DAILY',
                                overrides=None,
                                meta=None):

        if end_date is None:
            end_date = pd.Timestamp.now()

        modifiers = {
            'periodicityAdjustment': periodicity_adjustment,
            'periodicitySelection': periodicity_selection,
            'returnEids': 'true',
            'startDate': date_bloomberg_string(start_date),
            'endDate': date_bloomberg_string(end_date),
        }

        request = self.create_request('HistoricalDataRequest',
                                      as_list(symbols),
                                      as_list(fields),
                                      modifiers=modifiers,
                                      overrides=overrides)

        return self.send_request(request, meta)

    def request_intraday_bar(self,
                             symbol,
                             interval,
                             start,
                             end,
                             event_type='TRADE',
                             meta=None):

        request = self.refdata_service.createRequest("IntradayBarRequest")
        request.set("security", symbol)
        request.set("eventType", event_type)
        request.set("interval", interval)  # bar interval in minutes
        request.set("startDateTime", to_timestamp(start))
        request.set("endDateTime", to_timestamp(end))
        return self.send_request(request, meta)

    def request_reference_data(self,
                               symbols,
                               fields,
                               *,
                               overrides=None,
                               meta=None):

        request = self.create_request('ReferenceDataRequest',
                                      as_list(symbols),
                                      as_list(fields),
                                      overrides=overrides)

        return self.send_request(request, meta)

    def request_bulk_data(self,
                          symbols,
                          field,
                          *,
                          overrides=None,
                          meta=None):

        if not is_string(field):
            raise ValueError('Field must be a single string')

        request = self.create_request('ReferenceDataRequest',
                                      as_list(symbols),
                                      [field],
                                      overrides=overrides)

        return self.send_request(request, meta)

    def request_instrument_list(self,
                                symbol,
                                key_filter=None,
                                meta=None):

        request = self.instrument_service.createRequest('instrumentListRequest')
        request.set('query', symbol)
        request.set('maxResults', 100000)

        if key_filter:
            request.set('yellowKeyFilter', key_filter)

        return self.send_request(request, meta)

    def create_request(self,
                       request_type,
                       symbols,
                       fields,
                       *,
                       modifiers=None,
                       overrides=None):

        if not modifiers:
            modifiers = {}

        if not overrides:
            overrides = {}

        request = self.refdata_service.createRequest(request_type)

        for symbol in dedupe(symbols):
            request.append('securities', symbol)

        for field in dedupe(fields):
            request.append('fields', field)

        for mod_key, mod_value in modifiers.items():
            request.set(mod_key, mod_value)

        overrides_element = request.getElement('overrides')
        for override_key, override_value in overrides.items():
            oe = overrides_element.appendElement()
            oe.setElement('fieldId', override_key)
            oe.setElement('value', override_value)

        return request

    def send_request(self, request, meta=None, converter=None):
        # print('Sending Request: ' + str(request))
        request_id = self.session.sendRequest(request)

        # Convert request to object form (for easy serialization)
        req_object = parse_message(request)

        # Convert and accumulate received events
        ret_object = []

        while True:
            ev = self.session.nextEvent(timeout=500)  # For Ctrl+C handling:
            for msg in ev:
                # print(msg)
                corr_ids = msg.correlationIds()
                if len(corr_ids) > 1:
                    raise RuntimeError('Response has more than one correlation id: ' + str(msg))
                if request_id == corr_ids[0]:
                    ret_object.append(parse_message(msg))

            # Response completely received, so we could exit
            if ev.eventType() == Event.RESPONSE:
                break

        return BloombergRequestResult(ret_object, req_object, meta=meta, converter=converter)


# Excel-like Bloomberg function aliases
BloombergBridge.bdh = BloombergBridge.request_historical_data
BloombergBridge.bdp = BloombergBridge.request_reference_data
BloombergBridge.bds = BloombergBridge.request_bulk_data
BloombergBridge.bdib = BloombergBridge.request_intraday_bar
