import datetime
import unittest

import numpy as np
import numpy.testing as npt
import pandas as pd
from collections import OrderedDict

from bbgbridge.converters import price_to_frame
from bbgbridge.result import BloombergRequestResult
from bbgbridge.util import to_timestamp


def assert_dict_in_series(testcase, under_test, expected_dict):
    """ NB: numpy arrays area asserted with assert_almost_equal """
    for expected_key, expected_value in expected_dict.items():
        actual_value = under_test[expected_key]
        if isinstance(actual_value, np.ndarray):
            npt.assert_almost_equal(actual_value, expected_value)
        elif isinstance(actual_value, float):
            if np.isnan(actual_value):
                testcase.assertTrue(np.isnan(expected_value), msg='Error: ' + expected_key)
            else:
                testcase.assertAlmostEqual(expected_value, actual_value, msg='Error: ' + expected_key)
        elif actual_value is pd.NaT:
            testcase.assertIs(actual_value, expected_value)
        else:
            testcase.assertEqual(expected_value, actual_value, 'Error for ' + expected_key)


class BloombergRequestResultTest(unittest.TestCase):
    def setUp(self):
        result = [{'securityData': OrderedDict(
            [('security', 'SPY US Equity'), ('eidData', []), ('sequenceNumber', 0), ('fieldExceptions', []), (
                'fieldData', [{'fieldData': OrderedDict([('date', datetime.date(2015, 8, 25)), ('PX_LOW', 186.92)])},
                              {'fieldData': OrderedDict([('date', datetime.date(2015, 8, 26)), ('PX_LOW', 188.37)])},
                              {'fieldData': OrderedDict([('date', datetime.date(2015, 8, 27)), ('PX_LOW', 195.21)])},
                              {'fieldData': OrderedDict([('date', datetime.date(2015, 8, 28)), ('PX_LOW', 197.92)])}])])}]
        self.bbg_result = BloombergRequestResult(result, {'test_request': 'test_request_params'},
                                                 meta=OrderedDict([('type', 'HistoricalDataRequest'), ('subtype', 'futures')]))

    def test_to_dataframe_no_converter(self):
        new_bbg_result = self.bbg_result.with_df_converter(None)
        self.assertRaisesRegex(ValueError, "I need to know the converter in order to convert to DataFrame", new_bbg_result.to_dataframe)

    def test_to_dataframe_with_converter(self):
        new_bbg_result = self.bbg_result.with_df_converter(price_to_frame)
        df = new_bbg_result.to_dataframe()
        self.assertEqual(len(df), 4)
        assert_dict_in_series(self, df.loc[0], {
            'symbol': 'SPY US Equity',
            'date': to_timestamp('2015-08-25').date(),
            'PX_LOW': 186.92
        })
        assert_dict_in_series(self, df.loc[3], {
            'symbol': 'SPY US Equity',
            'date': to_timestamp('2015-08-28').date(),
            'PX_LOW': 197.92
        })

    def test_to_dict(self):
        to_dict = self.bbg_result.to_dict()
        self.assertDictEqual({'type': 'HistoricalDataRequest', 'subtype': 'futures'}, to_dict['meta'])

    def test_serialize_to_json(self):
        expected_json = '''{
  "meta": {
    "type": "HistoricalDataRequest",
    "subtype": "futures"
  },
  "converter": null,
  "request": {
    "test_request": "test_request_params"
  },
  "result": [
    {
      "securityData": {
        "security": "SPY US Equity",
        "eidData": [],
        "sequenceNumber": 0,
        "fieldExceptions": [],
        "fieldData": [
          {
            "fieldData": {
              "date": "2015-08-25",
              "PX_LOW": 186.92
            }
          },
          {
            "fieldData": {
              "date": "2015-08-26",
              "PX_LOW": 188.37
            }
          },
          {
            "fieldData": {
              "date": "2015-08-27",
              "PX_LOW": 195.21
            }
          },
          {
            "fieldData": {
              "date": "2015-08-28",
              "PX_LOW": 197.92
            }
          }
        ]
      }
    }
  ]
}'''
        actual_json = self.bbg_result.to_json()
        # print('******* expected *******\n{}\n******* expected *******'.format(expected_json), file=sys.stderr)
        # print('******* actual *******\n{}\n******* actual *******'.format(actual_json), file=sys.stderr)
        self.assertEqual(expected_json, actual_json)
