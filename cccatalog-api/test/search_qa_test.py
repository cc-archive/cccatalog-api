import requests
import pprint
import json
import pytest
from enum import Enum
from .api_live_integration_test import API_URL

"""
Perform some basic tests to ensure that search rankings work as anticipated.
"""


class QAScores(Enum):
    TARGET = 1
    LESS_RELEVANT = 2
    NOT_RELEVANT = 3


@pytest.mark.skip(reason="This test is nondeterministic")
def test_phrase_relevance():
    res = requests.get(
        "{}/image/search?q=home office&filter_dead=false&qa=true"
        .format(API_URL)
    )
    parsed = json.loads(res.text)
    pprint.pprint(parsed)
    assert int(parsed['results'][0]['id']) == QAScores.TARGET.value
    assert int(parsed['results'][1]['id']) < QAScores.NOT_RELEVANT.value
    assert int(parsed['results'][-1]['id']) != QAScores.NOT_RELEVANT.value
