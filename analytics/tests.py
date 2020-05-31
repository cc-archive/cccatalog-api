import pytest
import uuid
import requests
import os
"""
End-to-end tests of the analytics server. Run with `pytest -s`.
"""


API_URL = os.getenv('ANALYTICS_SERVER_URL', 'http://localhost:8090')
session_id = '00000000-0000-0000-0000-000000000000'
result_id = '11111111-1111-1111-1111-111111111111'
test_query = 'integration test'


def test_search_event():
    body = {
        'query': test_query,
        'session_uuid': session_id
    }
    response = requests.post(API_URL + '/search_event', json=body, verify=False)
    assert response.status_code == 201


def test_search_rating():
    body = {
        'query': test_query,
        'relevant': True
    }
    response = requests.post(
        API_URL + '/search_rating_event', json=body, verify=False
    )
    assert response.status_code == 201

    invalid_rating = {
        'query': test_query,
        'relevant': 6
    }
    bad_response = requests.post(
        API_URL + '/search_rating_event', json=invalid_rating, verify=False
    )
    assert bad_response.status_code == 400


def test_result_clicked():
    body = {
        'query': test_query,
        'session_uuid': session_id,
        'result_uuid': result_id,
        'result_rank': 0
    }
    response = requests.post(
        API_URL + '/result_click_event', json=body, verify=False
    )
    assert response.status_code == 201


def test_detail_event():
    body = {
        'event_type': 'SHARED_SOCIAL',
        'result_uuid': result_id
    }
    response = requests.post(
        API_URL + '/detail_page_event', json=body, verify=False
    )
    assert response.status_code == 201

    invalid_event = {
        'event_type': 'FOO',
        'result_uuid': result_id
    }
    bad_response = requests.post(
        API_URL + '/detail_page_event', json=invalid_event, verify=False
    )
    assert bad_response.status_code == 400

