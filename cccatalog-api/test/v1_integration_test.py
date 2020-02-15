import requests
import json
import pytest
import os
import uuid
import time
import cccatalog.settings
from django.db.models import Max
from django.urls import reverse
from cccatalog.api.licenses import LICENSE_GROUPS
from cccatalog.api.models import Image, OAuth2Verification
from cccatalog.api.utils.watermark import watermark

"""
End-to-end API tests. Can be used to verify a live deployment is functioning as
designed. Run with the `pytest -s` command from this directory.
"""

API_URL = os.getenv('INTEGRATION_TEST_URL', 'http://localhost:8000')
known_apis = {
    'http://localhost:8000': 'LOCAL',
    'https://api.creativecommons.engineering': 'PRODUCTION',
    'https://api-dev.creativecommons.engineering': 'TESTING'
}


def setup_module():
    if API_URL in known_apis:
        print(
            '\n\033[1;31;40mTesting {} environment'.format(known_apis[API_URL])
        )


@pytest.fixture
def search_fixture():
    response = requests.get(API_URL + '/v1/images?q=dog',
                            verify=False)
    assert response.status_code == 200
    parsed = json.loads(response.text)
    return parsed


def test_search_quotes():
    """
    We want to return a response even if the user messes up quote matching.
    """
    response = requests.get(API_URL + '/v1/images?q="test', verify=False)
    assert response.status_code == 200


def test_search_with_special_characters():
    response = requests.get(API_URL + '/v1/images?q=dog!', verify=False)
    assert response.status_code == 200


def test_search(search_fixture):
    assert search_fixture['result_count'] > 0


def test_search_consistency():
    """
    Elasticsearch sometimes reaches an inconsistent state, which causes search
    results to appear differently upon page refresh. This can also introduce
    image duplicates in subsequent pages. This test ensures that no duplicates
    appear in the first few pages of a search query.
    """
    n_pages = 5
    searches = set(
        requests.get(API_URL + '/v1/images?q=dog;page={}'.format(page),
                     verify=False)
        for page in range(1, n_pages)
    )

    images = set()
    for response in searches:
        parsed = json.loads(response.text)
        for result in parsed['results']:
            image_id = result['id']
            assert image_id not in images
            images.add(image_id)


def test_image_detail(search_fixture):
    test_id = search_fixture['results'][0]['id']
    response = requests.get(API_URL + '/v1/images/{}'.format(test_id), verify=False)
    assert response.status_code == 200


def test_image_delete_invalid_creds(search_fixture):
    test_id = search_fixture['results'][0]['id']
    should_fail = requests.delete(
        f'{API_URL}/v1/images/{test_id}',
        auth=('invalid', 'credentials'),
        verify=False
    )
    assert should_fail.status_code == 401


def test_image_delete(search_fixture):
    test_id = search_fixture['results'][0]['id']
    response = requests.delete(
        f'{API_URL}/v1/images/{test_id}',
        auth=('continuous_integration', 'deploydeploy'),
        verify=False
    )
    assert response.status_code == 204
    deleted_response = requests.get(f'{API_URL}/image/{test_id}')
    assert deleted_response.status_code == 404


@pytest.fixture
def link_shortener_fixture(search_fixture):
    link_to_shorten = search_fixture['results'][0]['detail_url']
    payload = {"full_url": link_to_shorten}
    response = requests.post(API_URL + '/v1/link', json=payload, verify=False)
    assert response.status_code == 200
    return json.loads(response.text)


def test_link_shortener_create(link_shortener_fixture):
    assert 'shortened_url' in link_shortener_fixture


def test_link_shortener_resolve(link_shortener_fixture):
    path = link_shortener_fixture['shortened_url'].split('/')[-1]
    response = requests.get(API_URL + '/v1/link/' + path, allow_redirects=False,
                            verify=False)
    assert response.status_code == 301


def test_stats():
    response = requests.get(API_URL + '/v1/sources?type=images', verify=False)
    parsed_response = json.loads(response.text)
    assert response.status_code == 200
    num_images = 0
    source_count = 0
    for pair in parsed_response:
        image_count = pair['image_count']
        num_images += int(image_count)
        source_count += 1
    assert num_images > 0
    assert source_count > 0


@pytest.mark.skip(reason="Disabled feature")
@pytest.fixture
def test_list_create(search_fixture):
    payload = {
        'title': 'INTEGRATION TEST',
        'images': [search_fixture['results'][0]['id']]
    }
    response = requests.post(API_URL + '/list', json=payload, verify=False)
    parsed_response = json.loads(response.text)
    assert response.status_code == 201
    return parsed_response


@pytest.mark.skip(reason="Disabled feature")
def test_list_detail(test_list_create):
    list_slug = test_list_create['url'].split('/')[-1]
    response = requests.get(
        API_URL + '/list/{}'.format(list_slug), verify=False
    )
    assert response.status_code == 200


@pytest.mark.skip(reason="Disabled feature")
def test_list_delete(test_list_create):
    list_slug = test_list_create['url'].split('/')[-1]
    token = test_list_create['auth']
    headers = {"Authorization": "Token {}".format(token)}
    response = requests.delete(
        API_URL + '/list/{}'.format(list_slug),
        headers=headers,
        verify=False
    )
    assert response.status_code == 204


def test_license_type_filtering():
    """
    Ensure that multiple license type filters interact together correctly.
    """
    commercial = LICENSE_GROUPS['commercial']
    modification = LICENSE_GROUPS['modification']
    commercial_and_modification = set.intersection(modification, commercial)
    response = requests.get(
        API_URL + '/v1/images?q=dog&license_type=commercial,modification',
        verify=False
    )
    parsed = json.loads(response.text)
    for result in parsed['results']:
        assert result['license'].upper() in commercial_and_modification


def test_single_license_type_filtering():
    commercial = LICENSE_GROUPS['commercial']
    response = requests.get(
        API_URL + '/v1/images?q=dog&license_type=commercial', verify=False
    )
    parsed = json.loads(response.text)
    for result in parsed['results']:
        assert result['license'].upper() in commercial


def test_specific_license_filter():
    response = requests.get(
        API_URL + '/v1/images?q=dog&license=by', verify=False
    )
    parsed = json.loads(response.text)
    for result in parsed['results']:
        assert result['license'] == 'by'


def test_creator_quotation_grouping():
    """
    Users should be able to group terms together with quotation marks to narrow
    down their searches more effectively.
    """
    no_quotes = json.loads(
        requests.get(
            API_URL + '/v1/images?creator=claude%20monet',
            verify=False
        ).text
    )
    quotes = json.loads(
        requests.get(
            API_URL + '/v1/images?creator="claude%20monet"',
            verify=False
        ).text
    )
    # Did quotation marks actually narrow down the search?
    assert len(no_quotes['results']) > len(quotes['results'])
    # Did we find only Claude Monet works, or did his lesser known brother Jim
    # Monet sneak into the results?
    for result in quotes['results']:
        assert 'Claude Monet' in result['creator']


@pytest.fixture
def test_auth_tokens_registration():
    payload = {
        'name': 'INTEGRATION TEST APPLICATION {}'.format(uuid.uuid4()),
        'description': 'A key for testing the OAuth2 registration process.',
        'email': 'example@example.org'
    }
    response = requests.post(
        API_URL + '/v1/auth_tokens/register', json=payload, verify=False
    )
    parsed_response = json.loads(response.text)
    assert response.status_code == 201
    return parsed_response


@pytest.fixture
def test_auth_token_exchange(test_auth_tokens_registration):
    client_id = test_auth_tokens_registration['client_id']
    client_secret = test_auth_tokens_registration['client_secret']
    token_exchange_request = \
        'client_id={_id}&client_secret={secret}&grant_type=client_credentials' \
        .format(_id=client_id, secret=client_secret)
    headers = {
        'content-type': "application/x-www-form-urlencoded",
        'cache-control': "no-cache",
    }
    response = json.loads(
        requests.post(
            API_URL + '/v1/auth_tokens/token/',
            data=token_exchange_request,
            headers=headers,
            verify=False
        ).text
    )
    assert 'access_token' in response
    return response


def test_auth_rate_limit_reporting(test_auth_token_exchange, verified=False):
    # We're anonymous still, so we need to wait a second before exchanging
    # the token.
    time.sleep(1)
    token = test_auth_token_exchange['access_token']
    headers = {
        'Authorization': f'Bearer {token}'
    }
    response = json.loads(
        requests.get(f'{API_URL}/v1/rate_limit', headers=headers).text
    )
    if verified:
        assert response['rate_limit_model'] == 'standard'
        assert response['verified'] is True
    else:
        assert response['rate_limit_model'] == 'standard'
        assert response['verified'] is False


@pytest.fixture(scope='session')
def django_db_setup():
    if API_URL == 'http://localhost:8000':
        cccatalog.settings.DATABASES['default'] = {
            'ENGINE': 'django.db.backends.postgresql',
            'HOST': '127.0.0.1',
            'NAME': 'openledger',
            'PASSWORD': 'deploy',
            'USER': 'deploy',
            'PORT': 5432
        }


@pytest.mark.django_db
def test_auth_email_verification(test_auth_token_exchange, django_db_setup):
        # This test needs to cheat by looking in the database, so it will be
        # skipped in non-local environments.
        if API_URL == 'http://localhost:8000':
            _id = OAuth2Verification.objects.aggregate(Max('id'))['id__max']
            verify = OAuth2Verification.objects.get(id=_id)
            code = verify.code
            path = reverse('verify-email', args=[code])
            url = f'{API_URL}{path}'
            response = requests.get(url)
            assert response.status_code == 200
            test_auth_rate_limit_reporting(
                test_auth_token_exchange, verified=True
            )


@pytest.mark.skip(reason="Unmaintained feature/grequests ssl recursion bug")
def test_watermark_preserves_exif():
    img_with_exif = 'https://raw.githubusercontent.com/ianare/exif-samples/' \
                    'master/jpg/Canon_PowerShot_S40.jpg'
    info = {
        'title': 'test',
        'creator': 'test',
        'license': 'test',
        'license_version': 'test'
    }
    _, exif = watermark(image_url=img_with_exif, info=info)
    assert exif is not None

    img_no_exif = 'https://creativecommons.org/wp-content/uploads/' \
                  '2019/03/9467312978_64cd5d2f3b_z.jpg'
    _, no_exif = watermark(image_url=img_no_exif, info=info)
    assert no_exif is None


def test_attribution():
    """
    The API includes an attribution string. Since there are some works where
    the title or creator is not known, the format of the attribution string
    can need to be tweaked slightly.
    """
    title_and_creator_missing = Image(
        identifier="ab80dbe1-414c-4ee8-9543-f9599312aeb8",
        title=None,
        creator=None,
        license="by",
        license_version="3.0"
    )
    assert "This work" in title_and_creator_missing.attribution

    title = "A foo walks into a bar"
    creator_missing = Image(
        identifier="ab80dbe1-414c-4ee8-9543-f9599312aeb8",
        title=title,
        creator=None,
        license="by",
        license_version="3.0"
    )
    assert title in creator_missing.attribution
    assert "by " not in creator_missing.attribution

    creator = "John Doe"
    title_missing = Image(
        identifier="ab80dbe1-414c-4ee8-9543-f9599312aeb8",
        title=None,
        creator=creator,
        license="by",
        license_version="3.0"
    )
    assert creator in title_missing.attribution
    assert "This work" in title_missing.attribution

    all_data_present = Image(
        identifier="ab80dbe1-414c-4ee8-9543-f9599312aeb8",
        title=title,
        creator=creator,
        license="by",
        license_version="3.0"
    )
    assert title in all_data_present.attribution
    assert creator in all_data_present.attribution


def test_source_search():
    response = requests.get(API_URL + '/v1/images?source=behance',
                            verify=False)
    assert response.status_code == 200
    parsed = json.loads(response.text)
    assert parsed['result_count'] > 0


def test_extension_filter():
    response = requests.get(API_URL + '/v1/images?q=dog&extension=jpg')
    parsed = json.loads(response.text)
    for result in parsed['results']:
        assert '.jpg' in result['url']


@pytest.fixture
def search_factory():
    """
    Allows passing url parameters along with a search request.
    """
    def _parameterized_search(**kwargs):
        response = requests.get(
            API_URL + '/v1/images',
            params=kwargs,
            verify=False
        )
        assert response.status_code == 200
        parsed = response.json()
        return parsed
    return _parameterized_search


@pytest.fixture
def search_with_dead_links(search_factory):
    """
    Here we pass filter_dead = False.
    """
    def _search_with_dead_links(**kwargs):
        return search_factory(filter_dead=False, **kwargs)
    return _search_with_dead_links


@pytest.fixture
def search_without_dead_links(search_factory):
    """
    Here we pass filter_dead = True.
    """
    def _search_without_dead_links(**kwargs):
        return search_factory(filter_dead=True, **kwargs)
    return _search_without_dead_links


def test_page_size_removing_dead_links(search_without_dead_links):
    """
    We have about 500 dead links in the sample data and should have around
    8 dead links in the first 100 results on a query composed of a single
    wildcard operator.

    Test whether the number of results returned is equal to the requested
    page_size of 100.
    """
    data = search_without_dead_links(q='*', page_size=100)
    assert len(data['results']) == 100


def test_dead_links_are_correctly_filtered(search_with_dead_links,
                                           search_without_dead_links):
    """
    Test the results for the same query with and without dead links are
    actually different.

    We use the results' id to compare them.
    """
    data_with_dead_links = search_with_dead_links(q='*', page_size=100)
    data_without_dead_links = search_without_dead_links(q='*', page_size=100)

    comparisons = []
    for result_1 in data_with_dead_links['results']:
        for result_2 in data_without_dead_links['results']:
            comparisons.append(result_1['id'] == result_2['id'])

    # Some results should be different
    # so we should have less than 100 True comparisons
    assert comparisons.count(True) < 100


def test_page_consistency_removing_dead_links(search_without_dead_links):
    """
    Test the results returned in consecutive pages are never repeated when
    filtering out dead links.
    """
    total_pages = 30
    page_size = 5

    page_results = []
    for page in range(1, total_pages + 1):
        page_data = search_without_dead_links(
            q='*',
            page_size=page_size,
            page=page
        )
        page_results += page_data['results']

    def no_duplicates(l):
        s = set()
        for x in l:
            if x in s:
                return False
            s.add(x)
        return True

    ids = list(map(lambda x: x['id'], page_results))
    # No results should be repeated so we should have no duplicate ids
    assert no_duplicates(ids)


@pytest.fixture
def recommendation_factory():
    """
    Allows passing url parameters along with a related images request.
    """
    def _parameterized_search(identifier, **kwargs):
        response = requests.get(
            API_URL + f'/v1/recommendations?type=images&id={identifier}',
            params=kwargs,
            verify=False
        )
        assert response.status_code == 200
        parsed = response.json()
        return parsed
    return _parameterized_search


@pytest.mark.skip(reason="Generally, we don't paginate related images, so "
                    "consistency is less of an issue.")
def test_related_image_search_page_consistency(recommendation, search_without_dead_links):
    initial_images = search_without_dead_links(q='*', page_size=10)
    for image in initial_images['results']:
        related = recommendation_factory(image['id'])
        assert related['result_count'] > 0
        assert len(related['results']) == 10
