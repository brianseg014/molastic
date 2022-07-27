import furl
import requests

from molastic import mock_elasticsearch


@mock_elasticsearch("mock://molastic")
def test_create_index():
    url = furl.furl("mock://molastic", path="my-index")

    response = requests.put(str(url), json={})
    assert response.status_code == 200


@mock_elasticsearch("mock://molastic")
def test_create_index_with_mapping():
    url = furl.furl("mock://molastic", path="my-index")

    response = requests.put(
        str(url),
        json={"mappings": {"properties": {"field": {"type": "keyword"}}}},
    )
    assert response.status_code == 200

@mock_elasticsearch("mock://molastic")
def test_create_index_with_mapping():
    url = furl.furl("mock://molastic", path="my-index")

    # Create index first
    response = requests.put(str(url), json={})
    assert response.status_code == 200

    response = requests.head(str(url), json={})
    assert response.status_code == 200

    url = furl.furl("mock://molastic", path="my-unknown-index")
    response = requests.head(str(url), json={})
    assert response.status_code == 404


@mock_elasticsearch("mock://molastic")
def test_update_index_mapping():
    url = furl.furl("mock://molastic", path="my-index")

    # Create index first
    response = requests.put(str(url), json={})
    assert response.status_code == 200

    # Update mappings
    mapping_url = furl.furl(str(url), path=url.path).add(path="_mapping")
    response = requests.put(
        str(mapping_url),
        json={"properties": {"field": {"type": "keyword"}}},
    )
    assert response.status_code == 200


@mock_elasticsearch("mock://molastic")
def test_delete_index():
    url = furl.furl("mock://molastic", path="my-index")

    # Create index first
    response = requests.put(str(url), json={})
    assert response.status_code == 200

    response = requests.delete(str(url), json={})
    assert response.status_code == 200


@mock_elasticsearch("mock://molastic")
def test_delete_index():
    url = furl.furl("mock://molastic", path="my-index")

    # Create index first
    response = requests.put(str(url), json={})
    assert response.status_code == 200

    mapping_url = furl.furl(str(url), path=url.path).add(path='_mapping')
    response = requests.get(str(mapping_url), json={})
    assert response.status_code == 200
