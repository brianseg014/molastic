import furl
import requests

from molastic import mock_elasticsearch


@mock_elasticsearch("mock://molastic")
def test_add_or_override_document_add():
    url = furl.furl("mock://molastic", path="my-index/_doc/1")

    response = requests.put(str(url), json={"user": {"id": "molastic"}})
    assert response.status_code == 200


@mock_elasticsearch("mock://molastic")
def test_add_or_override_document_override():
    url = furl.furl("mock://molastic", path="my-index/_doc/1")

    response = requests.put(str(url), json={"user": {"id": "molastic"}})
    assert response.status_code == 200

    response = requests.put(str(url), json={"user": {"id": "molastic"}})
    assert response.status_code == 200


@mock_elasticsearch("mock://molastic")
def test_add_document():
    url = furl.furl("mock://molastic", path="my-index/_doc")

    response = requests.post(str(url), json={"user": {"id": "molastic"}})
    assert response.status_code == 200


@mock_elasticsearch("mock://molastic")
def test_delete_document():
    url = furl.furl("mock://molastic", path="my-index/_doc/1")

    response = requests.put(str(url), json={"user": {"id": "molastic"}})
    assert response.status_code == 200

    response = requests.delete(str(url), json={})
    assert response.status_code == 200


@mock_elasticsearch("mock://molastic")
def test_update_document():
    url = furl.furl("mock://molastic", path="my-index/_doc/1")

    response = requests.put(str(url), json={"user": {"id": "molastic"}})
    assert response.status_code == 200

    update_url = furl.furl(str(url), path="my-index/_update/1")
    response = requests.post(
        str(update_url),
        json={
            "script": {
                "source": "ctx['_source']['user']['id'] = params['id'];",
                "params": {"id": "molastic-2"},
            }
        },
    )
    assert response.status_code == 200
