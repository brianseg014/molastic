import contextlib

from .rest_api import ElasticApiMock


@contextlib.contextmanager
def mock_elasticsearch(endpoint: str):
    api = ElasticApiMock(endpoint)
    try:
        api.start()
        yield api
    finally:
        api.stop()
