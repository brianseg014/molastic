import json
import typing
import furl
import requests

from .api_interface import RequestHandler

from .. import core
from .. import utils
from .. import query_dsl


class SearchHandler(RequestHandler):
    def __init__(self, engine: core.ElasticEngine) -> None:
        self.engine = engine

    def can_handle(self, request: requests.PreparedRequest) -> bool:
        url = furl.furl(request.url)

        return (
            request.method in ["GET", "POST"]
            and len(url.path.segments) == 2
            and url.path.segments[1] == "_search"
        )

    def handle(
        self, request: requests.PreparedRequest, context
    ) -> typing.Optional[str]:
        url = furl.furl(request.url)

        target = url.path.segments[0]
        body = self.loads(request.body)

        with utils.timer() as elapsed:
            hits = query_dsl.search(body, self.engine.resources(target))
            took = elapsed()

        return json.dumps(
            {
                "took": took,
                "timed_out": False,
                "_shards": {
                    "total": 1,
                    "successful": 1,
                    "skipped": 0,
                    "failed": 0,
                },
                "hits": {
                    "total": {"value": len(hits), "relation": "eq"},
                    "max_score": max((h["_score"] for h in hits), default=0.0),
                    "hits": hits,
                },
            }
        )


class CountHandler(RequestHandler):
    def __init__(self, engine: core.ElasticEngine) -> None:
        self.engine = engine

    def can_handle(self, request: requests.PreparedRequest) -> bool:
        url = furl.furl(request.url)

        return (
            request.method == "GET"
            and len(url.path.segments) == 2
            and url.path.segments[1] == "_count"
        )

    def handle(
        self, request: requests.PreparedRequest, context
    ) -> typing.Optional[str]:
        url = furl.furl(request.url)

        target = url.path.segments[0]
        body = self.loads(request.body)

        count = query_dsl.count(body, self.engine.resources(target))

        return json.dumps(
            {
                "count": count,
                "_shards": {
                    "total": 1,
                    "successful": 1,
                    "skipped": 0,
                    "failed": 0,
                },
            }
        )
