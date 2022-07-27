import json
import typing
import furl
import requests

from .api_interface import RequestHandler

from .. import core


# INDEX MANAGEMENT
class CreateIndexHandler(RequestHandler):
    def __init__(self, engine: core.ElasticEngine) -> None:
        self.engine = engine

    def can_handle(self, request: requests.PreparedRequest) -> bool:
        url = furl.furl(request.url)

        return request.method == "PUT" and len(url.path.segments) == 1

    def handle(
        self, request: requests.PreparedRequest, context
    ) -> typing.Optional[str]:
        url = furl.furl(request.url)

        target = core.IndiceName.parse(url.path.segments[0])
        body = self.loads(request.body)

        self.engine.create_indice(target, **body)
        return json.dumps(
            {"acknowledge": True, "shards_acknowledge": True, "index": target}
        )


class DeleteIndexHandler(RequestHandler):
    def __init__(self, engine: core.ElasticEngine) -> None:
        self.engine = engine

    def can_handle(self, request: requests.PreparedRequest) -> bool:
        url = furl.furl(request.url)

        return request.method == "DELETE" and len(url.path.segments) == 1

    def handle(
        self, request: requests.PreparedRequest, context
    ) -> typing.Optional[str]:
        url = furl.furl(request.url)

        target = core.IndiceName.parse(url.path.segments[0])

        self.engine.delete_indice(target)

        return json.dumps({"acknowledge": True})


class ExistsHandler(RequestHandler):
    def __init__(self, engine: core.ElasticEngine) -> None:
        self.engine = engine

    def can_handle(self, request: requests.PreparedRequest) -> bool:
        url = furl.furl(request.url)

        return request.method == "HEAD" and len(url.path.segments) == 1

    def handle(
        self, request: requests.PreparedRequest, context
    ) -> typing.Optional[str]:
        url = furl.furl(request.url)

        target = url.path.segments[0]

        exists = self.engine.exists(target)
        if not exists:
            context.status_code = 404

        return None


# MAPPING MANAGERMENT
class UpdateMappingHandler(RequestHandler):
    def __init__(self, engine: core.ElasticEngine) -> None:
        self.engine = engine

    def can_handle(self, request: requests.PreparedRequest) -> bool:
        url = furl.furl(request.url)

        return (
            request.method == "PUT"
            and len(url.path.segments) == 2
            and url.path.segments[1] == "_mapping"
        )

    def handle(
        self, request: requests.PreparedRequest, context
    ) -> typing.Optional[str]:
        url = furl.furl(request.url)

        target = url.path.segments[0]
        body = self.loads(request.body)

        for resource in self.engine.resources(target):
            resource.update_mappings(body)

        return json.dumps({"acknowledge": True})


class GetMappingHandler(RequestHandler):
    def __init__(self, engine: core.ElasticEngine) -> None:
        self.engine = engine

    def can_handle(self, request: requests.PreparedRequest) -> bool:
        url = furl.furl(request.url)

        return (
            request.method == "GET"
            and len(url.path.segments) == 1
            and url.path.segments[0] == "_mapping"
        ) or (
            request.method == "GET"
            and len(url.path.segments) == 2
            and url.path.segments[1] == "_mapping"
        )

    def handle(
        self, request: requests.PreparedRequest, context
    ) -> typing.Optional[str]:
        url = furl.furl(request.url)

        if len(url.path.segments) == 1:
            resources = self.engine.resources("*")
        else:
            target = url.path.segments[0]
            resources = self.engine.resources(target)

        return json.dumps({r._id: {"mappings": r.mappings} for r in resources})
