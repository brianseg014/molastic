import json
import typing
import furl
import requests

from .api_interface import RequestHandler

from .. import core


# SINGLE DOCUMENT MAPPING


class AddOrOverwriteDocumentHandler(RequestHandler):
    def __init__(self, engine: core.ElasticEngine) -> None:
        self.engine = engine

    def can_handle(self, request: requests.PreparedRequest) -> bool:
        url = furl.furl(request.url)

        return (
            request.method == "PUT"
            and len(url.path.segments) == 3
            and url.path.segments[1] == "_doc"
        )

    def handle(
        self, request: requests.PreparedRequest, context
    ) -> typing.Optional[str]:
        url = furl.furl(request.url)

        target = core.IndiceName.parse(url.path.segments[0])
        body = self.loads(request.body)
        id = url.path.segments[2]
        op_type = core.OperationType.INDEX

        indice = self.engine.indice(target, autocreate=True)

        document, operation_result = indice.index(
            body=body, id=id, op_type=op_type
        )

        if operation_result == operation_result.CREATED:
            context.status_code = 201

        return json.dumps(
            {
                "shards": {"total": 1, "successful": 1, "failed": 0},
                "_index": indice._id,
                "_type": "_doc",
                "_id": document["_id"],
                "_version": document["_version"],
                "result": operation_result.value,
                "_seq_no": document["_seq_no"],
                "_primary_term": document["_primary_term"],
            }
        )


class AddDocumentHandler(RequestHandler):
    def __init__(self, engine: core.ElasticEngine) -> None:
        self.engine = engine

    def can_handle(self, request: requests.PreparedRequest) -> bool:
        url = furl.furl(request.url)

        return (
            request.method == "POST"
            and len(url.path.segments) == 2
            and url.path.segments[1] == "_doc"
        )

    def handle(
        self, request: requests.PreparedRequest, context
    ) -> typing.Optional[str]:
        url = furl.furl(request.url)

        target = core.IndiceName.parse(url.path.segments[0])
        body = self.loads(request.body)

        indice = self.engine.indice(target, autocreate=True)

        document, operation_result = indice.index(body=body)

        if operation_result == operation_result.CREATED:
            context.status_code = 201

        return json.dumps(
            {
                "shards": {"total": 1, "successful": 1, "failed": 0},
                "_index": indice._id,
                "_type": "_doc",
                "_id": document["_id"],
                "_version": document["_version"],
                "result": operation_result.value,
                "_seq_no": document["_seq_no"],
                "_primary_term": document["_primary_term"],
            }
        )


class UpdateDocumentHandler(RequestHandler):
    def __init__(self, engine: core.ElasticEngine) -> None:
        self.engine = engine

    def can_handle(self, request: requests.PreparedRequest) -> bool:
        url = furl.furl(request.url)

        return (
            request.method == "POST"
            and len(url.path.segments) == 3
            and url.path.segments[1] == "_update"
        )

    def handle(
        self, request: requests.PreparedRequest, context
    ) -> typing.Optional[str]:
        url = furl.furl(request.url)

        target = core.IndiceName.parse(url.path.segments[0])
        body = self.loads(request.body)
        id = url.path.segments[2]

        indice = self.engine.indice(target, autocreate=True)

        document, operation_result = indice.update(body=body, id=id)

        return json.dumps(
            {
                "shards": {"total": 1, "successful": 1, "failed": 0},
                "_index": indice._id,
                "_type": "_doc",
                "_id": document["_id"],
                "_version": document["_version"],
                "result": operation_result.value,
                "_seq_no": document["_seq_no"],
                "_primary_term": document["_primary_term"],
            }
        )


class DeleteDocumentHandler(RequestHandler):
    def __init__(self, engine: core.ElasticEngine) -> None:
        self.engine = engine

    def can_handle(self, request: requests.PreparedRequest) -> bool:
        url = furl.furl(request.url)

        return (
            request.method == "DELETE"
            and len(url.path.segments) == 3
            and url.path.segments[1] == "_doc"
        )

    def handle(
        self, request: requests.PreparedRequest, context
    ) -> typing.Optional[str]:
        url = furl.furl(request.url)

        target = core.IndiceName.parse(url.path.segments[0])
        id = url.path.segments[2]

        indice = self.engine.indice(target, autocreate=True)

        document, operation_result = indice.delete(id=id)

        if document is None:
            return json.dumps(
                {
                    "shards": {"total": 1, "successful": 1, "failed": 0},
                    "_index": indice._id,
                    "_type": "_doc",
                    "_id": id,
                    "_version": 1,
                    "result": operation_result.value,
                    "_seq_no": 0,
                    "_primary_term": 1,
                }
            )

        return json.dumps(
            {
                "shards": {"total": 1, "successful": 1, "failed": 0},
                "_index": indice._id,
                "_type": "_doc",
                "_id": document["_id"],
                "_version": document["_version"],
                "result": operation_result.value,
                "_seq_no": document["_seq_no"],
                "_primary_term": document["_primary_term"],
            }
        )


class GetDocumentHandler(RequestHandler):
    def __init__(self, engine: core.ElasticEngine) -> None:
        self.engine = engine

    def can_handle(self, request: requests.PreparedRequest) -> bool:
        url = furl.furl(request.url)

        return (
            request.method == "GET"
            and len(url.path.segments) == 3
            and url.path.segments[1] == "_doc"
        )

    def handle(
        self, request: requests.PreparedRequest, context
    ) -> typing.Optional[str]:
        url = furl.furl(request.url)

        target = core.IndiceName.parse(url.path.segments[0])
        id = url.path.segments[2]

        indice = self.engine.indice(target, autocreate=True)

        document = indice.get(id)

        if document is None:
            context.status_code = 404
            return json.dumps(
                {
                    "_index": indice._id,
                    "_type": "_doc",
                    "_id": id,
                    "found": False,
                }
            )
        else:
            return json.dumps(
                {
                    "_index": indice._id,
                    "_type": document["_type"],
                    "_id": document["_id"],
                    "_version": document["_version"],
                    "_seq_no": document["_seq_no"],
                    "_primary_term": document["_primary_term"],
                    "_found": True,
                    "_source": document["_source"],
                }
            )


class ExistsDocumentHandler(RequestHandler):
    def __init__(self, engine: core.ElasticEngine) -> None:
        self.engine = engine

    def can_handle(self, request: requests.PreparedRequest) -> bool:
        url = furl.furl(request.url)

        return (
            request.method == "HEAD"
            and len(url.path.segments) == 3
            and url.path.segments[1] == "_doc"
        )
    
    def handle(self, request: requests.PreparedRequest, context) -> str | None:
        url = furl.furl(request.url)

        target = core.IndiceName.parse(url.path.segments[0])
        id = url.path.segments[2]

        indice = self.engine.indice(target, autocreate=True)

        if indice.exists(id):
            context.status_code = 200
        else:
            context.status_code = 404
