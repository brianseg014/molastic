import re
import json
import dataclasses
import furl
import requests
import requests_mock

from . import core
from . import query_dsl
from . import utils


class JSONEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, utils.CaseInsensitveEnum):
            return o.value.lower()
        return super().default(o)


class ElasticApiMock:
    def __init__(self, endpoint: str) -> None:
        self.engine = core.ElasticEngine()
        self.mocker = requests_mock.Mocker()

        self.mocker.register_uri(
            method=requests_mock.ANY,
            url=re.compile(f"^{endpoint}.*$"),
            text=self.handle_request,
        )

    def start(self) -> None:
        self.mocker.start()

    def stop(self) -> None:
        self.mocker.stop()

    def handle_request(
        self, request: requests.PreparedRequest, context
    ) -> str:
        f = furl.furl(request.url)

        # Add matches for reserved paths

        # PUT /<target>
        if request.method == "PUT" and len(f.path.segments) == 1:
            target = f.path.segments[0]
            body = json.loads(request.body)

            result = self.engine.indice(target, create=True).config(body)
            return self.serialize_for_response(result)

        # PUT /<target>/_mapping
        if (
            request.method == "PUT"
            and len(f.path.segments) == 2
            and f.path.segments[1] == "_mapping"
        ):
            target = f.path.segments[0]
            body = json.loads(request.body)

            result = self.engine.indice(target).config_mappings(body)
            return self.serialize_for_response(result)

        # Index API
        # PUT /<target>/_doc/<_id>
        if (
            request.method == "PUT"
            and len(f.path.segments) == 3
            and f.path.segments[1] == "_doc"
        ):
            target = f.path.segments[0]
            body = json.loads(request.body)
            id = f.path.segments[2]
            op_type = core.OperationType.INDEX

            result = self.engine.indice(target, create=True).index(
                body=body, id=id, op_type=op_type
            )
            return self.serialize_for_response(result)

        # POST /<target>/_doc/
        if (
            request.method == "POST"
            and len(f.path.segments) == 2
            and f.path.segments[1] == "_doc"
        ):
            target = f.path.segments[0]
            body = json.loads(request.body)

            result = self.engine.indice(target, create=True).index(body=body)
            return self.serialize_for_response(result)

        # PUT /<target>/_create/<_id>
        # if request.method == "PUT" and f.path.segments[1] == "_create":
        #     if len(f.path.segments) < 3:
        #         raise ElasticError("_id is required")
        #     return

        # POST /<target>/_create/<_id>
        # if request.method == "POST" and f.path.segments[1] == "_create":
        #     if len(f.path.segments) < 3:
        #         raise ElasticError("_id is required")
        #     return

        # POST /<index>/_update/<_id>
        if (
            request.method == "POST"
            and len(f.path.segments) == 3
            and f.path.segments[1] == "_update"
        ):
            target = f.path.segments[0]
            body = json.loads(request.body)
            id = f.path.segments[2]

            result = self.engine.indice(target, create=True).update(
                body=body, id=id
            )
            return self.serialize_for_response(result)

        # DELETE /<target>/_doc/<_id>
        if (
            request.method == "DELETE"
            and len(f.path.segments) == 3
            and f.path.segments[1] == "_doc"
        ):
            target = f.path.segments[0]
            id = f.path.segments[2]

            result = self.engine.indice(target, create=False).delete(id)
            return self.serialize_for_response(result)

        # GET /<target>/_search
        if (
            request.method == "GET"
            and len(f.path.segments) == 2
            and f.path.segments[1] == "_search"
        ):
            target = f.path.segments[0]
            body = json.loads(request.body)

            result = query_dsl.search(body, self.engine.indices(target))
            return self.serialize_for_response(result)

        # GET /<target>/_count
        if (
            request.method == "GET"
            and len(f.path.segments) == 2
            and f.path.segments[1] == "_count"
        ):
            target = f.path.segments[0]
            body = json.loads(request.body)

            result = query_dsl.count(body, self.engine.indices(target))
            return self.serialize_for_response(result)

        context.status_code = 405
        return json.dumps(
            {
                "error": f"Incorrect HTTP method for uri [{request.url}] and method [{request.method}]"
            }
        )

    def serialize_for_response(self, result) -> str:
        return json.dumps(dataclasses.asdict(result), cls=JSONEncoder)
