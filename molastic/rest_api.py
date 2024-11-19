from __future__ import annotations

import re
import json
import typing
import requests
import requests_mock

from . import core
from . import utils

from .api.api_interface import RequestHandler

from .api import document
from .api import index
from .api import search


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
            text=self.handle_request,  # type: ignore
        )

        self.handlers: typing.Sequence[RequestHandler] = [
            document.AddOrOverwriteDocumentHandler(self.engine),
            document.AddDocumentHandler(self.engine),
            document.UpdateDocumentHandler(self.engine),
            document.DeleteDocumentHandler(self.engine),
            document.GetDocumentHandler(self.engine),
            document.ExistsDocumentHandler(self.engine),
            index.CreateIndexHandler(self.engine),
            index.DeleteIndexHandler(self.engine),
            index.ExistsHandler(self.engine),
            index.UpdateMappingHandler(self.engine),
            index.GetMappingHandler(self.engine),
            search.SearchHandler(self.engine),
            search.CountHandler(self.engine),
        ]

    def start(self) -> None:
        self.mocker.start()

    def stop(self) -> None:
        self.mocker.stop()

    def handle_request(
        self, request: requests.PreparedRequest, context
    ) -> typing.Optional[str]:
        handlers = [h for h in self.handlers if h.can_handle(request)]

        if len(handlers) == 0:
            context.status_code = 405
            return json.dumps(
                {
                    "error": (
                        f"Incorrect HTTP method for uri [{request.url}] "
                        f"and method [{request.method}]"
                    )
                }
            )

        if len(handlers) > 1:
            raise Exception("single handler should match")

        return handlers[0].handle(request, context)
