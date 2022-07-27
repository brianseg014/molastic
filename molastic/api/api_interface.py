import abc
import json
import typing
import requests


class ParseException(Exception):
    pass


class JsonParseException(ParseException):
    pass


class RequestHandler(abc.ABC):
    @abc.abstractmethod
    def can_handle(self, request: requests.PreparedRequest) -> bool:
        pass

    @abc.abstractmethod
    def handle(
        self, request: requests.PreparedRequest, context
    ) -> typing.Optional[str]:
        pass

    def loads(self, request_body) -> dict:
        try:
            return json.loads(request_body)
        except json.decoder.JSONDecodeError as ex:
            raise JsonParseException() from ex
