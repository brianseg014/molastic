import abc
import typing
import ply.lex


class Analyzer(abc.ABC):
    @abc.abstractmethod
    def create_stream(self, source: str) -> typing.Iterable[str]:
        pass


class Tokenizer(abc.ABC):
    @abc.abstractmethod
    def create_stream(self, source: str) -> typing.Iterable[str]:
        pass


class StandardTokenizer(Tokenizer):
    tokens = ("ALPHANUM", "DECIMAL")

    t_ignore_WHITESPACE = r"\s+"

    t_ALPHANUM = "\\w+"
    t_DECIMAL = (
        r"( 0 | [1-9] [0-9]* ) (DOT [0-9]+)? ( [eE] [+\-]? [0-9]+ )? [fFdD]?"
    )

    def t_error(self, t):
        pass

    def __init__(self, max_token_length: int) -> None:
        self.max_token_length = max_token_length

    def create_stream(self, source: str) -> typing.Iterable[str]:
        lexer = ply.lex.lex(module=self)

        lexer.input(source)
        while True:
            token = lexer.token()
            if token is None:
                break

            if len(token.value) > self.max_token_length:
                continue

            yield token.value


class StandardAnalyzer(Analyzer):
    def __init__(self, max_token_length: int = 255) -> None:
        self.tokenizer = StandardTokenizer(max_token_length)

    def create_stream(self, source: str) -> typing.Iterable[str]:
        yield from self.tokenizer.create_stream(source)
