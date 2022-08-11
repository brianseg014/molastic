# pylint: disable=invalid-name

from __future__ import annotations

import re
import ast
import enum
import typing
import functools
import ply.lex
import ply.yacc
import inspect
import logging


logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def execute(code: str, variables: dict):
    from . import java_api

    python = compile(transpile(parse(code)), filename="<ast>", mode="exec")

    shared_api = {m[0]: m[1] for m in inspect.getmembers(java_api)}
    _globals: dict = {**variables, **shared_api}
    _locals: dict = {}
    exec(python, _globals, _locals)
    _locals["execute"]()


def transpile(node: ANode, **kwargs):
    "Translate from painless ast node into python ast node"
    if isinstance(node, SClass):
        return ast.Module(
            body=[
                transpile(s, **kwargs, ctx=ast.Load())
                for s in node.function_nodes
            ],
            type_ignores=[],
            _fields=("body", "type_ignores"),
            lineno=0,
            col_offset=0,
        )
    if isinstance(node, SFunction):
        return ast.FunctionDef(
            name=node.name,
            args=ast.arguments(
                posonlyargs=[],
                args=[ast.arg(a) for a in node.paramter_names],
                kwonlyargs=[],
                kw_defaults=[],
                defaults=[],
            ),
            body=transpile(node.block_node, **kwargs),
            decorator_list=[],
            _fields=("name", "args", "body", "decorator_list"),
            lineno=0,
            col_offset=0,
        )
    if isinstance(node, SBlock):
        transpiled_nodes = [
            transpile(s, **kwargs) for s in node.statement_nodes
        ]
        for i, transpiled_node in enumerate(transpiled_nodes):
            if isinstance(transpiled_node, ast.expr):
                transpiled_nodes[i] = ast.Expr(
                    value=transpiled_node,
                    _fields=("value",),
                    lineno=0,
                    col_offset=0,
                )
            elif isinstance(transpiled_node, ast.stmt):
                pass
            else:
                raise NotImplementedError(
                    f"should implement handler for {type(transpiled_node)}"
                )
        return transpiled_nodes
    if isinstance(node, EAssignment):
        if node.operation is None:
            return ast.Assign(
                targets=[transpile(node.left_node, ctx=ast.Store())],
                value=transpile(node.right_node, **kwargs),
                lineno=0,
                col_offset=0,
            )
        else:
            return ast.AugAssign(
                target=transpile(node.left_node, ctx=ast.Store()),
                value=transpile(node.right_node, **kwargs),
                op=transpile_operation(node.operation),
                lineno=0,
                col_offset=0,
            )
    if isinstance(node, ECall):
        return ast.Call(
            func=ast.Attribute(
                value=transpile(node.prefix_node, **kwargs),
                attr=node.method_name,
                ctx=ast.Load(),
                _fields=("value", "attr", "ctx"),
                lineno=0,
                col_offset=0,
            ),
            args=[transpile(a, **kwargs) for a in node.argument_nodes],
            keywords=[],
            starargs=None,
            kwargs=None,
            _fields=("func", "args", "starargs", "keywords"),
            lineno=0,
            col_offset=0,
        )
    if isinstance(node, EBrace):
        return ast.Subscript(
            value=transpile(node.prefix_node, ctx=ast.Load()),
            slice=transpile(node.index_node, ctx=ast.Load()),
            ctx=kwargs["ctx"],
            _fields=("value", "slice", "ctx"),
            lineno=0,
            col_offset=0,
        )
    if isinstance(node, EDot):
        if node.is_null_safe:
            raise NotImplementedError("null-safe not yet supported")
        return ast.Attribute(
            value=transpile(node.prefix_node, ctx=ast.Load()),
            attr=node.index,
            ctx=kwargs["ctx"],
            _fields=("value", "attr", "ctx"),
            lineno=0,
            col_offset=0,
        )
    if isinstance(node, ESymbol):
        return ast.Name(
            id=node.symbol,
            ctx=kwargs["ctx"],
            _fields=("id", "ctx"),
            lineno=0,
            col_offset=0,
        )
    if isinstance(node, EString):
        return ast.Constant(
            value=node.string, _fields=("value",), lineno=0, col_offset=0
        )
    if isinstance(node, EDecimal):
        return ast.Constant(
            value=node.decimal, _fields=("value",), lineno=0, col_offset=0
        )
    if isinstance(node, ENumeric):
        return ast.Constant(
            value=int(node.numeric, base=node.radix),
            _fields=("value",),
            lineno=0,
            col_offset=0,
        )
    if isinstance(node, EBooleanConstant):
        if node.constant is True:
            return ast.Name(
                id="true",
                ctx=ast.Load(),
                _fields=("id", "ctx"),
                lineno=0,
                col_offset=0,
            )
        else:
            return ast.Name(
                id="false",
                ctx=ast.Load(),
                _fields=("id", "ctx"),
                lineno=0,
                col_offset=0,
            )
    raise NotImplementedError(f"cannot transpile {node.__class__.__name__}")


def transpile_operation(operation: Operation):
    if operation == Operation.ADD:
        return ast.Add()
    elif operation == Operation.SUB:
        return ast.Sub()
    else:
        raise NotImplementedError(operation)


def parse(source: str):
    "Parse the source into an painless ast node equivalent."
    yacc = typing.cast(
        ply.yacc.LRParser,
        ply.yacc.yacc(
            module=PainlessParserModule(),
            tabmodule="painless_parsetab",
            debuglog=logger,
        ),
    )
    return yacc.parse(source, lexer=ply.lex.lex(module=PainlessLexerModule()))


class Operation(enum.Enum):
    MUL = ("*", "multiplication")
    DIV = ("/", "division")
    REM = ("%", "remainder")
    ADD = ("+", "addition")
    SUB = ("-", "subsctraction")
    FIND = ("=~", "find")
    MATCH = ("==~", "match")
    LSH = ("<<", "left shift")
    RSH = (">>", "right shift")
    USH = (">>>", "unsigned right shift")
    BWNOT = ("~", "bitwise not")
    BWAND = ("&", "bitwise and")
    XOR = ("|", "bitwise xor")
    BWOR = ("^", "bitwise or")
    NOT = ("!", "boolean not")
    AND = ("&&", "boolean and")
    OR = ("||", "boolean or")
    LT = ("<", "less than")
    LTE = ("<=", "less than or equals")
    GT = (">", "greater than")
    GTE = (">=", "greater than or equals")
    EQ = ("==", "equals")
    EQR = ("===", "reference equals")
    NE = ("!=", "not equals")
    NER = ("!==", "reference not equals")

    @classmethod
    def from_symbol(cls, symbol: str) -> Operation:
        for m in Operation:
            if m.value[0] == symbol:
                return m
        raise KeyError(symbol)


class ANode:
    pass


class AExpression(ANode):
    pass


class AStatement(ANode):
    pass


class EAssignment(AExpression):
    def __init__(
        self,
        left_node: AExpression,
        right_node: AExpression,
        post_if_read: bool,
        operation: typing.Optional[Operation],
    ) -> None:
        self.left_node = left_node
        self.right_node = right_node
        self.post_if_read = post_if_read
        self.operation = operation

    def __repr__(self) -> str:
        return (
            f"EAssignment("
            f"left_node={self.left_node}, "
            f"right_node={self.right_node}, "
            f"post_if_read={self.post_if_read}, "
            f"operation={self.operation})"
        )


class EBinary(AExpression):
    def __init__(
        self,
        left_node: AExpression,
        right_node: AExpression,
        operation: Operation,
    ) -> None:
        self.left_node = left_node
        self.right_node = right_node
        self.operation = operation

    def __repr__(self) -> str:
        return (
            f"EBinary("
            f"left_node={self.left_node}, "
            f"right_node={self.right_node}, "
            f"operation={self.operation})"
        )


class EBooleanComp(AExpression):
    def __init__(
        self,
        left_node: AExpression,
        right_node: AExpression,
        operation: Operation,
    ) -> None:
        self.left_node = left_node
        self.right_node = right_node
        self.operation = operation

    def __repr__(self) -> str:
        return (
            f"EBooleanComp("
            f"left_node={self.left_node}, "
            f"right_node={self.right_node}, "
            f"operation={self.operation})"
        )


class EBooleanConstant(AExpression):
    def __init__(self, constant: bool) -> None:
        self.constant = constant

    def __repr__(self) -> str:
        return f"EBooleanConstant(constant={self.constant})"


class EBrace(AExpression):
    def __init__(
        self,
        prefix_node: AExpression,
        index_node: AExpression,
    ) -> None:
        self.prefix_node = prefix_node
        self.index_node = index_node

    def __repr__(self) -> str:
        return (
            f"EBrace("
            f"prefix_node={self.prefix_node}, "
            f"index_node={self.index_node})"
        )


class ECall(AExpression):
    def __init__(
        self,
        prefix_node: AExpression,
        method_name: str,
        argument_nodes: typing.List[AExpression],
        is_null_safe: bool,
    ) -> None:
        self.prefix_node = prefix_node
        self.method_name = method_name
        self.argument_nodes = argument_nodes
        self.is_null_safe = is_null_safe

    def __repr__(self) -> str:
        return (
            f"ECall("
            f"prefix_node={self.prefix_node}, "
            f"method_name={self.method_name}, "
            f"argument_nodes={self.argument_nodes}, "
            f"is_null_safe={self.is_null_safe})"
        )


class ECallLocal(AExpression):
    def __init__(
        self,
        method_name: str,
        argument_nodes: typing.List[AExpression],
    ) -> None:
        self.method_name = method_name
        self.argument_nodes = argument_nodes

    def __repr__(self) -> str:
        return (
            f"ECallLocal("
            f"method_name={self.method_name}, "
            f"argument_nodes={self.argument_nodes})"
        )


class EComp(AExpression):
    def __init__(
        self,
        left_node: AExpression,
        right_node: AExpression,
        operation: Operation,
    ) -> None:
        self.left_node = left_node
        self.right_node = right_node
        self.operation = operation

    def __repr__(self) -> str:
        return (
            f"EComp("
            f"left_node={self.left_node}, "
            f"right_node={self.right_node}, "
            f"operation={self.operation})"
        )


class EConditional(AExpression):
    def __init__(
        self,
        condition_node: AExpression,
        true_node: AExpression,
        false_node: Operation,
    ) -> None:
        self.condition_node = condition_node
        self.true_node = true_node
        self.false_node = false_node

    def __repr__(self) -> str:
        return (
            f"EConditional("
            f"condition_node={self.condition_node}, "
            f"true_node={self.true_node}, "
            f"false_node={self.false_node})"
        )


class EDecimal(AExpression):
    def __init__(self, decimal: str) -> None:
        self.decimal = decimal

    def __repr__(self) -> str:
        return f"EDecimal(decimal='{self.decimal}')"


class EDot(AExpression):
    def __init__(
        self,
        prefix_node: AExpression,
        index: str,
        is_null_safe: bool,
    ) -> None:
        self.prefix_node = prefix_node
        self.index = index
        self.is_null_safe = is_null_safe

    def __repr__(self) -> str:
        return (
            f"EDot("
            f"prefix_node={self.prefix_node}, "
            f"index={self.index}, "
            f"is_null_safe={self.is_null_safe})"
        )


class EElvis(AExpression):
    def __init__(
        self,
        left_node: AExpression,
        right_node: AExpression,
    ) -> None:
        self.left_node = left_node
        self.right_node = right_node

    def __repr__(self) -> str:
        return (
            f"EElvis("
            f"left_node={self.left_node}, "
            f"right_node={self.right_node})"
        )


class EExplicit(AExpression):
    def __init__(
        self,
        canonical_type_name: str,
        child_node: AExpression,
    ) -> None:
        self.canonical_type_name = canonical_type_name
        self.child_node = child_node

    def __repr__(self) -> str:
        return (
            f"EExplicit("
            f"canonical_type_name='{self.canonical_type_name}', "
            f"child_node={self.child_node})"
        )


class EFunctionRef(AExpression):
    def __init__(self, symbol: str, method_name: str) -> None:
        self.symbol = symbol
        self.method_name = method_name

    def __repr__(self) -> str:
        return (
            f"EFunctionRef("
            f"symbol='{self.symbol}', "
            f"method_name='{self.method_name}')"
        )


class EInstanceOf(AExpression):
    def __init__(
        self,
        expression: AExpression,
        canonical_type_name: str,
    ) -> None:
        self.expression = expression
        self.canonical_type_name = canonical_type_name

    def __repr__(self) -> str:
        return (
            f"EInstanceOf("
            f"expression={self.expression}, "
            f"canonical_type_name='{self.canonical_type_name}')"
        )


class ELambda(AExpression):
    def __init__(
        self,
        canonical_type_name_parameters: typing.List[typing.Optional[str]],
        parameter_names: typing.List[str],
        block_node: SBlock,
    ) -> None:
        self.canonical_type_name_parameters = canonical_type_name_parameters
        self.parameter_names = parameter_names
        self.block_node = block_node

    def __repr__(self) -> str:
        return (
            f"ELambda("
            f"canonical_type_name_parameters={self.canonical_type_name_parameters},"
            f"parameter_names={self.parameter_names}, "
            f"block_node={self.block_node})"
        )


class EListInit(AExpression):
    def __init__(self, value_nodes: typing.List[AExpression]) -> None:
        self.value_nodes = value_nodes

    def __repr__(self) -> str:
        return f"EListInit(value_nodes={self.value_nodes})"


class EMapInit(AExpression):
    def __init__(
        self,
        key_nodes: typing.List[AExpression],
        value_nodes: typing.List[AExpression],
    ) -> None:
        self.key_nodes = key_nodes
        self.value_nodes = value_nodes

    def __repr__(self) -> str:
        return (
            f"EMapInit("
            f"key_nodes={self.key_nodes}, "
            f"value_nodes={self.value_nodes})"
        )


class ENewArray(AExpression):
    def __init__(
        self,
        canonical_type_name: str,
        value_nodes: typing.List[AExpression],
        is_initializer: bool,
    ) -> None:
        self.canonical_type_name = canonical_type_name
        self.value_nodes = value_nodes
        self.is_initializer = is_initializer

    def __repr__(self) -> str:
        return (
            f"ENewArray("
            f"canonical_type_name='{self.canonical_type_name}', "
            f"value_nodes={self.value_nodes}, "
            f"is_initializer={self.is_initializer})"
        )


class ENewArrayFunctionRef(AExpression):
    def __init__(self, canonical_type_name: str) -> None:
        self.canonical_type_name = canonical_type_name

    def __repr__(self) -> str:
        return f"ENewArrayFuncRef(canonical_type_name='{self.canonical_type_name}')"


class ENewObj(AExpression):
    def __init__(
        self,
        canonical_type_name: str,
        argument_nodes: typing.List[AExpression],
    ) -> None:
        self.canonical_type_name = canonical_type_name
        self.argument_nodes = argument_nodes

    def __repr__(self) -> str:
        return (
            f"ENewObj("
            f"canonical_type_name='{self.canonical_type_name}', "
            f"argument_nodes={self.argument_nodes})"
        )


class ENull(AExpression):
    def __repr__(self) -> str:
        return "ENull()"


class ENumeric(AExpression):
    def __init__(self, numeric: str, radix: int) -> None:
        self.numeric = numeric
        self.radix = radix

    def __repr__(self) -> str:
        return f"ENumeric(numeric={self.numeric}, radix={self.radix})"


class ERegex(AExpression):
    def __init__(self, pattern: str, flags: str) -> None:
        self.pattern = pattern
        self.flags = flags

    def __repr__(self) -> str:
        return f"ERegex(pattern='{self.pattern}', flags='{self.flags}')"


class EString(AExpression):
    def __init__(self, string: str) -> None:
        self.string = string

    def __repr__(self) -> str:
        return f"EString(string='{self.string}')"


class ESymbol(AExpression):
    def __init__(self, symbol: str) -> None:
        self.symbol = symbol

    def __repr__(self) -> str:
        return f"ESymbol(symbol='{self.symbol}')"


class EUnary(AExpression):
    def __init__(
        self,
        child_node: AExpression,
        operation: Operation,
    ) -> None:
        self.child_node = child_node
        self.operation = operation

    def __repr__(self) -> str:
        return (
            f"EUnary(child_node={self.child_node}, operation={self.operation})"
        )


class SBlock(AStatement):
    def __init__(self, statement_nodes: typing.List[AStatement]) -> None:
        self.statement_nodes = statement_nodes

    def __repr__(self) -> str:
        return f"SBlock(statement_nodes={self.statement_nodes})"


class SBreak(AStatement):
    def __repr__(self) -> str:
        return "SBreak()"


class SCatch(AStatement):
    def __init__(
        self,
        base_exception: type,
        canonical_type_name: str,
        symbol: str,
        block_node: SBlock,
    ) -> None:
        self.base_exception = base_exception
        self.canonical_type_name = canonical_type_name
        self.symbol = symbol
        self.block_node = block_node

    def __repr__(self) -> str:
        return (
            f"SCatch(base_exception={self.base_exception}, "
            f"canonical_type_name='{self.canonical_type_name}', "
            f"symbol='{self.symbol}', block_node={self.block_node})"
        )


class SClass(ANode):
    def __init__(self, function_nodes: typing.List[SFunction]) -> None:
        self.function_nodes = function_nodes

    def __repr__(self) -> str:
        return f"SClass(function_nodes={self.function_nodes})"


class SContinue(AStatement):
    def __repr__(self) -> str:
        return "SContinue()"


class SDeclBlock(AStatement):
    def __init__(self, declaration_nodes: typing.List[SDeclaration]) -> None:
        self.declaration_nodes = declaration_nodes

    def __repr__(self) -> str:
        return f"SDeclBlock(declation_nodes={self.declaration_nodes})"


class SDeclaration(AStatement):
    def __init__(
        self,
        canonical_type_name: str,
        symbol: str,
        value_node: typing.Optional[AExpression],
    ) -> None:
        self.canonical_type_name = canonical_type_name
        self.symbol = symbol
        self.value_node = value_node

    def __repr__(self) -> str:
        return (
            f"SDeclaration("
            f"canonical_type_name='{self.canonical_type_name}', "
            f"symbol='{self.symbol}', "
            f"value_node={self.value_node})"
        )


class SDo(AStatement):
    def __init__(
        self,
        condition_node: AExpression,
        block_node: SBlock,
    ) -> None:
        self.condition_node = condition_node
        self.block_node = block_node

    def __repr__(self) -> str:
        return (
            f"SDo(condition_node={self.condition_node}, "
            f"block_node={self.block_node})"
        )


class SEach(AStatement):
    def __init__(
        self,
        canonical_type_name: str,
        symbol: str,
        iterable_node: AExpression,
        block_node: SBlock,
    ) -> None:
        self.canonical_type_name = canonical_type_name
        self.symbol = symbol
        self.iterable_node = iterable_node
        self.block_node = block_node

    def __repr__(self) -> str:
        return (
            f"SEach("
            f"canonical_type_name='{self.canonical_type_name}', "
            f"symbol='{self.symbol}', "
            f"iterable_node={self.iterable_node}, "
            f"block_node={self.block_node})"
        )


class SExpression(AStatement):
    def __init__(self, statement_node: AExpression) -> None:
        self.statement_node = statement_node

    def __repr__(self) -> str:
        return f"SExpression(statement_node={self.statement_node})"


class SFor(AStatement):
    def __init__(
        self,
        initializer_node: ANode,
        continue_node: AExpression,
        afterthought_node: AExpression,
        block_node: SBlock,
    ) -> None:
        self.initializer_node = initializer_node
        self.conditnue_node = continue_node
        self.afterthought_node = afterthought_node
        self.block_node = block_node

    def __repr__(self) -> str:
        return (
            f"SFor("
            f"initializer_node={self.initializer_node}, "
            f"continue_node={self.conditnue_node}, "
            f"afterthought_node={self.afterthought_node}, "
            f"block_node={self.block_node})"
        )


class SFunction(ANode):
    def __init__(
        self,
        return_canonical_type_name: str,
        name: str,
        canonical_type_name_parameters: typing.List[str],
        parameter_names: typing.List[str],
        block_node: SBlock,
        is_internal: bool,
        is_static: bool,
        is_synthetic: bool,
        is_auto_return_enabled: bool,
    ) -> None:
        self.return_canonical_type_name = return_canonical_type_name
        self.name = name
        self.canonical_type_name_parameters = canonical_type_name_parameters
        self.paramter_names = parameter_names
        self.block_node = block_node
        self.is_internal = is_internal
        self.is_static = is_static
        self.is_synthetic = is_synthetic
        self.is_auto_return_enabled = is_auto_return_enabled

    def __repr__(self) -> str:
        return (
            f"SFunction("
            f"return_canonical_type_name='{self.return_canonical_type_name}', "
            f"name='{self.name}', "
            f"canonical_type_name_parameters={self.canonical_type_name_parameters}, "
            f"parameter_names={self.paramter_names}, "
            f"block_node={self.block_node}, "
            f"is_internal={self.is_internal}, "
            f"is_synthetic={self.is_synthetic}, "
            f"is_auto_return_enabled={self.is_auto_return_enabled})"
        )


class SIf(AStatement):
    def __init__(
        self,
        condition_node: AExpression,
        if_block_node: SBlock,
    ) -> None:
        self.condition_node = condition_node
        self.if_block_node = if_block_node

    def __repr__(self) -> str:
        return (
            f"SIf("
            f"condition_node={self.condition_node}, "
            f"if_block_node={self.if_block_node})"
        )


class SIfElse(AStatement):
    def __init__(
        self,
        condition_node: AExpression,
        if_block_node: SBlock,
        else_block_node: SBlock,
    ) -> None:
        self.condition_node = condition_node
        self.if_block_node = if_block_node
        self.else_block_node = else_block_node

    def __repr__(self) -> str:
        return (
            f"SIfElse("
            f"condition_node={self.condition_node}, "
            f"if_block_node={self.if_block_node}, "
            f"else_block_node={self.else_block_node})"
        )


class SReturn(AStatement):
    def __init__(self, value_node: typing.Optional[AExpression]) -> None:
        self.value_node = value_node

    def __repr__(self) -> str:
        return f"SReturn(value_node={self.value_node})"


class SThrow(AStatement):
    def __init__(self, expression_node: AExpression) -> None:
        self.expression_node = expression_node

    def __repr__(self) -> str:
        return f"SThrow(expression_node={self.expression_node})"


class STry(AStatement):
    def __init__(
        self,
        block_node: SBlock,
        catch_nodes: typing.List[SCatch],
    ) -> None:
        self.block_node = block_node
        self.catch_nodes = catch_nodes

    def __repr__(self) -> str:
        return (
            f"STry(block_node={self.block_node}, "
            f"catch_nodes={self.catch_nodes})"
        )


class SWhile(AStatement):
    def __init__(
        self,
        condition_node: AExpression,
        block_node: SBlock,
    ) -> None:
        self.condition_node = condition_node
        self.block_node = block_node

    def __repr__(self) -> str:
        return (
            f"SWhile(condition_node={self.condition_node}, "
            f"block_node={self.block_node})"
        )


class PainlessLexerModule:
    keywords = {
        # keywords
        "if": "IF",
        "else": "ELSE",
        "while": "WHILE",
        "do": "DO",
        "for": "FOR",
        "in": "IN",
        "continue": "CONTINUE",
        "break": "BREAK",
        "return": "RETURN",
        "new": "NEW",
        "try": "TRY",
        "catch": "CATCH",
        "throw": "THROW",
        "this": "THIS",
        "instanceof": "INSTANCEOF",
        "true": "TRUE",
        "false": "FALSE",
        "null": "NULL",
        # types
        "byte": "PRIMITIVE",
        "short": "PRIMITIVE",
        "char": "PRIMITIVE",
        "int": "PRIMITIVE",
        "long": "PRIMITIVE",
        "float": "PRIMITIVE",
        "double": "PRIMITIVE",
        "boolean": "PRIMITIVE",
        "def": "DEF",
    }

    tokens = (
        "OCTAL",
        "HEX",
        "INTEGER",
        "DECIMAL",
        "STRING",
        "REGEX",
        "ID",
        "LBRACK",
        "RBRACK",
        "LBRACE",
        "RBRACE",
        "LP",
        "RP",
        "DOLLAR",
        "DOT",
        "NSDOT",
        "COMMA",
        "SEMICOLON",
        "BOOLNOT",
        "BWNOT",
        "MUL",
        "DIV",
        "REM",
        "ADD",
        "SUB",
        "LSH",
        "RSH",
        "USH",
        "LT",
        "LTE",
        "GT",
        "GTE",
        "EQ",
        "EQR",
        "NE",
        "NER",
        "BWAND",
        "XOR",
        "BWOR",
        "BOOLAND",
        "BOOLOR",
        "COND",
        "COLON",
        "ELVIS",
        "REF",
        "ARROW",
        "FIND",
        "MATCH",
        "INCR",
        "DECR",
        "ASSIGN",
        "AADD",
        "ASUB",
        "AMUL",
        "ADIV",
        "AREM",
        "AAND",
        "AXOR",
        "AOR",
        "ALSH",
        "ARSH",
        "AUSH",
    ) + tuple(set(keywords.values()))

    t_ignore_COMMENT = r"// .*? [\n\r] | /\* .*? \*/"
    t_ignore_NEW_LINE = r"\n+"
    t_ignore_TAB = r"\t"
    t_ignore_WHITESPACE = r"\s+"

    t_INTEGER = r"( 0 | [1-9] [0-9]* ) [lLfFdD]?"
    t_OCTAL = r"0 [0-7]+ [lL]?"
    t_HEX = r"0 [xX] [0-9a-fA-F]+ [lL]?"
    t_DECIMAL = (
        r"( 0 | [1-9] [0-9]* ) (DOT [0-9]+)? ( [eE] [+\-]? [0-9]+ )? [fFdD]?"
    )

    t_STRING = r'"[^"]*"|\'[^\']*\''
    t_REGEX = r"/ ( \\ ~\n | ~(/ | \n) )+? / [cilmsUux]*"

    # Three char operator
    t_USH = r"\>\>\>"
    t_EQR = r"\=\=\="
    t_NER = r"\!\=\="
    t_MATCH = r"\=\=\~"

    # Two char operator
    t_LSH = r"\<\<"
    t_RSH = r"\>\>"
    t_LTE = r"\<\="
    t_GTE = r"\>\="
    t_EQ = r"\=\="
    t_NE = r"\!\="
    t_BOOLAND = r"\&\&"
    t_BOOLOR = r"\|\|"
    t_ELVIS = r"\?\:"
    t_REF = r"\:\:"
    t_ARROW = r"\-\>"
    t_FIND = r"\=\~"
    t_INCR = r"\+\+"
    t_DECR = r"\-\-"
    t_NSDOT = r"\?\."

    # Single char operator
    t_BOOLNOT = r"\!"
    t_BWNOT = r"\~"
    t_MUL = r"\*"
    t_DIV = r"\/"
    t_REM = r"\%"
    t_ADD = r"\+"
    t_SUB = r"\-"
    t_LT = r"\<"
    t_GT = r"\>"
    t_COND = r"\?"
    t_BWAND = r"\&"
    t_XOR = r"\^"
    t_BWOR = r"\|"
    t_COLON = r"\:"
    t_DOT = r"\."

    t_LBRACK = r"\{"
    t_RBRACK = r"\}"
    t_LBRACE = r"\["
    t_RBRACE = r"\]"
    t_LP = r"\("
    t_RP = r"\)"
    t_DOLLAR = r"\$"
    t_COMMA = r"\,"
    t_SEMICOLON = r"\;"

    # Four char assignment
    t_AUSH = r"\>\>\>\="

    # Threew char assignment
    t_ALSH = r"\>\>\="
    t_ARSH = r"\<\<\="

    # Two char assignment
    t_AADD = r"\+\="
    t_ASUB = r"\-\="
    t_AMUL = r"\*\="
    t_ADIV = r"\/\="
    t_AREM = r"\%\="
    t_AAND = r"\&\="
    t_AXOR = r"\^\="
    t_AOR = r"\|\="

    # Single char assignment
    t_ASSIGN = r"\="

    def t_ID(self, t):
        r"[_a-zA-Z] [_a-zA-Z0-9]*"
        t.type = PainlessLexerModule.keywords.get(t.value, "ID")
        return t

    def t_error(self, t):
        pass


class PainlessParserModule:
    tokens = PainlessLexerModule.tokens

    precedence = (
        ("left", "ADD", "SUB"),
        ("left", "MUL", "DIV", "REM"),
        ("left", "FIND", "MATCH"),
        ("left", "LSH", "RSH", "USH"),
        ("left", "GT", "GTE", "LT", "LTE"),
        ("left", "INSTANCEOF"),
        ("left", "EQ", "EQR", "NE", "NER"),
        ("left", "BWAND"),
        ("left", "XOR"),
        ("left", "BWOR"),
        ("left", "BOOLAND"),
        ("left", "BOOLOR"),
        ("right", "ELVIS"),
        ("right", "COND"),
        (
            "right",
            "ASSIGN",
            "AADD",
            "ASUB",
            "AMUL",
            "ADIV",
            "AREM",
            "AAND",
            "AXOR",
            "AOR",
            "ALSH",
            "AUSH",
        ),
    )

    def p_source_func_and_stmt(self, p):
        "source : functions statements"
        p[0] = SClass(
            p[1]
            + [
                SFunction(
                    "<internal>",
                    "execute",
                    [],
                    [],
                    SBlock(p[2]),
                    False,
                    False,
                    False,
                    False,
                )
            ]
        )

    def p_source_func(self, p):
        "source : functions"
        p[0] = SClass(p[1])

    def p_source_stmt(self, p):
        "source : statements"
        p[0] = SClass(
            [
                SFunction(
                    "<internal>",
                    "execute",
                    [],
                    [],
                    SBlock(p[1]),
                    False,
                    False,
                    False,
                    False,
                )
            ]
        )

    def p_source_empty(self, p):
        "source :"
        p[0] = SClass([])

    def p_functions(self, p):
        """functions : functions function
        | function"""
        if len(p) == 3:
            p[0] = p[1] + [p[3]]
        elif len(p) == 2:
            p[0] = [p[1]]

    def p_function(self, p):
        "function : decltype ID parameters block"
        p[0] = SFunction(
            p[1],
            p[2],
            tuple(ip[0] for ip in p[3]),
            tuple(ip[1] for ip in p[3]),
            p[4],
            False,
            False,
            False,
            False,
        )

    def p_parameters(self, p):
        """parameters : LP innerparameters RP
        | LP RP"""
        if len(p) == 4:
            p[0] = p[2]
        elif len(p) == 3:
            p[0] = []

    def p_innerparameters(self, p):
        """innerparameters : innerparameters COMMA declparam
        | declparam"""
        if len(p) == 4:
            p[0] = p[1] + [p[3]]
        else:
            p[0] = [p[1]]

    def p_declparam(self, p):
        "declparam : decltype ID"
        p[0] = (p[1], p[2])

    def p_statements(self, p):
        """statements : statements statement
        | statement"""
        if len(p) == 3:
            p[0] = p[1] + [p[2]]
        elif len(p) == 2:
            p[0] = [p[1]]

    def p_statement(self, p):
        """statement : rstatement
        | dstatement SEMICOLON"""
        p[0] = p[1]

    def p_rstatement(self, p):
        """rstatement : if_else
        | if
        | while
        | for
        | each
        | ineach
        | try"""
        p[0] = p[1]

    def p_if(self, p):
        "if : IF LP expression RP trailer"
        p[0] = SIf(p[3], p[5])

    def p_if_else(self, p):
        "if_else : IF LP expression RP trailer ELSE trailer"
        p[0] = SIfElse(p[3], p[5], p[7])

    def p_while(self, p):
        "while : WHILE LP expression RP trailer_or_empty"
        p[0] = SWhile(p[3], p[5])

    def p_for(self, p):
        "for : FOR LP initializer SEMICOLON optional_expression SEMICOLON afterthought RP trailer_or_empty"
        p[0] = SFor(p[3], p[5], p[7], p[9])

    def p_each(self, p):
        "each : FOR LP decltype ID COLON expression RP trailer"
        p[0] = SEach(p[3], p[4], p[6], p[8])

    def p_ineach(self, p):
        "ineach : FOR LP ID IN expression RP trailer"
        p[0] = SEach("def", p[3], p[5], p[7])

    def p_try(self, p):
        "try : TRY block traps"
        p[0] = STry(p[2], p[3])

    def p_dstatement(self, p):
        """dstatement : do
        | declaration
        | continue
        | break
        | return
        | throw
        | expression"""
        p[0] = p[1]

    def p_do_while(self, p):
        "do : DO block WHILE LP expression RP"
        p[0] = SDo(p[5], p[2])

    def p_continue(self, p):
        "continue : CONTINUE"
        p[0] = SContinue()

    def p_break(self, p):
        "break : BREAK"
        p[0] = SBreak()

    def p_return(self, p):
        "return : RETURN optional_expression"
        p[0] = SReturn(p[2])

    def p_throw(self, p):
        "throw : THROW expression"
        p[0] = SThrow(p[2])

    def p_trailer_or_empty(self, p):
        """trailer_or_empty : trailer
        | empty"""
        p[0] = p[1]

    def p_trailer(self, p):
        """trailer : block
        | statement"""
        p[0] = p[1]

    def p_block(self, p):
        "block : LBRACK statements RBRACK"
        p[0] = SBlock(p[2])

    def p_block_no_last_semicolon_1_stmt(self, p):
        "block : LBRACK dstatement RBRACK"
        p[0] = SBlock([p[2]])

    def p_block_no_last_semicolon_n_stmt(self, p):
        "block : LBRACK statements dstatement RBRACK"
        p[0] = SBlock(p[2] + [p[3]])

    def p_block_no_stmts(self, p):
        "block : LBRACK RBRACK"
        p[0] = SBlock([])

    def p_empty(self, p):
        "empty : SEMICOLON"

    def p_initializer(self, p):
        """initializer : declaration
        | expression
        |"""
        if len(p) == 2:
            p[0] = p[1]

    def p_afterthought(self, p):
        """afterthought : expression
        |"""
        if len(p) == 2:
            p[0] = p[1]

    def p_declaration(self, p):
        "declaration : decltype declvars"
        p[0] = SDeclBlock([SDeclaration(p[1], d[0], d[1]) for d in p[2]])

    def p_decltype(self, p):
        """decltype : type closedbraces
        | type"""
        p[0] = "".join(p[1:])

    def p_decltype_ambigous(self, p):
        """decltype : idaccess_lbrace RBRACE closedbraces
        | idaccess_lbrace RBRACE"""
        if len(p) == 4:
            p[0] = ".".join(p[1]) + "".join("[]")
        elif len(p) == 3:
            p[0] = ".".join(p[1]) + "[]"

    def p_closedbraces(self, p):
        """closedbraces : closedbraces LBRACE RBRACE
        | LBRACE RBRACE"""
        p[0] = "".join(p[1:])

    def p_type(self, p):
        """type : DEF
        | PRIMITIVE"""
        p[0] = p[1]

    def p_type_idaccess(self, p):
        "type : idaccess"
        p[0] = ".".join(p[1])

    def p_declvars(self, p):
        """declvars : declvars COMMA declvar
        | declvar"""
        if len(p) == 4:
            p[0] = p[1] + [p[3]]
        elif len(p) == 2:
            p[0] = [p[1]]

    def p_declvar(self, p):
        """declvar : ID ASSIGN expression
        | ID"""
        if len(p) == 4:
            p[0] = (p[1], p[3])
        elif len(p) == 2:
            p[0] = (p[1], None)

    def p_traps(self, p):
        """traps : traps trap
        | trap"""
        if len(p) == 3:
            p[0] = p[1] + [p[2]]
        elif len(p) == 2:
            p[0] = [p[1]]

    def p_trap(self, p):
        "trap : CATCH LP type ID RP block"
        p[0] = SCatch(Exception, p[3], p[4], p[6])

    def p_optional_expression(self, p):
        """optional_expression : expression
        |"""
        if len(p) == 2:
            p[0] = p[1]

    def p_noncondexpression(self, p):
        """noncondexpression : unary
        | binary
        | bool
        | comp
        | instanceof
        | elvis"""
        p[0] = p[1]

    def p_binary(self, p):
        """binary : noncondexpression MUL noncondexpression
        | noncondexpression DIV noncondexpression
        | noncondexpression REM noncondexpression
        | noncondexpression ADD noncondexpression
        | noncondexpression SUB noncondexpression
        | noncondexpression FIND noncondexpression
        | noncondexpression MATCH noncondexpression
        | noncondexpression LSH noncondexpression
        | noncondexpression RSH noncondexpression
        | noncondexpression USH noncondexpression
        | noncondexpression BWAND noncondexpression
        | noncondexpression XOR noncondexpression
        | noncondexpression BWOR noncondexpression"""
        p[0] = EBinary(p[1], p[3], Operation.from_symbol(p[2]))

    def p_bool(self, p):
        """bool : noncondexpression BOOLAND noncondexpression
        | noncondexpression BOOLOR noncondexpression"""
        p[0] = EBooleanComp(p[1], p[3], Operation.from_symbol(p[2]))

    def p_comp(self, p):
        """comp : noncondexpression GT noncondexpression
        | noncondexpression GTE noncondexpression
        | noncondexpression LT noncondexpression
        | noncondexpression LTE noncondexpression
        | noncondexpression EQ noncondexpression
        | noncondexpression EQR noncondexpression
        | noncondexpression NE noncondexpression
        | noncondexpression NER noncondexpression"""
        p[0] = EComp(p[1], p[3], Operation.from_symbol(p[2]))

    def p_instanceof(self, p):
        "instanceof : noncondexpression INSTANCEOF decltype"
        p[0] = EInstanceOf(p[1], p[3])

    def p_elvis(self, p):
        "elvis : noncondexpression ELVIS noncondexpression"
        p[0] = EElvis(p[1], p[3])

    def p_expression(self, p):
        """expression : noncondexpression
        | conditional
        | assignment"""
        p[0] = p[1]

    def p_conditional(self, p):
        "conditional : noncondexpression COND expression COLON expression"
        p[0] = EConditional(p[1], p[3], p[5])

    def p_assignment(self, p):
        """assignment : only_assignment
        | compound_assignment"""
        p[0] = p[1]

    def p_only_assignment(self, p):
        "only_assignment : noncondexpression ASSIGN expression"
        p[0] = EAssignment(p[1], p[3], False, None)

    def p_compound_assignment(self, p):
        """compound_assignment : noncondexpression AADD expression
        | noncondexpression ASUB expression
        | noncondexpression AMUL expression
        | noncondexpression ADIV expression
        | noncondexpression AREM expression
        | noncondexpression AAND expression
        | noncondexpression AXOR expression
        | noncondexpression AOR expression
        | noncondexpression ALSH expression
        | noncondexpression ARSH expression
        | noncondexpression AUSH expression"""
        p[0] = EAssignment(p[1], p[3], False, Operation.from_symbol(p[2][:-1]))

    def p_unary(self, p):
        """unary : pre
        | unarynotaddsub"""
        p[0] = p[1]

    def p_pre(self, p):
        """pre : INCR chain
        | DECR chain
        | ADD unary
        | SUB unary"""
        p[0] = EAssignment(
            p[2],
            ENumeric("1", 10),
            False,
            Operation.from_symbol(p[1]),
        )

    def p_unarynotaddsub(self, p):
        """unarynotaddsub : chain
        | post
        | not
        | castexpression"""
        p[0] = p[1]

    def p_post(self, p):
        """post : chain INCR
        | chain DECR"""
        p[0] = EAssignment(
            p[1],
            ENumeric("1", 10),
            True,
            Operation.from_symbol(p[2]),
        )

    def p_not(self, p):
        """not : BOOLNOT unary
        | BWNOT unary"""
        p[0] = EUnary(p[2], Operation.from_symbol(p[1]))

    def p_castexpression(self, p):
        """castexpression : primordefcast
        | refcast"""
        p[0] = p[1]

    def p_primordefcast(self, p):
        "primordefcast : LP primordefcasttype RP unary"
        p[0] = EExplicit(p[2], p[4])

    def p_refcast(self, p):
        "refcast : LP refcasttype RP unarynotaddsub"
        p[0] = EExplicit(p[2], p[4])

    def p_primordefcasttype(self, p):
        """primordefcasttype : DEF
        | PRIMITIVE"""
        p[0] = p[1]

    def p_refcasttype(self, p):
        """refcasttype : DEF closedbraces
        | PRIMITIVE closedbraces
        | idaccess closedbraces
        | idaccess"""
        p[0] = "".join(p[1:])

    def p_chain(self, p):
        """chain : arrayinitializer
        | dynamic"""
        p[0] = p[1]

    def p_dynamic(self, p):
        """dynamic : callinvoke
        | fieldaccess
        | braceaccess
        | primary"""
        p[0] = p[1]

    def p_callinvoke(self, p):
        """callinvoke : callinvoke_no_null_safe
        | callinvoke_null_safe"""
        p[0] = p[1]

    def p_callinvoke_no_null_safe(self, p):
        "callinvoke_no_null_safe : dynamic DOT ID arguments"
        p[0] = ECall(p[1], p[3], p[4], False)

    def p_callinvoke_no_null_safe_ambigous(self, p):
        "callinvoke_no_null_safe : idaccess arguments"
        p[0] = ECall(
            prefix_node=functools.reduce(
                lambda prev, idname: EDot(prev, idname, False),
                p[1][1:-1],
                ESymbol(p[1][0]),
            ),
            method_name=p[1][-1],
            argument_nodes=p[2],
            is_null_safe=False,
        )

    def p_callinvoke_null_safe(self, p):
        "callinvoke_null_safe : dynamic NSDOT ID arguments"
        p[0] = ECall(p[1], p[3], p[4], True)

    def p_fieldaccess(self, p):
        """fieldaccess : fieldaccess_no_null_safe
        | fieldaccess_null_safe"""
        p[0] = p[1]

    def p_fieldaccess_no_null_safe(self, p):
        "fieldaccess_no_null_safe : dynamic DOT ID"
        p[0] = EDot(p[1], p[3], False)

    def p_fieldaccess_no_null_safe_ambigous(self, p):
        "fieldaccess_no_null_safe : idaccess"
        p[0] = functools.reduce(
            lambda prev, idname: EDot(prev, idname, False),
            p[1][1:],
            ESymbol(p[1][0]),
        )

    def p_fieldaccess_null_safe(self, p):
        "fieldaccess_null_safe : dynamic NSDOT ID"
        p[0] = EDot(p[1], p[3], True)

    def p_braceaccess(self, p):
        "braceaccess : dynamic LBRACE expression RBRACE"
        p[0] = EBrace(p[1], p[3])

    def p_braceaccess_ambigous(self, p):
        "braceaccess : idaccess_lbrace expression RBRACE"
        p[0] = EBrace(
            prefix_node=functools.reduce(
                lambda prev, idname: EDot(prev, idname, False),
                p[1][1:],
                ESymbol(p[1][0]),
            ),
            index_node=p[2],
        )

    def p_primary(self, p):
        """primary : precedence
        | numeric
        | boolean
        | null
        | string
        | regex
        | listinitializer
        | mapinitializer
        | calllocal
        | newobject"""
        p[0] = p[1]

    def p_precedence(self, p):
        "precedence : LP expression RP"
        p[0] = p[2]

    def p_numeric(self, p):
        """numeric : octal
        | hex
        | integer
        | decimal"""
        p[0] = p[1]

    def p_octal(self, p):
        "octal : OCTAL"
        p[0] = ENumeric(p[1], 8)

    def p_hex(self, p):
        "hex : HEX"
        p[0] = ENumeric(p[1], 16)

    def p_integer(self, p):
        "integer : INTEGER"
        p[0] = ENumeric(p[1], 10)

    def p_decimal(self, p):
        "decimal : DECIMAL"
        p[0] = EDecimal(p[1])

    def p_boolean(self, p):
        """boolean : TRUE
        | FALSE"""
        p[0] = EBooleanConstant(bool(p[1]))

    def p_null(self, p):
        "null : NULL"
        p[0] = ENull()

    def p_string(self, p):
        "string : STRING"
        p[0] = EString(p[1][1:-1])

    def p_regex(self, p):
        "regex : REGEX"
        last_slash_index = p[1].rfind("/")

        pattern = p[1][1:last_slash_index]
        flags = p[1][last_slash_index + 1 :]

        p[0] = ERegex(pattern, flags)

    def p_idaccess(self, p):
        """idaccess : idaccess DOT ID
        | ID"""
        if len(p) == 4:
            p[0] = p[1] + [p[3]]
        elif len(p) == 2:
            p[0] = [p[1]]

    def p_idaccess_lbrace(self, p):
        "idaccess_lbrace : idaccess LBRACE"
        p[0] = p[1]

    def p_calllocal(self, p):
        """calllocal : ID arguments
        | DOLLAR arguments"""
        p[0] = ECallLocal(p[1], p[2])

    def p_newobject(self, p):
        """newobject : NEW type arguments"""
        p[0] = ENewObj(p[2], p[3])

    def p_arrayinitializer(self, p):
        """arrayinitializer : newstandardarray
        | newinitializedarray"""

    def p_newstandardarray(self, p):
        "newstandardarray : NEW type dims"
        p[0] = ENewArray(f'{p[2]}{"[]" * len(p[3])}', p[3], False)

    def p_newinitializedarray(self, p):
        "newinitializedarray : NEW type LBRACE RBRACE LBRACK arguments RBRACK"
        p[0] = ENewArray(f"{p[2]}[]", p[6], True)

    def p_dims(self, p):
        """dims : dims dim
        | dim"""
        if len(p) == 3:
            p[0] = p[1] + [p[2]]
        elif len(p) == 2:
            p[0] = [p[1]]

    def p_dim(self, p):
        "dim : LBRACE expression RBRACE"
        p[0] = p[2]

    def p_listinitializer(self, p):
        "listinitializer : LBRACE optional_expressions RBRACE"
        p[0] = EListInit(p[2])

    def p_optional_expressions(self, p):
        """optional_expressions : expressions
        |"""
        if len(p) == 2:
            p[0] = p[1]
        elif len(p) == 1:
            p[0] = []

    def p_expressions(self, p):
        """expressions : expression COMMA expression
        | expression"""
        if len(p) == 4:
            p[0] = p[1] + [p[3]]
        elif len(p) == 2:
            p[0] = [p[1]]

    def p_mapinitializer(self, p):
        "mapinitializer : LBRACE optional_maptokens RBRACE"
        p[0] = EMapInit([mt[0] for mt in p[2]], [mt[1] for mt in p[2]])

    def p_optional_maptokens(self, p):
        """optional_maptokens : maptokens
        | COLON"""
        if len(p) == 2:
            p[0] = p[1]
        elif len(p) == 1:
            p[0] = []

    def p_maptokens(self, p):
        """maptokens : maptokens COMMA maptoken
        | maptoken"""
        if len(p) == 4:
            p[0] = p[1] + [p[3]]
        elif len(p) == 2:
            p[0] = [p[1]]

    def p_maptoken(self, p):
        "maptoken : expression COLON expression"
        p[0] = (p[1], p[2])

    def p_arguments(self, p):
        """arguments : LP innerarguments RP
        | LP RP"""
        if len(p) == 4:
            p[0] = p[2]
        elif len(p) == 3:
            p[0] = []

    def p_innerarguments(self, p):
        """innerarguments : innerarguments COMMA argument
        | argument"""
        if len(p) == 4:
            p[0] = p[1] + [p[3]]
        elif len(p) == 2:
            p[0] = [p[1]]

    def p_argument(self, p):
        """argument : expression
        | lambda
        | funcref"""
        p[0] = p[1]

    def p_lambda_block(self, p):
        "lambda : lamtypes ARROW block"
        p[0] = ELambda(
            canonical_type_name_parameters=[lt[0] for lt in p[1]],
            parameter_names=[lt[1] for lt in p[1]],
            block=p[3],
        )

    def p_lambda_expression(self, p):
        "lambda : lamtypes ARROW expression"
        p[0] = ELambda(
            canonical_type_name_parameters=[lt[0] for lt in p[1]],
            parameter_names=[lt[1] for lt in p[1]],
            block=SBlock([SReturn(p[3])]),
        )

    def p_lamtypes(self, p):
        """lamtypes : LP innerlamtypes RP
        | lamtype"""
        if len(p) == 4:
            p[0] = p[2]
        elif len(p) == 2:
            p[0] = [p[1]]

    def p_innerlamtypes(self, p):
        """innerlamtypes : innerlamtypes COMMA lamtype
        | lamtype"""
        if len(p) == 4:
            p[0] = p[1] + [p[3]]
        elif len(p) == 2:
            p[0] = [p[1]]

    def p_lamtype(self, p):
        """lamtype : decltype ID
        | ID"""
        if len(p) == 3:
            p[0] = (p[1], p[2])
        elif len(p) == 2:
            p[0] = (None, p[1])

    def p_funcref(self, p):
        """funcref : classfuncref
        | constructorfuncref
        | localfuncref"""
        p[0] = p[1]

    def p_classfuncref(self, p):
        "classfuncref : decltype REF ID"
        p[0] = EFunctionRef(p[1], p[3])

    def p_constructorfuncref(self, p):
        "constructorfuncref : decltype REF NEW"
        if re.match(PainlessLexerModule.t_LBRACE, p[1]) is None:
            p[0] = EFunctionRef(p[1], p[3])
        else:
            p[0] = ENewArrayFunctionRef(p[1])

    def p_localfuncref(self, p):
        "localfuncref : THIS REF ID"
        p[0] = EFunctionRef(p[1], p[3])
