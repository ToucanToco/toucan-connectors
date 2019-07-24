import ast
import re
from abc import ABCMeta, ABC, abstractmethod
from copy import deepcopy
from enum import Enum
from typing import Any, Dict, List, Tuple, Type, Union

from jinja2 import Template
from pydantic import BaseModel, create_model
from pydantic.fields import Field
from toucan_data_sdk.utils.helpers import slugify

RE_PARAM = r'%\(([^(%\()]*)\)s'
RE_JINJA = r'{{([^({{)}]*)}}'

RE_PARAM_ALONE = r"^" + RE_PARAM + "$"
RE_JINJA_ALONE = r"^" + RE_JINJA + "$"

# Identify jinja params with no quotes around or complex condition
RE_JINJA_ALONE_IN_STRING = [RE_JINJA + r"([ )])", RE_JINJA + r"()$"]

RE_SET_KEEP_TYPE = r'{{__keep_type__\1}}\2'
RE_GET_KEEP_TYPE = r'{{(__keep_type__[^({{)}]*)}}'


class StrEnum(str, Enum):
    """Class to easily make schemas with enum values and type string"""


def strlist_to_enum(field: str, strlist: List[str], default_value=...) -> tuple:
    """
    Convert a list of strings to a pydantic schema enum
    the value is either <default value> or a tuple ( <type>, <default value> )
    If the field is required, the <default value> has to be '...' (cf pydantic doc)
    By default, the field is considered required.
    """
    return StrEnum(field, {v: v for v in strlist}), default_value


class classproperty:
    """Author: jchl https://stackoverflow.com/questions/5189699/how-to-make-a-class-property"""

    def __init__(self, f):
        self.f = f

    def __get__(self, obj, owner):
        return self.f(owner)


class TemplatedMixin:
    """
    Mixin to allow templated fields to be set.
    It will add a new `templated_model` property, which creates a new model
    allowing string values for templated fields.
    These fields are set in the `__templated__` class attribute.
    """
    __templated__ = []

    @classproperty
    def templated_model(cls) -> Type[BaseModel]:
        return create_templated_model(cls, cls.__templated__)


def create_templated_model(
    model_cls: Type[BaseModel],
    templated_fields: List[str]
) -> Type[BaseModel]:
    """
    Create a new BaseModel base on `model_cls` but by allowing `str` type
    for all fields of `templated_fields`
    """
    templated_field_definitions: Dict[str, Tuple[type, Any]] = {}
    for field_name in templated_fields:
        try:
            field: Field = model_cls.__fields__[field_name]
        except KeyError:
            continue
        type_or_str = Union[str, field.type_]
        default_value = ... if field.required else field.default
        templated_field_definitions[field_name] = (type_or_str, default_value)

    return create_model(
        f'Templated{model_cls.__name__}', **templated_field_definitions, __base__=model_cls
    )


def nosql_apply_parameters_to_query(query, parameters):
    """
    WARNING : DO NOT USE THIS WITH VARIANTS OF SQL
    Instead use your client library parameter substitution method.
    https://www.owasp.org/index.php/Query_Parameterization_Cheat_Sheet
    """
    def _prepare_parameters(p):
        if isinstance(p, str):
            return f'"{p}"'
        elif isinstance(p, list):
            return [_prepare_parameters(e) for e in p]
        elif isinstance(p, dict):
            return {k: _prepare_parameters(v) for k, v in p.items()}
        else:
            return p

    def _prepare_result(res):
        if isinstance(res, str):
            return ast.literal_eval(res)
        elif isinstance(res, list):
            return [_prepare_result(e) for e in res]
        elif isinstance(res, dict):
            return {k: _prepare_result(v) for k, v in res.items()}
        else:
            return res

    def _render_query(query, parameters):
        if isinstance(query, dict):
            return {key: _render_query(value, parameters)
                    for key, value in deepcopy(query).items()}
        elif isinstance(query, list):
            return [_render_query(elt, parameters) for elt in deepcopy(query)]
        elif type(query) is str:
            clean_p = deepcopy(parameters)
            # Add quotes to string parameters to keep type if not complex
            if re.match(RE_PARAM_ALONE, query) or re.match(RE_JINJA_ALONE, query):
                clean_p = _prepare_parameters(clean_p)

            # Render jinja then render parameters `%()s`
            res = Template(query).render(clean_p) % clean_p

            # Remove extra quotes with literal_eval
            try:
                res = ast.literal_eval(res)
                if isinstance(res, str):
                    return res
                else:
                    return _prepare_result(res)
            except (SyntaxError, ValueError):
                return res
        else:
            return query

    def _handle_missing_params(elt, params):
        """
        Remove a dictionary key if its value has a missing parameter.
        This is used to support the __VOID__ syntax, specific at Toucan Toco :
            cf. https://bit.ly/2Ln6rcf
        """
        if isinstance(elt, dict):
            e = {}
            for k, v in elt.items():
                if isinstance(v, str):
                    matches = re.findall(RE_PARAM, v) + re.findall(RE_JINJA, v)
                    missing_params = []
                    for m in matches:
                        try:
                            eval(m, deepcopy(params))
                        except Exception:
                            missing_params.append(m)
                    if any(missing_params):
                        continue
                    else:
                        e[k] = v
                else:
                    e[k] = _handle_missing_params(v, params)
            return e
        elif isinstance(elt, list):
            return [_handle_missing_params(e, params) for e in elt]
        else:
            return elt

    query = _handle_missing_params(query, parameters)

    if parameters is None:
        return query

    query = _render_query(query, parameters)
    return query


def render_raw_permissions(query, parameters):
    def _flatten_dict(p, parent_key=''):
        new_p = {}
        for k, v in deepcopy(p).items():
            new_key = f'{parent_key}_{k}' if parent_key else k
            new_p[new_key] = v
            if isinstance(v, list):
                v = {idx: elt for idx, elt in enumerate(v)}
            if isinstance(v, dict):
                new_p.update(_flatten_dict(v, new_key))
            elif isinstance(v, str):
                new_p.update({new_key: f'"{v}"'})
            else:
                new_p.update({new_key: v})
        return new_p

    if parameters is None:
        return query

    # Flag params to keep type if not complex (no quotes or condition)
    for pattern in RE_JINJA_ALONE_IN_STRING:
        query = re.sub(pattern, RE_SET_KEEP_TYPE, query)
    p_keep_type = re.findall(RE_GET_KEEP_TYPE, query)
    for key in p_keep_type:
        query = query.replace(key, slugify(key, separator='_'))
    if len(p_keep_type):
        # Add a version of parameters flatten + with quotes for string
        p_keep_type = _flatten_dict(parameters, parent_key='__keep_type_')
        parameters.update(p_keep_type)

    return Template(query).render(parameters)


class AstTranslator(ABC):
    def resolve(self, elt):
        elt_name = elt.__class__.__name__
        try:
            method = getattr(self, elt_name)
        except AttributeError:
            raise Exception(f'Missing method for {elt_name}')
        return method

    def translate(self, elt):
        return self.resolve(elt)(elt)

    def parse(self, expr):
        ex = ast.parse(expr, mode='eval')
        return self.translate(ex.body)


class Expression(AstTranslator, metaclass=ABCMeta):
    @abstractmethod
    def BoolOp(self, op):
        """Boolean expressions with or/and """

    @abstractmethod
    def And(self, op):
        """Boolean operator and """

    @abstractmethod
    def Or(self, op):
        """Boolean operator and """

    @abstractmethod
    def Compare(self, compare):
        """Expression with left, operator and right elements"""


class Operator(AstTranslator, metaclass=ABCMeta):
    @abstractmethod
    def Eq(self, node):
        """Equal operator"""

    @abstractmethod
    def NotEq(self, node):
        """Not equal operator"""

    @abstractmethod
    def In(self, node):
        """In operator"""

    @abstractmethod
    def NotIn(self, node):
        """Not in operator"""

    @abstractmethod
    def Gt(self, node):
        """Greater than operator"""

    @abstractmethod
    def Lt(self, node):
        """Less than operator"""

    @abstractmethod
    def GtE(self, node):
        """Greater than or equal operator"""

    @abstractmethod
    def LtE(self, node):
        """Less than or equal operator"""


class Column(AstTranslator, metaclass=ABCMeta):
    def Name(self, node):
        """Column name"""

    def Str(self, node):
        """Column name as str"""


class Value(AstTranslator, metaclass=ABCMeta):
    @abstractmethod
    def Name(self, node):
        """Var field"""

    @abstractmethod
    def Str(self, node):
        """String field"""

    @abstractmethod
    def Num(self, node):
        """String field"""

    @abstractmethod
    def List(self, node):
        """List field"""

    @abstractmethod
    def UnaryOp(self, op):
        """Value with unary operator +/-"""

    @abstractmethod
    def Set(self, node):
        """Set (jinja parameters)"""

    @abstractmethod
    def Subscript(self, node):
        """List or Dict call (jinja parameters)"""

    @abstractmethod
    def Index(self, node):
        """Indice in list or dict (jinja parameters)"""
