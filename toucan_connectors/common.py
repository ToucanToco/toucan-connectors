import ast
import json
import re
from abc import ABCMeta, ABC, abstractmethod


def nosql_apply_parameters_to_query(query, parameters):
    """
    WARNING : DO NOT USE THIS WITH VARIANTS OF SQL
    Instead use your client library parameter substitution method.
    https://www.owasp.org/index.php/Query_Parameterization_Cheat_Sheet
    """
    if parameters is None:
        return query

    json_query = json.dumps(query)

    # find which parameters are directly used as value of a key (no interpolation)
    values_parameters = re.findall(r'"%\((\w*)\)s"', json_query)

    # get the relevant str repr of the parameters according to how they are going to be used
    json_parameters = {
        key: json.dumps(val) if key in values_parameters else val
        for key, val in parameters.items()
    }

    # change the JSON repr of the query so that parameters used directly are not quoted
    re_query = re.sub(r'"(%\(\w*\)s)"', r'\g<1>', json_query)

    # now we can safely interpolate the str repr of the query and the parameters
    return json.loads(re_query % json_parameters)


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
