import json
from ast import USub

from toucan_connectors.common import Expression, Operator, Column, Value


class MongoExpression(Expression):
    def BoolOp(self, op):
        return {self.translate(op.op): list(map(self.translate, op.values))}

    def And(self, op):
        return '$and'

    def Or(self, op):
        return '$or'

    def Compare(self, compare):
        field = MongoColumn().translate(compare.left)
        operator = compare.ops[0]
        right = compare.comparators[0]
        return {field: MongoOperator().resolve(operator)(right)}


class MongoOperator(Operator):
    def Eq(self, node):
        '''=='''
        return MongoValue().translate(node)

    def NotEq(self, node):
        '''!='''
        return {'$ne': MongoValue().translate(node)}

    def In(self, node):
        '''in'''
        return {'$in': MongoValue().translate(node)}

    def NotIn(self, node):
        '''not in'''
        return {'$nin': MongoValue().translate(node)}

    def Gt(self, node):
        '''>'''
        return {'$gt': MongoValue().translate(node)}

    def Lt(self, node):
        '''<'''
        return {'$lt': MongoValue().translate(node)}

    def GtE(self, node):
        '''>='''
        return {'$gte': MongoValue().translate(node)}

    def LtE(self, node):
        '''<='''
        return {'$lte': MongoValue().translate(node)}


class MongoColumn(Column):
    def Name(self, node):
        return node.id

    def Str(self, node):
        return node.s


class MongoValue(Value):
    SPECIAL_VALUES = {
        'null': None,
        'false': False,
        'true': True
    }

    def Name(self, node):
        if node.id in self.SPECIAL_VALUES:
            return self.SPECIAL_VALUES[node.id]
        else:
            return node.id

    def Str(self, node):
        return node.s

    def Num(self, node):
        return node.n

    def List(self, node):
        return list(map(self.translate, node.elts))

    def UnaryOp(self, op):
        value = self.translate(op.operand)
        if isinstance(op.op, USub):
            value = -value
        return value

    def Set(self, node):
        elts = [self.translate(elt) for elt in node.elts]
        return '{' + ', '.join(elts) + '}'

    def Subscript(self, node):
        indice = json.dumps(self.translate(node.slice))
        return self.translate(node.value) + '[' + indice + ']'

    def Index(self, node):
        return self.translate(node.value)
