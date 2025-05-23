import json
import logging
from enum import Enum

from pydantic import AnyHttpUrl, SecretStr


def custom_json_serializer(obj):
    if isinstance(obj, SecretStr):
        return obj.get_secret_value()
    if isinstance(obj, Enum):
        return obj.value
    if isinstance(obj, AnyHttpUrl):
        return str(obj)
    return obj


class JsonWrapper:
    @staticmethod
    def dump(
        obj,
        fp,
        *,
        skipkeys=False,
        ensure_ascii=True,
        check_circular=True,
        allow_nan=True,
        cls=None,
        indent=None,
        # separators=None,
        default=custom_json_serializer,
        sort_keys=False,
        **kwargs,
    ):
        logging.getLogger(__name__).debug(f"JSON object {obj}")
        json.dump(
            obj,
            fp,
            skipkeys=skipkeys,
            ensure_ascii=ensure_ascii,
            check_circular=check_circular,
            allow_nan=allow_nan,
            cls=cls,
            indent=indent,
            separators=(",", ":"),
            default=default,
            sort_keys=sort_keys,
            **kwargs,
        )
        logging.getLogger(__name__).debug(f"Stringify JSON in file {fp}")

    @staticmethod
    def dumps(
        obj,
        *,
        skipkeys=False,
        ensure_ascii=True,
        check_circular=True,
        allow_nan=True,
        cls=None,
        indent=None,
        # separators=None,
        default=custom_json_serializer,
        sort_keys=False,
        **kwargs,
    ):
        logging.getLogger(__name__).debug(f"JSON object {obj}")
        result = json.dumps(
            obj,
            skipkeys=skipkeys,
            ensure_ascii=ensure_ascii,
            check_circular=check_circular,
            allow_nan=allow_nan,
            cls=cls,
            indent=indent,
            separators=(",", ":"),
            default=default,
            sort_keys=sort_keys,
            **kwargs,
        )
        logging.getLogger(__name__).debug(f"Stringify JSON {result}")
        return result

    @staticmethod
    def load(
        fp,
        *,
        cls=None,
        object_hook=None,
        parse_float=None,
        parse_int=None,
        parse_constant=None,
        object_pairs_hook=None,
        **kwargs,
    ) -> dict:
        return JsonWrapper.loads(
            fp.read(),
            cls=cls,
            object_hook=object_hook,
            parse_float=parse_float,
            parse_int=parse_int,
            parse_constant=parse_constant,
            object_pairs_hook=object_pairs_hook,
            **kwargs,
        )

    @staticmethod
    def loads(
        s,
        *,
        cls=None,
        object_hook=None,
        parse_float=None,
        parse_int=None,
        parse_constant=None,
        object_pairs_hook=None,
        **kwargs,
    ) -> dict:
        logging.getLogger(__name__).debug(f"Stringify JSON {s}")
        result = json.loads(
            s,
            cls=cls,
            object_hook=object_hook,
            parse_float=parse_float,
            parse_int=parse_int,
            parse_constant=parse_constant,
            object_pairs_hook=object_pairs_hook,
            **kwargs,
        )
        logging.getLogger(__name__).debug(f"Parsed JSON {result}")
        return result
