import logging
from typing import Optional, Any, Iterable, Dict

logger = logging.getLogger(__name__)


def from_optional(jobj) -> Optional[Any]:
    try:
        return None if jobj.isEmpty() else jobj.get()
    except:
        logger.exception('Error getting Optional value')
        return None


def get_field(jobj, field: str, optional=False) -> Optional[Any]:
    try:
        if optional:
            return from_optional(getattr(jobj, field)())
        else:
            return getattr(jobj, field)()
    except:
        logger.exception('Error using java field "%s"', field)
        return None


def get_java_values(jobj, fields: Iterable[str], exclude: Iterable[str] = None) -> Dict[str, Optional[Any]]:
    exclude = exclude or tuple()
    result = {}  # type: Dict[str, Optional[Any]]
    for field in fields:
        if field in exclude:
            continue
        result[field] = get_field(jobj, field)

    return result


def from_scala_seq(jobj):
    return [jobj.apply(i) for i in range(jobj.length())]


def dotget(obj, *path: str, default=None) -> Optional[Any]:
    for field in path:
        obj = getattr(obj, field, None)
        if obj is None:
            return default
    return obj
