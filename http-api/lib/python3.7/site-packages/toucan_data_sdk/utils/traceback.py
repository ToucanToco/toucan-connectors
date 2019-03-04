import joblib
import types
import io
import os


def _from_pickled_dict(d):
    def from_picklable(obj):
        if obj is None:
            return None
        try:
            f = io.BytesIO(obj)
            return joblib.load(f)
        except Exception:
            return None
    unpickled = {k: from_picklable(v) for k, v in d.items()}
    unpickling_failed = {k: v for k, v in unpickled.items() if v is None}
    unpickled = {k: v for k, v in unpickled.items() if v is not None}
    unpickled['__unpickling_failed__'] = list(unpickling_failed.keys())
    return unpickled


def _from_serializable_traceback(d):
    tb = d['tb']
    tb = types.SimpleNamespace(**tb)
    tb.tb_frame = types.SimpleNamespace(**tb.tb_frame)
    tb.tb_frame.f_code = types.SimpleNamespace(**tb.tb_frame.f_code)
    tb.tb_frame.f_locals = _from_pickled_dict(tb.tb_frame.f_locals)
    tb.tb_frame.f_globals = _from_pickled_dict(tb.tb_frame.f_globals)
    return d['exc_type'], d['exc_value'], tb


def _print_tb(exc_value, tb):
    lineno = tb.tb_lineno
    lines = tb.sourcecode.splitlines()

    relative_err_lineno = min(11, lineno)
    first_lineno = max(0, lineno - 12)
    last_lineno = min(len(lines), lineno + 4)

    lines = lines[first_lineno:last_lineno]
    lines_numbers = list(range(first_lineno + 1, last_lineno + 1))
    lines_numbers = [str(n).rjust(4) + '|' for n in lines_numbers]
    lines_numbers[relative_err_lineno] = '====>'

    for lineno, line in zip(lines_numbers, lines):
        print(lineno + line)
    print('−−−−−−−−−−−−')
    print(exc_value)


def _inject_into_globals(tb):
    tb_values = {}
    for k, v in tb.tb_frame.f_globals.items():
        if k not in globals():
            tb_values[k] = v
    for k, v in tb.tb_frame.f_locals.items():
        if k not in globals():
            tb_values[k] = v
    return tb_values


def load_traceback(filename):
    stb = joblib.load(filename)
    os.remove(filename)
    exc_type, exc_value, tb = _from_serializable_traceback(stb)
    _print_tb(exc_value, tb)
    return _inject_into_globals(tb)
