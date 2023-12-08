import inspect
import typeguard

def check_value_type(value, type_hint):
    if type_hint is inspect.Signature.empty:
        return value
    else:
        return typeguard.check_type(value, type_hint)


def check_function_param_types(func, *args, **kwargs):
    func_signature = inspect.signature(func)
    func_params = {p.name: p.annotation for p in func_signature.parameters.values()}
    # check args types against type hints
    for arg, arg_type in zip(args, func_params.values()):
        check_value_type(arg, arg_type)
    # check kwargs types against type hints
    for name, value in kwargs.items():
        check_value_type(value, func_params[name])
    return func_params


def check_function_return_type(func, return_value):
    func_signature = inspect.signature(func)
    func_return = func_signature.return_annotation
    # check return_value against type hint
    if func_return is not inspect.Signature.empty:
        try:
            check_value_type(return_value, func_return)
        except typeguard.TypeCheckError as e:
            print(f"return type {type(return_value)} of function '{func.__name__}' does not match type hint: {func_return}")
            raise e
    return func_return


