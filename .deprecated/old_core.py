import functools
import inspect
from delphix import typing_util





def task(cache_results=False):

    class InnerTaskDecorator:
        def __init__(self, func):
            functools.update_wrapper(self, func)
            self.func = func
       

        def __call__(self, *args, **kwargs):
            # save args for later
            self.call_args = args
            self.call_kwargs = kwargs

            # check args types against function type hints
            typing_util.check_function_param_types(self.func, *args, **kwargs)

            # return self for chaining
            return self
        

        def __get__(self, instance, owner):
            # if called from instance, bind __call__ to instance
            if instance is not None:
                return functools.partial(self.__call__, instance)
            # else return self
            return self


        def run(self):
            return_value = self.func(*self.call_args, **self.call_kwargs)
            # check return_value against type hint
            typing_util.check_function_return_type(self.func, return_value)
            return return_value
        
        
    return InnerTaskDecorator



