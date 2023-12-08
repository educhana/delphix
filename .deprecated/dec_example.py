
import functools





def task(**task_kwargs):
    class InnerTaskDecorator:
        def __init__(self, func):
            # update the wrapper's metadata to match the original func's
            functools.update_wrapper(self, func)
            # store the original parameters
            self.func = func
            self.task_kwargs = task_kwargs
                
        def __call__(self, *args, **kwargs):
            self.args = args
            self.kwargs = kwargs
            return self

        def run(self):
            return self.func(*self.args, **self.kwargs)

        def __get__(self, instance, owner):
            # bound the method to the instance
            # bound the instance (which is self) to first argument of the method
            # note that self.__call__ is already bound to this self
            return functools.partial(self.__call__, instance)


    return InnerTaskDecorator



@task(param1="some value")
def my_first_task(msg):
    print(msg[::-1])
    return len(msg)

my_first_task("normal function")





class Other:
    def __init__(self, p=3):
        self.p = p

    @classmethod
    @task(param1="some value")
    def class_method(cls, msg):
        print(msg)
        return len(msg)        

    @task(feature_store="unity catalog")
    def task_method(self, msg):
        print("p", self.p, msg[::-1])
        return len(msg)
Other.class_method("class method")

other = Other()
print(other.task_method("method of instance"))