from .core import *

import importlib
import sys

# load dynamically the module name passed as argument
def load_module(module_name):
    # load the module containing the pipeline
    module = __import__(module_name)
    return module



def load_pipeline(pipeline_name):
    # check if the pipeline name is fully qualified
    if "::" in pipeline_name:
        module_name, pipeline_name = pipeline_name.split("::")
    else:
        module_name = pipeline_name


