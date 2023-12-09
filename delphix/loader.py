import yaml
import importlib

def instantiate_class_from_yaml(yaml_file):
    # Load the YAML file
    with open(yaml_file, 'r') as file:
        data = yaml.safe_load(file)

    # Extract the class name and parameters
    class_path = data['class']
    params = data['params']

    # Dynamically import the class
    module_name, class_name = class_path.rsplit(".", 1)
    MyClass = getattr(importlib.import_module(module_name), class_name)

    # Instantiate the class with the parameters
    instance = MyClass(**params)

    return instance

if __name__ == "__main__":
    import os
    print(os.getcwd())
    # Instantiate the class
    instance = instantiate_class_from_yaml("delphix/example.yaml")

    # Call the method
    instance.method()