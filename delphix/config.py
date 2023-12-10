import yaml

from scheduler import LocalScheduler





def scheduler_constructor(loader, node):
    assert node.tag == "!Scheduler"
    #for child in node.value:
    #    print(child)

    return LocalScheduler()



def build_stack(yaml_stack):
    loader = yaml.SafeLoader
    loader.add_constructor("!Scheduler", scheduler_constructor)

    with open("./stack.yaml", "r") as f:
        yaml_stack = yaml.load(f, Loader=loader)
        stack = build_stack(yaml_stack)
        print(stack)


if __name__ == "__main__":
    stack = build_stack("./stack.yaml")
