from yaml import load, Loader

class InputParser(object):
    """
    PArse the input YAML and feed it to the Admin
    """


    def __init__(self, input_file):
        with open(input_file, "r") as fp:
            self.data = load(fp.read(),Loader=Loader)

    def create_topics(self):
        """
        Create any topics defined in the YAML
        """
        pass

    def create_schemas(self):
        """
        Create any schemas defined in the YAML
        """
        pass
    def create_rolebindings(self):
        """
        Create an rolebindigns defined in the YAML
        """
        pass
