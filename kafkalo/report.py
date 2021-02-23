from pathlib import Path
from jinja2 import Environment, PackageLoader, select_autoescape

current = Path(__file__).parent
env = Environment(
    loader=PackageLoader("kafkalo", "templates"),
    autoescape=select_autoescape(["tmpl"]),
)


class Report(object):
    """
    A Report class. Can produce dry-run reports
    """

    def __init__(
        self,
        template="text.tmpl",
        topics_context=None,
        client_context=None,
        schema_context=None,
    ):
        self.template = template
        self.context = {}
        self.context["clients"] = client_context
        self.context["topics"] = topics_context
        self.context["schemas"] = schema_context

    def render(self):
        template = env.get_template(self.template)
        return template.render(self.context)
