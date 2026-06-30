
from typing import Final

# Note: this is a bit rough around the edges but it gets the job done for our use case


class MetricLabelInstance:
    """Label with value"""
    def __init__(self, name: str, value: str):
        self.name: Final[str] = name
        self.value: Final[str] = value


class MetricLabel:
    """Basically, this is just used to store the label name and generate a `MetricLabelInstance`"""
    def __init__(self, name: str):
        self._name: Final[str] = name
    def __call__(self, value: str) -> MetricLabelInstance :
        return MetricLabelInstance(self._name, value)


class MetricDataPoint:
    """A metric value associated with a collection of labels

    - Does not check for the unicity of label names
    """

    def __init__(self, labels: list[MetricLabelInstance], value: float):
        self.labels = labels
        self.value = value


class MetricSection:
    """A metric containing values associated with labels

    - Does not check for the unicity of label sets
    - Does not check for the validity of strings (metric name, type, labels, ...)
    """

    def __init__(self, name: str, type: str, description: str):
        self._name: Final[str] = name
        self._type: Final[str] = type
        self._description: Final[str] = description
        self._points: list[MetricDataPoint] = []

    def add(self, labels: list[MetricLabelInstance], value: float):
        self._points.append(MetricDataPoint(labels, value))

    def write(self, prefix: str, out: list[str]):
        if len(self._points) == 0 :
            return
        full_name = f"{prefix}_{self._name}"
        out.append(f"# HELP {full_name} {self._description}")
        out.append(f"# TYPE {full_name} {self._type}")
        for pt in self._points :
            labels = ("{" + ", ".join(f"{l.name}=\"{l.value}\"" for l in pt.labels) + "}") if len(pt.labels) > 0 else ""
            out.append(f"{full_name}{labels} {pt.value}")
