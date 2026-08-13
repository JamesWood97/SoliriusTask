class Filter:
    def __init__(self, column: str, operation: str, values: tuple):
        self._column = column
        if any(isinstance(values, x) for x in (str, float, int, bool)):
            self._values = values,
        elif isinstance(values, (list, tuple)):
            self._values = values
        else:
            raise TypeError
        self._operation: str = operation

    @property
    def column(self):
        return self._column

    @property
    def values(self):
        return self._values

    @property
    def operation(self):
        return self._operation

    @values.setter
    def values(self, values):
        self._values = values
