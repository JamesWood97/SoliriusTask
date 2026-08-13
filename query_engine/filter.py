class Filter:
    def __init__(self, column: str, operation: str, values: tuple):
        self.column = column
        self.values = values
        self.operation: str = operation


    @property
    def column(self):
        return self._column

    @column.setter
    def column(self, column):
        self._column = column

    @property
    def values(self):
        return self._values

    @property
    def operation(self):
        return self._operation

    @operation.setter
    def operation(self, operation):
        self._operation = operation
        if self._operation == "between" and len(self._values) != 2:
            raise ValueError("Between operation requires exactly two values.")

    @values.setter
    def values(self, values):
        if any(isinstance(values, x) for x in (str, float, int, bool)):
            self._values = values,
        elif isinstance(values, (list, tuple)):
            self._values = values
        else:
            raise TypeError

