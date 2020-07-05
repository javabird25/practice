from typing import Optional, List
from math import sqrt, ceil


class Matrix:
    def __init__(self):
        self.matrix = []

    def add_item(self, element: Optional = None):
        if element is None:
            raise ValueError

        self.matrix.append(element)

    def pop(self):
        return self.matrix.pop()

    def __str__(self):
        string = ""
        # Минимальный размер измерения квадратной матрицы без учета пустых строк
        dim_size = ceil(sqrt(len(self.matrix)))
        # Свободные места в матрице
        free_cells = dim_size ** 2 - len(self.matrix)
        # Если их меньше, чем размер измерения, нужно добавить свободную строку
        if free_cells < dim_size:
            dim_size += 1

        for y in range(dim_size):
            row_offset = y * dim_size
            for x in range(dim_size):
                try:
                    elem = self.matrix[row_offset + x]
                except IndexError:
                    elem = None
                string += str(elem) + ("\n" if x == dim_size - 1 else " ")

        return string.strip()
