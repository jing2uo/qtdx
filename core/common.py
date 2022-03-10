from enum import IntEnum


class StockState(IntEnum):
    def __str__(self):
        return int(self.value)
    NONE = 0
    LISTED = 1
    PAUSE = 2
    DELIST = 3
