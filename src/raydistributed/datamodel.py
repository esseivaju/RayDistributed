import ray


class EventContext:

    def __init__(self, id) -> None:
        self._id = id

    @property
    def id(self):
        return self._id
