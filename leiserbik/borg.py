# ref: https://baites.github.io/computer-science/patterns/2018/06/11/python-borg-and-the-new-metaborg.html
class Borg():
    _state = {}

    def __new__(cls, *args, **kwargs):
        instance = super().__new__(cls, *args, **kwargs)
        instance.__dict__ = cls._state
        return instance
