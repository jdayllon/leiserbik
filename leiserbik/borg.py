# ref: https://baites.github.io/computer-science/patterns/2018/06/11/python-borg-and-the-new-metaborg.html
class Borg():
    _state = {}

    def __new__(cls, *args, **kwargs):
        instance = super().__new__(cls, *args, **kwargs)
        instance.__dict__ = cls._state
        return instance


class MetaBorg(type):
    _state = {"__skip_init__": False}

    def __check_args(cls, *args, **kwargs):
        nargs = len(args)
        if nargs > 0:
            raise TypeError(
                '{}() takes 0 positional arguments after first initialization but {} was given'.format(
                    cls.__name__, nargs
                )
            )
        nkeys = len(kwargs)
        if nkeys > 0:
            raise TypeError(
                "{}() got an unexpected keyword argument '{}' after first initialization".format(
                    cls.__name__, list(kwargs.keys())[0]
                )
            )

    def __call__(cls, *args, **kwargs):
        if cls._state['__skip_init__']:
            cls.__check_args(*args, **kwargs)
        instance = object().__new__(cls, *args, **kwargs)
        instance.__dict__ = cls._state
        if not cls._state['__skip_init__']:
            instance.__init__(*args, **kwargs)
            cls._state['__skip_init__'] = True
        return instance

from kafka import KafkaProducer
from leiserbik import LEISERBIK_TOPIC_STATUS_ID


class Kakfa(metaclass=MetaBorg):
    def __init__(self, kafka_servers):
        self.producer = KafkaProducer(bootstrap_servers=kafka_servers)
        self.topic = LEISERBIK_TOPIC_STATUS_ID
