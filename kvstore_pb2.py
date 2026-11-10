import json

class ReadConsistency:
    LINEARIZABLE = 0
    FOLLOWER_STALE = 1

class WriteStrategy:
    QUORUM_CP = 0
    ALWAYS_AP = 1

class BaseMessage:
    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)

    def SerializeToString(self):
        data = {k: v for k, v in self.__dict__.items() if not k.startswith('_')}
        return json.dumps(data).encode('utf-8')

    @classmethod
    def FromString(cls, byte_data):
        data = json.loads(byte_data.decode('utf-8'))
        return cls(**data)

class GetRequest(BaseMessage):
    def __init__(self, key="", consistency=0, max_staleness_ms=0):
        super().__init__(key=key, consistency=consistency, max_staleness_ms=max_staleness_ms)

class GetReply(BaseMessage):
    def __init__(self, value="", found=False, read_timestamp=0, is_stale=False, error_message=""):
        super().__init__(value=value, found=found, read_timestamp=read_timestamp, is_stale=is_stale, error_message=error_message)

class PutRequest(BaseMessage):
    def __init__(self, key="", value="", idempotency_key="", strategy=0, timestamp=0):
        super().__init__(key=key, value=value, idempotency_key=idempotency_key, strategy=strategy, timestamp=timestamp)

class PutReply(BaseMessage):
    def __init__(self, success=False, acks_received=0, error_message=""):
        super().__init__(success=success, acks_received=acks_received, error_message=error_message)
