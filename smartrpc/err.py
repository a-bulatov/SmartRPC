class ErrorRPC(Exception):

    def __init__(self, code, message=None):
        if isinstance(code, tuple):
            if message is None:
                message = code[1]
            elif message.startswith('+'):
                message = code[1] + '. ' + message[1:]
            code = code[0]
        self.__code = code
        self.__message = message
        Exception.__init__(self, message)

    @property
    def code(self):
        return self.__code

    @property
    def args(self):
        return [self.__message,]

    def message(self, id=None):
        return {
            "jsonrpc": "2.0", "result": None,
            "id": id,
            "error": {"code": self.code, "message": self.__message}
        }

ERR_PARSE =     (-32700, "Parse error")
ERR_REQUEST =   (-32600, "Invalid Request")
ERR_NOT_FOUND = (-32601, "Method not found")
ERR_BAD_PARAMS =(-32602, "Invalid params")
ERR_INTERNAL =  (-32603, "Internal error")
ERR_SERVER =    (-32000, "Server error")

ERR_CONNECT =   (-32701, "Connection error")

def error(code=ERR_INTERNAL, message=None, json_msg=None):
    if isinstance(code, tuple):
        if message is None: message = code[1]
        code = code[0]
    return {
        "jsonrpc": "2.0", "result": None,
        "id": json_msg.get('id') if json_msg else None,
        "error": {"code": code, "message": message}
    }