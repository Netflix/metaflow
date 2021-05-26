# Type of the message
FIELD_MSGTYPE = "t"
MSG_OP = 1  # This is an operation
MSG_REPLY = 2  # This is a regular reply
MSG_EXCEPTION = 3  # This is an exception
MSG_CONTROL = 4  # This is a control message
MSG_INTERNAL_ERROR = 5  # Some internal error happened

# Fields for operations/control
FIELD_OPTYPE = "o"
FIELD_TARGET = "o_t"
FIELD_ARGS = "o_a"
FIELD_KWARGS = "o_ka"

# Fields for reply/exception
FIELD_CONTENT = "c"

# Fields for values
# Indicates that the object is remote to the receiver (and local to the sender)
VALUE_REMOTE = 1
# Indicates that the object is local to the receiver (and remote to the sender)
VALUE_LOCAL = 2

# Operations that we support
OP_GETATTR = 1
OP_SETATTR = 2
OP_DELATTR = 3
OP_CALL = 4
OP_CALLATTR = 5
OP_REPR = 6
OP_STR = 7
OP_HASH = 9
OP_PICKLE = 10
OP_DEL = 11
OP_GETMETHODS = 12
OP_DIR = 13
OP_CALLFUNC = 14
OP_GETVAL = 15
OP_SETVAL = 16
OP_INIT = 17
OP_CALLONCLASS = 18

# Control messages
CONTROL_SHUTDOWN = 1
CONTROL_GETEXPORTS = 2
