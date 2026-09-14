namespace rs biubin
namespace js biubin

enum ErrorKind {
  INVALID = 1,
  INTERNAL = 2,
  TIMEOUT = 3,
}

struct EchoRequest {
  1: required string message,
  2: optional binary payload,
}

struct EchoResponse {
  1: required string message,
  2: required binary payload,
}

service BiubinService {
  EchoResponse echo(1: EchoRequest request),
  i64 sum(1: list<i64> values),
  void raiseError(1: ErrorKind kind),
  oneway void notify(1: string message),
}
