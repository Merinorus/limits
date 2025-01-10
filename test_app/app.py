from fastapi import FastAPI, Request
from fastapi.responses import PlainTextResponse
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address

# redis_client = redis.StrictRedis(host='localhost', port=6379, db=0, decode_responses=True)

# limiter = Limiter(key_func=get_remote_address, strategy="sliding-window-counter", storage_uri="redis://localhost:7379/0")
limiter = Limiter(key_func=get_remote_address, strategy="sliding-window-counter", storage_uri="memcached://localhost:22122")

# limiter = Limiter(key_func=get_remote_address, strategy="sliding-window-counter")

app = FastAPI()
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)


# Note: the route decorator must be above the limit decorator, not below it
@app.get("/home")
@limiter.limit("2/1 second")
async def homepage(request: Request):
    return PlainTextResponse("test")
