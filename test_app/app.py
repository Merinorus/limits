from fastapi import FastAPI, Request
from fastapi.responses import PlainTextResponse, Response
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address

limiter = Limiter(key_func=get_remote_address, strategy="approximated-moving-window")
app = FastAPI()
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)


# Note: the route decorator must be above the limit decorator, not below it
@app.get("/home")
@limiter.limit("10/10 seconds")
async def homepage(request: Request):
    return PlainTextResponse("test")
