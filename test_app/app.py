import time
from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse, PlainTextResponse
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address
from io import BytesIO

# redis_client = redis.StrictRedis(host='localhost', port=6379, db=0, decode_responses=True)

# limiter = Limiter(key_func=get_remote_address, strategy="sliding-window-counter", headers_enabled=True)
limiter = Limiter(key_func=get_remote_address, strategy="sliding-window-counter", storage_uri="redis://localhost:7379/0", headers_enabled=True)
# limiter = Limiter(key_func=get_remote_address, strategy="sliding-window-counter", storage_uri="memcached://localhost:22122")
# limiter = Limiter(key_func=get_remote_address, strategy="sliding-window-counter", storage_uri="memcached://localhost:22122", headers_enabled=True)

# limiter = Limiter(key_func=get_remote_address, strategy="sliding-window-counter")

# def _inject_headers(
#         self, response: Response, current_limit: Tuple[RateLimitItem, List[str]]
#     ) -> Response:
#         if self.enabled and self._headers_enabled and current_limit is not None:
#             if not isinstance(response, Response):
#                 raise Exception(
#                     "parameter `response` must be an instance of starlette.responses.Response"
#                 )
#             try:
#                 window_stats: Tuple[int, int] = self.limiter.get_window_stats(
#                     current_limit[0], *current_limit[1]
#                 )
#                 reset_in = 1 + window_stats[0]
#                 response.headers.append(
#                     self._header_mapping[HEADERS.LIMIT], str(current_limit[0].amount)
#                 )
#                 response.headers.append(
#                     self._header_mapping[HEADERS.REMAINING], str(window_stats[1])
#                 )
#                 response.headers.append(
#                     self._header_mapping[HEADERS.RESET], str(reset_in)
#                 )

#                 # response may have an existing retry after
#                 existing_retry_after_header = response.headers.get("Retry-After")

#                 if existing_retry_after_header is not None:
#                     reset_in = max(
#                         self._determine_retry_time(existing_retry_after_header),
#                         reset_in,
#                     )

#                 response.headers[self._header_mapping[HEADERS.RETRY_AFTER]] = (
#                     formatdate(reset_in)
#                     if self._retry_after == "http-date"
#                     else str(int(reset_in - time.time()))
#                 )
#             except:
#                 if self._in_memory_fallback and not self._storage_dead:
#                     self.logger.warning(
#                         "Rate limit storage unreachable - falling back to"
#                         " in-memory storage"
#                     )
#                     self._storage_dead = True
#                     response = self._inject_headers(response, current_limit)
#                 if self._swallow_errors:
#                     self.logger.exception(
#                         "Failed to update rate limit headers. Swallowing error"
#                     )
#                 else:
#                     raise
#         return response

def _my_rate_limit_exceeded_handler(request: Request, exc: RateLimitExceeded) -> Response:
    """
    Build a simple JSON response that includes the details of the rate limit
    that was hit. If no limit is hit, the countdown is added to headers.
    """
    response = JSONResponse(
        {"error": f"Rate limit exceeded: {exc.detail}"}
        # {}
        # {"error": f"Rate limit exceeded: {exc.detail}", "details": {"Reset in": f"{time.time() - request.app.state.limiter.limiter.get_window_stats().reset_time} seconds", "Remaining": request.app.state.limiter.limiter.get_window_stats().remaining}}, status_code=429
    )
    response = request.app.state.limiter._inject_headers(
        response, request.state.view_rate_limit
    )
    now = time.time()
    reset = float(response.headers.get("X-Ratelimit-Reset") or now)
    reset_in = reset - now
    remaining = response.headers.get("X-Ratelimit-Remaining")
    
    response = JSONResponse(
        {"error": f"Rate limit exceeded: {exc.detail}", "reset in": f"{reset_in}", "remaining": remaining}
        # {}
        # {"error": f"Rate limit exceeded: {exc.detail}", "details": {"Reset in": f"{time.time() - request.app.state.limiter.limiter.get_window_stats().reset_time} seconds", "Remaining": request.app.state.limiter.limiter.get_window_stats().remaining}}, status_code=429
    )
    response.status_code = 429
    response = request.app.state.limiter._inject_headers(
        response, request.state.view_rate_limit
    )
    return response

app = FastAPI()
app.state.limiter = limiter
# app.add_exception_handler(RateLimitExceeded, _my_rate_limit_exceeded_handler)
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)


# # Note: the route decorator must be above the limit decorator, not below it
# @app.get("/home")
# @limiter.limit("3/10 second")
# async def homepage(request: Request):
#     response = PlainTextResponse("test")
#     response = request.app.state.limiter._inject_headers(
#         response, request.state.view_rate_limit
#     )
#     now = time.time()
#     reset = float(response.headers.get("X-Ratelimit-Reset") or now)
#     reset_in = reset - now
#     remaining = response.headers.get("X-Ratelimit-Remaining")
#     response = PlainTextResponse(f"Remaining: {remaining}. Ratelimiter reset in {reset_in} seconds")
#     return response

# Note: the route decorator must be above the limit decorator, not below it
@app.get("/home")
@limiter.limit("3/10 second")
# @limiter.limit("1/1 second")
async def homepage(request: Request):
    response = PlainTextResponse("test")
    return response

@app.get("/favicon.ico")
async def favicon(request: Request):
    dummy_icon = (
        b"\x00\x00\x01\x00"  # Header
        b"\x01\x00\x10\x10"  # Image directory
        b"\x01\x00\x04\x00"  # Color depth
        b"\x00\x00\x00\x00\x00\x00\x16\x00"  # Reserved, offset
        b"\x28\x00\x00\x00"  # Bitmap header
        b"\x01\x00\x01\x00\x01\x00\x18\x00"  # Pixel dimensions
        b"\x00\x00\x00\x00"  # Compression
        b"\x00\x00\x00\x00"  # Size
        b"\x00\x00\x00\x00"  # Horizontal/Vertical PPM
        b"\x00\x00\x00\x00"  # Color table
        b"\x00\x00\x00\x00\x00\x00\x00\x00"  # Pixel data
    )
    return Response(content=dummy_icon, media_type="image/x-icon")