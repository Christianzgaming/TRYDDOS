import json
import asyncio
import aiohttp
import ssl
import time
import random
from http.server import BaseHTTPRequestHandler
from urllib.parse import parse_qs, urlparse
from datetime import datetime
from collections import deque
from dataclasses import dataclass, field
from typing import Optional, List, Dict

# ============================================
# CONFIGURATION
# ============================================

VERSION = "5.0.0"
DEFAULT_TARGET = "https://172.65.55.227:19443/"
DEFAULT_CONCURRENCY = 5
DEFAULT_DURATION = 3
DEFAULT_TIMEOUT = 5

# SSL Configuration
SSL_CONTEXT = ssl.create_default_context()
SSL_CONTEXT.check_hostname = False
SSL_CONTEXT.verify_mode = ssl.CERT_NONE

# Headers
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Firefox/121.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) Safari/605.1.15",
]

HEADERS = {
    "accept": "*/*",
    "accept-encoding": "gzip, deflate",
    "connection": "keep-alive",
    "cache-control": "no-cache",
}

# ============================================
# DATA CLASSES
# ============================================

@dataclass
class RequestResult:
    status: int
    latency: float
    success: bool = False
    error: Optional[str] = None
    target_url: str = ""
    
    def __post_init__(self):
        self.success = 200 <= self.status < 400 if self.status > 0 else False

@dataclass
class Stats:
    total_requests: int = 0
    successful: int = 0
    failed: int = 0
    timeouts: int = 0
    start_time: float = 0.0
    end_time: float = 0.0
    latencies: deque = field(default_factory=lambda: deque(maxlen=1000))
    
    def add_result(self, result: RequestResult):
        self.total_requests += 1
        if result.success:
            self.successful += 1
        else:
            self.failed += 1
            if result.status == 0:
                self.timeouts += 1
        if result.latency > 0:
            self.latencies.append(result.latency)
    
    @property
    def success_rate(self) -> float:
        if self.total_requests == 0:
            return 0.0
        return (self.successful / self.total_requests) * 100
    
    @property
    def elapsed(self) -> float:
        if self.end_time > 0:
            return self.end_time - self.start_time
        return time.time() - self.start_time
    
    @property
    def rps(self) -> float:
        elapsed = self.elapsed
        if elapsed == 0:
            return 0.0
        return self.total_requests / elapsed
    
    def get_percentile(self, p: float) -> float:
        if not self.latencies:
            return 0.0
        sorted_lat = sorted(self.latencies)
        idx = int(len(sorted_lat) * (p / 100))
        if idx >= len(sorted_lat):
            idx = len(sorted_lat) - 1
        return sorted_lat[idx]

# ============================================
# FAST WORKER
# ============================================

class FastWorker:
    __slots__ = ['session', 'url', 'headers']
    
    def __init__(self, session: aiohttp.ClientSession, url: str):
        self.session = session
        self.url = url
        self.headers = HEADERS.copy()
        self.headers["user-agent"] = random.choice(USER_AGENTS)
    
    async def fire(self) -> RequestResult:
        start = time.perf_counter()
        try:
            async with self.session.get(
                self.url,
                headers=self.headers,
                ssl=SSL_CONTEXT,
                timeout=aiohttp.ClientTimeout(total=5)
            ) as response:
                await response.read()
                return RequestResult(
                    status=response.status,
                    latency=time.perf_counter() - start,
                    target_url=self.url
                )
        except asyncio.TimeoutError:
            return RequestResult(status=0, latency=time.perf_counter() - start, error="timeout", target_url=self.url)
        except Exception as e:
            return RequestResult(status=0, latency=time.perf_counter() - start, error=str(e), target_url=self.url)

# ============================================
# STRESSER ENGINE
# ============================================

class VercelStressEngine:
    def __init__(self, target: str, concurrency: int, duration: int):
        self.target = target
        self.concurrency = concurrency
        self.duration = duration
        self.stats = Stats()
        self.workers: List[asyncio.Task] = []
        self.session: Optional[aiohttp.ClientSession] = None
        
    async def start(self) -> Dict:
        self.stats.start_time = time.time()
        
        connector = aiohttp.TCPConnector(
            limit=self.concurrency,
            limit_per_host=self.concurrency,
            enable_cleanup_closed=True,
            ssl=SSL_CONTEXT,
        )
        
        self.session = aiohttp.ClientSession(
            connector=connector,
            headers=HEADERS,
            timeout=aiohttp.ClientTimeout(total=5)
        )
        
        end_time = time.time() + self.duration
        
        for i in range(self.concurrency):
            worker = FastWorker(self.session, self.target)
            task = asyncio.create_task(self._worker_loop(worker, end_time))
            self.workers.append(task)
        
        await asyncio.sleep(self.duration)
        
        for worker in self.workers:
            worker.cancel()
        await asyncio.gather(*self.workers, return_exceptions=True)
        
        if self.session:
            await self.session.close()
        
        self.stats.end_time = time.time()
        
        return self._generate_report()
    
    async def _worker_loop(self, worker: FastWorker, end_time: float):
        while time.time() < end_time:
            result = await worker.fire()
            self.stats.add_result(result)
            await asyncio.sleep(0.01)
    
    def _generate_report(self) -> Dict:
        return {
            "status": "success",
            "version": VERSION,
            "timestamp": datetime.now().isoformat(),
            "config": {
                "target": self.target,
                "concurrency": self.concurrency,
                "duration": self.duration,
            },
            "results": {
                "total_requests": self.stats.total_requests,
                "successful": self.stats.successful,
                "failed": self.stats.failed,
                "timeouts": self.stats.timeouts,
                "success_rate": round(self.stats.success_rate, 2),
                "rps": round(self.stats.rps, 2),
                "elapsed": round(self.stats.elapsed, 2),
                "p50": round(self.stats.get_percentile(50), 4),
                "p90": round(self.stats.get_percentile(90), 4),
                "p95": round(self.stats.get_percentile(95), 4),
                "p99": round(self.stats.get_percentile(99), 4),
            }
        }

# ============================================
# VERCEL HANDLER
# ============================================

class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        # Parse query parameters
        parsed = urlparse(self.path)
        params = parse_qs(parsed.query)
        
        target = params.get('target', [DEFAULT_TARGET])[0]
        concurrency = int(params.get('concurrency', [DEFAULT_CONCURRENCY])[0])
        duration = int(params.get('duration', [DEFAULT_DURATION])[0])
        
        try:
            # Run stress test
            engine = VercelStressEngine(target, concurrency, duration)
            result = asyncio.run(engine.start())
            
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps(result, indent=2).encode())
            
        except Exception as e:
            self.send_response(500)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({"error": str(e)}).encode())
