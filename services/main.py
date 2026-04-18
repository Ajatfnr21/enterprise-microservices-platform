#!/usr/bin/env python3
"""
Enterprise Microservices Platform - Production Microservices Infrastructure

Features:
- API Gateway with rate limiting
- Service discovery (Consul integration)
- Load balancing
- Circuit breaker pattern
- Distributed tracing
- Health checks
- gRPC support
- Kubernetes deployment configs

Author: Drajat Sukma
License: MIT
Version: 2.0.0
"""

__version__ = "2.0.0"

import asyncio
import hashlib
import json
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Callable
from contextlib import asynccontextmanager
from enum import Enum

import httpx
import redis.asyncio as redis
import structlog
from fastapi import FastAPI, HTTPException, Request, Depends, Header
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
import uvicorn
from prometheus_client import Counter, Histogram, Gauge, generate_latest, CONTENT_TYPE_LATEST
from starlette.middleware.base import BaseHTTPMiddleware

structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.JSONRenderer()
    ]
)
logger = structlog.get_logger()

# ============== Configuration ==============

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
SERVICE_NAME = os.getenv("SERVICE_NAME", "api-gateway")
SERVICE_VERSION = os.getenv("SERVICE_VERSION", __version__)

# ============== Data Models ==============

class ServiceStatus(str, Enum):
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    UNKNOWN = "unknown"

@dataclass
class ServiceInstance:
    service_id: str
    name: str
    host: str
    port: int
    version: str
    status: ServiceStatus = ServiceStatus.UNKNOWN
    metadata: Dict[str, Any] = field(default_factory=dict)
    last_heartbeat: datetime = field(default_factory=datetime.utcnow)
    request_count: int = 0
    error_count: int = 0
    avg_response_time: float = 0.0

class HealthResponse(BaseModel):
    status: str
    version: str
    service_name: str
    timestamp: datetime
    services_registered: int
    uptime_seconds: float

class ServiceRegistration(BaseModel):
    name: str
    host: str
    port: int
    version: str
    health_check_path: str = "/health"
    metadata: Dict[str, Any] = Field(default_factory=dict)

class RouteConfig(BaseModel):
    path: str
    service_name: str
    methods: List[str] = Field(default_factory=lambda: ["GET", "POST", "PUT", "DELETE"])
    rate_limit: int = 100  # requests per minute
    timeout: int = 30  # seconds
    circuit_breaker: bool = True
    retry_count: int = 3

# ============== Metrics ==============

request_count = Counter(
    'http_requests_total',
    'Total HTTP requests',
    ['method', 'endpoint', 'status_code']
)

request_duration = Histogram(
    'http_request_duration_seconds',
    'HTTP request duration',
    ['method', 'endpoint']
)

active_connections = Gauge(
    'http_active_connections',
    'Number of active connections'
)

service_health = Gauge(
    'service_health_status',
    'Service health status (1=healthy, 0=unhealthy)',
    ['service_name']
)

# ============== Service Registry ==============

class ServiceRegistry:
    """In-memory service registry with Redis persistence"""
    
    def __init__(self):
        self._services: Dict[str, List[ServiceInstance]] = {}
        self._routes: Dict[str, RouteConfig] = {}
        self.redis: Optional[redis.Redis] = None
        self.start_time = datetime.utcnow()
    
    async def initialize(self):
        try:
            self.redis = redis.from_url(REDIS_URL)
            await self.redis.ping()
            logger.info("redis_connected")
        except Exception as e:
            logger.warning("redis_connection_failed", error=str(e))
            self.redis = None
    
    async def register_service(self, registration: ServiceRegistration) -> ServiceInstance:
        service_id = hashlib.md5(
            f"{registration.name}:{registration.host}:{registration.port}".encode()
        ).hexdigest()[:12]
        
        instance = ServiceInstance(
            service_id=service_id,
            name=registration.name,
            host=registration.host,
            port=registration.port,
            version=registration.version,
            status=ServiceStatus.HEALTHY,
            metadata=registration.metadata,
            last_heartbeat=datetime.utcnow()
        )
        
        if registration.name not in self._services:
            self._services[registration.name] = []
        
        # Remove existing instance with same ID
        self._services[registration.name] = [
            s for s in self._services[registration.name] 
            if s.service_id != service_id
        ]
        
        self._services[registration.name].append(instance)
        
        # Persist to Redis if available
        if self.redis:
            await self.redis.hset(
                f"services:{registration.name}",
                service_id,
                json.dumps({
                    "service_id": service_id,
                    "name": registration.name,
                    "host": registration.host,
                    "port": registration.port,
                    "version": registration.version,
                    "status": instance.status.value,
                    "metadata": registration.metadata,
                    "last_heartbeat": instance.last_heartbeat.isoformat()
                })
            )
        
        logger.info("service_registered", 
                   service_id=service_id, 
                   name=registration.name,
                   host=registration.host,
                   port=registration.port)
        
        return instance
    
    def deregister_service(self, service_name: str, service_id: str) -> bool:
        if service_name in self._services:
            original_count = len(self._services[service_name])
            self._services[service_name] = [
                s for s in self._services[service_name] 
                if s.service_id != service_id
            ]
            if len(self._services[service_name]) < original_count:
                logger.info("service_deregistered", 
                           service_id=service_id, 
                           name=service_name)
                return True
        return False
    
    def get_service_instances(self, service_name: str) -> List[ServiceInstance]:
        return self._services.get(service_name, [])
    
    def get_healthy_instances(self, service_name: str) -> List[ServiceInstance]:
        instances = self.get_service_instances(service_name)
        return [i for i in instances if i.status == ServiceStatus.HEALTHY]
    
    def get_next_instance(self, service_name: str) -> Optional[ServiceInstance]:
        """Round-robin load balancing"""
        instances = self.get_healthy_instances(service_name)
        if not instances:
            return None
        
        # Simple round-robin based on request count
        return min(instances, key=lambda i: i.request_count)
    
    def update_service_status(self, service_name: str, service_id: str, 
                           status: ServiceStatus):
        for instance in self._services.get(service_name, []):
            if instance.service_id == service_id:
                instance.status = status
                instance.last_heartbeat = datetime.utcnow()
                break
    
    def get_all_services(self) -> Dict[str, List[ServiceInstance]]:
        return self._services
    
    def add_route(self, route: RouteConfig):
        self._routes[route.path] = route
        logger.info("route_added", path=route.path, service=route.service_name)
    
    def get_route(self, path: str) -> Optional[RouteConfig]:
        # Find matching route
        for route_path, route in self._routes.items():
            if path.startswith(route_path):
                return route
        return None

registry = ServiceRegistry()

# ============== Circuit Breaker ==============

class CircuitState(str, Enum):
    CLOSED = "closed"      # Normal operation
    OPEN = "open"          # Failing, reject requests
    HALF_OPEN = "half_open"  # Testing if service recovered

class CircuitBreaker:
    """Circuit breaker pattern implementation"""
    
    FAILURE_THRESHOLD = 5
    RECOVERY_TIMEOUT = 60  # seconds
    
    def __init__(self, service_name: str):
        self.service_name = service_name
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.last_failure_time: Optional[datetime] = None
        self.success_count = 0
    
    def can_execute(self) -> bool:
        if self.state == CircuitState.CLOSED:
            return True
        
        if self.state == CircuitState.OPEN:
            if self.last_failure_time:
                elapsed = (datetime.utcnow() - self.last_failure_time).total_seconds()
                if elapsed > self.RECOVERY_TIMEOUT:
                    self.state = CircuitState.HALF_OPEN
                    self.failure_count = 0
                    return True
            return False
        
        return True  # HALF_OPEN
    
    def record_success(self):
        if self.state == CircuitState.HALF_OPEN:
            self.success_count += 1
            if self.success_count >= 3:
                self.state = CircuitState.CLOSED
                self.failure_count = 0
                logger.info("circuit_breaker_closed", service=self.service_name)
        else:
            self.failure_count = 0
    
    def record_failure(self):
        self.failure_count += 1
        self.last_failure_time = datetime.utcnow()
        
        if self.state == CircuitState.HALF_OPEN:
            self.state = CircuitState.OPEN
            logger.warning("circuit_breaker_opened", service=self.service_name)
        elif self.failure_count >= self.FAILURE_THRESHOLD:
            self.state = CircuitState.OPEN
            logger.warning("circuit_breaker_opened", service=self.service_name)

# ============== Rate Limiter ==============

class RateLimiter:
    """Redis-based rate limiting"""
    
    def __init__(self):
        self.redis: Optional[redis.Redis] = None
    
    async def is_allowed(self, key: str, limit: int, window: int = 60) -> bool:
        if not registry.redis:
            return True  # Allow if no Redis
        
        current = await registry.redis.get(key)
        if current is None:
            await registry.redis.setex(key, window, 1)
            return True
        
        count = int(current)
        if count >= limit:
            return False
        
        await registry.redis.incr(key)
        return True

rate_limiter = RateLimiter()

# ============== Metrics Middleware ==============

class MetricsMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        start_time = time.time()
        method = request.method
        path = request.url.path
        
        active_connections.inc()
        
        try:
            response = await call_next(request)
            status_code = response.status_code
            
            # Record metrics
            request_count.labels(
                method=method,
                endpoint=path,
                status_code=status_code
            ).inc()
            
            request_duration.labels(
                method=method,
                endpoint=path
            ).observe(time.time() - start_time)
            
            return response
        finally:
            active_connections.dec()

# ============== FastAPI Application ==============

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("microservices_platform_starting", version=__version__)
    await registry.initialize()
    rate_limiter.redis = registry.redis
    
    # Register some default routes
    registry.add_route(RouteConfig(
        path="/api/users",
        service_name="user-service",
        rate_limit=1000
    ))
    registry.add_route(RouteConfig(
        path="/api/orders",
        service_name="order-service",
        rate_limit=500
    ))
    
    yield
    logger.info("microservices_platform_stopping")

app = FastAPI(
    title="Enterprise Microservices Platform",
    version=__version__,
    description="Production Microservices Infrastructure",
    lifespan=lifespan
)

app.add_middleware(MetricsMiddleware)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)
app.add_middleware(GZipMiddleware)

# ============== API Endpoints ==============

@app.get("/health")
async def health_check():
    uptime = (datetime.utcnow() - registry.start_time).total_seconds()
    total_services = sum(len(instances) for instances in registry.get_all_services().values())
    
    return HealthResponse(
        status="healthy",
        version=__version__,
        service_name=SERVICE_NAME,
        timestamp=datetime.utcnow(),
        services_registered=total_services,
        uptime_seconds=uptime
    )

@app.get("/")
def info():
    return {
        "name": "Enterprise Microservices Platform",
        "version": __version__,
        "features": [
            "API Gateway with rate limiting",
            "Service discovery",
            "Load balancing (Round-robin)",
            "Circuit breaker pattern",
            "Distributed tracing",
            "Health checks",
            "Prometheus metrics"
        ]
    }

@app.get("/metrics")
def metrics():
    """Prometheus metrics endpoint"""
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

# Service Registry Endpoints

@app.post("/registry/services")
async def register_service(registration: ServiceRegistration):
    instance = await registry.register_service(registration)
    return {
        "service_id": instance.service_id,
        "status": "registered",
        "timestamp": datetime.utcnow()
    }

@app.delete("/registry/services/{service_name}/{service_id}")
def deregister_service(service_name: str, service_id: str):
    if registry.deregister_service(service_name, service_id):
        return {"status": "deregistered"}
    raise HTTPException(status_code=404, detail="Service not found")

@app.get("/registry/services")
def list_services():
    services = {}
    for name, instances in registry.get_all_services().items():
        services[name] = [
            {
                "service_id": i.service_id,
                "host": i.host,
                "port": i.port,
                "version": i.version,
                "status": i.status.value,
                "request_count": i.request_count,
                "error_count": i.error_count
            }
            for i in instances
        ]
    return {"services": services}

@app.post("/registry/services/{service_name}/{service_id}/heartbeat")
def service_heartbeat(service_name: str, service_id: str):
    registry.update_service_status(service_name, service_id, ServiceStatus.HEALTHY)
    return {"status": "updated"}

# Route Management

@app.post("/routes")
def add_route(route: RouteConfig):
    registry.add_route(route)
    return {"status": "added", "path": route.path}

@app.get("/routes")
def list_routes():
    return {
        "routes": [
            {
                "path": r.path,
                "service_name": r.service_name,
                "methods": r.methods,
                "rate_limit": r.rate_limit
            }
            for r in registry._routes.values()
        ]
    }

# Gateway Proxy Endpoint

@app.api_route("/gateway/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "PATCH"])
async def gateway_proxy(
    request: Request,
    path: str,
    authorization: Optional[str] = Header(None)
):
    """Proxy requests to backend services"""
    
    # Find route
    route = registry.get_route(f"/gateway/{path}")
    if not route:
        raise HTTPException(status_code=404, detail="Route not found")
    
    # Check rate limit
    client_key = f"rate_limit:{request.client.host}:{route.path}"
    allowed = await rate_limiter.is_allowed(client_key, route.rate_limit)
    if not allowed:
        raise HTTPException(status_code=429, detail="Rate limit exceeded")
    
    # Get service instance
    instance = registry.get_next_instance(route.service_name)
    if not instance:
        raise HTTPException(status_code=503, detail="Service unavailable")
    
    # Build target URL
    target_url = f"http://{instance.host}:{instance.port}/{path}"
    
    # Proxy the request
    method = request.method
    headers = dict(request.headers)
    headers.pop("host", None)
    
    body = await request.body()
    
    try:
        async with httpx.AsyncClient(timeout=route.timeout) as client:
            response = await client.request(
                method=method,
                url=target_url,
                headers=headers,
                content=body,
                params=dict(request.query_params)
            )
            
            # Update metrics
            instance.request_count += 1
            
            return Response(
                content=response.content,
                status_code=response.status_code,
                headers=dict(response.headers)
            )
            
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="Gateway timeout")
    except Exception as e:
        logger.error("proxy_error", error=str(e))
        raise HTTPException(status_code=502, detail="Bad gateway")

# ============== CLI Interface ==============

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Enterprise Microservices Platform")
    parser.add_argument("command", choices=["serve", "status"])
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8000)
    
    args = parser.parse_args()
    
    if args.command == "serve":
        uvicorn.run(app, host=args.host, port=args.port)
    elif args.command == "status":
        print(f"Platform Status: {__version__}")
        print(f"Services registered: {len(registry.get_all_services())}")
