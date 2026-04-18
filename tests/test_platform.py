"""Tests for Enterprise Microservices Platform"""

import pytest
from fastapi.testclient import TestClient

from services.main import app, registry, ServiceRegistration, RouteConfig, ServiceInstance, ServiceStatus

client = TestClient(app)


class TestHealth:
    def test_health_check(self):
        response = client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        assert data["service_name"] == "api-gateway"

    def test_info(self):
        response = client.get("/")
        assert response.status_code == 200
        assert "Enterprise Microservices Platform" in response.json()["name"]


class TestServiceRegistry:
    def test_register_service(self):
        registration = {
            "name": "test-service",
            "host": "localhost",
            "port": 8080,
            "version": "1.0.0"
        }
        response = client.post("/registry/services", json=registration)
        assert response.status_code == 200
        data = response.json()
        assert "service_id" in data
        assert data["status"] == "registered"

    def test_list_services(self):
        # Register a service first
        client.post("/registry/services", json={
            "name": "user-service",
            "host": "localhost",
            "port": 8081,
            "version": "1.0.0"
        })
        
        response = client.get("/registry/services")
        assert response.status_code == 200
        data = response.json()
        assert "services" in data

    def test_deregister_service(self):
        # Register then deregister
        reg_response = client.post("/registry/services", json={
            "name": "temp-service",
            "host": "localhost",
            "port": 8082,
            "version": "1.0.0"
        })
        service_id = reg_response.json()["service_id"]
        
        response = client.delete(f"/registry/services/temp-service/{service_id}")
        assert response.status_code == 200
        assert response.json()["status"] == "deregistered"


class TestRoutes:
    def test_add_route(self):
        route = {
            "path": "/api/test",
            "service_name": "test-service",
            "rate_limit": 100
        }
        response = client.post("/routes", json=route)
        assert response.status_code == 200
        assert response.json()["path"] == "/api/test"

    def test_list_routes(self):
        response = client.get("/routes")
        assert response.status_code == 200
        assert "routes" in response.json()


class TestCircuitBreaker:
    def test_circuit_breaker_initial_state(self):
        from services.main import CircuitBreaker, CircuitState
        
        cb = CircuitBreaker("test-service")
        assert cb.state == CircuitState.CLOSED
        assert cb.can_execute() is True

    def test_circuit_breaker_opens(self):
        from services.main import CircuitBreaker, CircuitState
        
        cb = CircuitBreaker("test-service")
        for _ in range(10):
            cb.record_failure()
        
        assert cb.state == CircuitState.OPEN
        assert cb.can_execute() is False


class TestRateLimiter:
    @pytest.mark.asyncio
    async def test_rate_limiter_no_redis(self):
        from services.main import rate_limiter
        
        # Should allow when no Redis
        allowed = await rate_limiter.is_allowed("test-key", 100)
        assert allowed is True
