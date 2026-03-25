"""
Tests for authentication endpoints.
"""

import pytest
from fastapi.testclient import TestClient


def test_register_success(client: TestClient, mock_user_data):
    """Test successful user registration."""
    response = client.post("/api/v1/auth/register", json=mock_user_data)
    
    assert response.status_code == 201
    data = response.json()
    assert "access_token" in data
    assert "refresh_token" in data
    assert data["token_type"] == "bearer"


def test_register_weak_password(client: TestClient):
    """Test registration with weak password fails."""
    weak_password_data = {
        "email": "test@example.com",
        "password": "weak"
    }
    
    response = client.post("/api/v1/auth/register", json=weak_password_data)
    
    assert response.status_code == 400
    assert "password" in response.json()["detail"].lower()


def test_login_success(client: TestClient, mock_user_data):
    """Test successful login."""
    # Note: In real tests, you'd first register the user
    response = client.post("/api/v1/auth/login", json={
        "email": mock_user_data["email"],
        "password": mock_user_data["password"]
    })
    
    assert response.status_code == 200
    data = response.json()
    assert "access_token" in data
    assert "refresh_token" in data


def test_get_current_user(client: TestClient, mock_user_data):
    """Test getting current user profile."""
    # First login to get token
    login_response = client.post("/api/v1/auth/login", json={
        "email": mock_user_data["email"],
        "password": mock_user_data["password"]
    })
    
    access_token = login_response.json()["access_token"]
    
    # Get user profile
    response = client.get(
        "/api/v1/auth/me",
        headers={"Authorization": f"Bearer {access_token}"}
    )
    
    assert response.status_code == 200
    data = response.json()
    assert "id" in data
    assert "email" in data


def test_refresh_token(client: TestClient, mock_user_data):
    """Test refreshing access token."""
    # First login to get refresh token
    login_response = client.post("/api/v1/auth/login", json={
        "email": mock_user_data["email"],
        "password": mock_user_data["password"]
    })
    
    refresh_token = login_response.json()["refresh_token"]
    
    # Refresh token
    response = client.post("/api/v1/auth/refresh", json={
        "refresh_token": refresh_token
    })
    
    assert response.status_code == 200
    data = response.json()
    assert "access_token" in data
    assert "refresh_token" in data


def test_logout(client: TestClient, mock_user_data):
    """Test logout."""
    # First login to get token
    login_response = client.post("/api/v1/auth/login", json={
        "email": mock_user_data["email"],
        "password": mock_user_data["password"]
    })
    
    access_token = login_response.json()["access_token"]
    
    # Logout
    response = client.post(
        "/api/v1/auth/logout",
        headers={"Authorization": f"Bearer {access_token}"}
    )
    
    assert response.status_code == 200
    assert "message" in response.json()
