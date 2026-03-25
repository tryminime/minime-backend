"""
Load Tests for Graph API
Tests 100 RPS with 50K nodes
Uses locust for load testing
"""

from locust import HttpUser, task, between, events
import random
import json
from datetime import datetime


class GraphAPIUser(HttpUser):
    """Simulated user for Graph API load testing."""
    
    wait_time = between(0.5, 2.0)  # Wait 0.5-2 seconds between requests
    
    def on_start(self):
        """Setup - login and get auth token."""
        # Mock authentication
        self.token = "test-token-123"
        self.headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }
        
        # Sample node IDs for testing (would be populated from actual data)
        self.node_ids = list(range(1, 1001))  # 1000 sample nodes
        self.topic_ids = list(range(1, 101))  # 100 sample topics
    
    @task(10)
    def get_node_details(self):
        """Test GET /nodes/{id} endpoint."""
        node_id = random.choice(self.node_ids)
        
        with self.client.get(
            f"/api/v1/graph/nodes/{node_id}",
            headers=self.headers,
            catch_response=True,
            name="/api/v1/graph/nodes/[id]"
        ) as response:
            if response.status_code == 200:
                response.success()
            elif response.status_code == 404:
                response.success()  # Expected for non-existent nodes
            else:
                response.failure(f"Got status {response.status_code}")
    
    @task(5)
    def get_node_neighbors(self):
        """Test GET /nodes/{id}/neighbors endpoint."""
        node_id = random.choice(self.node_ids)
        
        with self.client.get(
            f"/api/v1/graph/nodes/{node_id}/neighbors",
            headers=self.headers,
            catch_response=True,
            name="/api/v1/graph/nodes/[id]/neighbors"
        ) as response:
            if response.status_code in [200, 404]:
                response.success()
            else:
                response.failure(f"Got status {response.status_code}")
    
    @task(8)
    def get_experts(self):
        """Test GET /experts endpoint."""
        params = {
            "page": random.randint(1, 10),
            "page_size": random.choice([10, 20, 50])
        }
        
        with self.client.get(
            "/api/v1/graph/experts",
            params=params,
            headers=self.headers,
            catch_response=True,
            name="/api/v1/graph/experts"
        ) as response:
            if response.status_code == 200:
                response.success()
            else:
                response.failure(f"Got status {response.status_code}")
    
    @task(6)
    def get_topic_experts(self):
        """Test GET /experts?topic_id=... endpoint."""
        topic_id = random.choice(self.topic_ids)
        params = {
            "topic_id": topic_id,
            "page_size": 20
        }
        
        with self.client.get(
            "/api/v1/graph/experts",
            params=params,
            headers=self.headers,
            catch_response=True,
            name="/api/v1/graph/experts?topic_id=[id]"
        ) as response:
            if response.status_code in [200, 404]:
                response.success()
            else:
                response.failure(f"Got status {response.status_code}")
    
    @task(4)
    def get_collaborator_recommendations(self):
        """Test GET /collaborators/recommend endpoint."""
        node_id = random.choice(self.node_ids)
        params = {
            "for_node_id": node_id,
            "top_k": 10
        }
        
        with self.client.get(
            "/api/v1/graph/collaborators/recommend",
            params=params,
            headers=self.headers,
            catch_response=True,
            name="/api/v1/graph/collaborators/recommend"
        ) as response:
            if response.status_code in [200, 400, 404]:
                response.success()
            else:
                response.failure(f"Got status {response.status_code}")
    
    @task(3)
    def get_communities(self):
        """Test GET /communities endpoint."""
        params = {
            "min_size": random.choice([1, 5, 10]),
            "page": random.randint(1, 5),
            "page_size": 20
        }
        
        with self.client.get(
            "/api/v1/graph/communities",
            params=params,
            headers=self.headers,
            catch_response=True,
            name="/api/v1/graph/communities"
        ) as response:
            if response.status_code == 200:
                response.success()
            else:
                response.failure(f"Got status {response.status_code}")
    
    @task(5)
    def search_embeddings(self):
        """Test POST /embeddings/search endpoint."""
        node_id = random.choice(self.node_ids)
        payload = {
            "node_id": node_id,
            "top_k": 10,
            "min_similarity": 0.7
        }
        
        with self.client.post(
            "/api/v1/graph/embeddings/search",
            json=payload,
            headers=self.headers,
            catch_response=True,
            name="/api/v1/graph/embeddings/search"
        ) as response:
            if response.status_code in [200, 400, 404]:
                response.success()
            else:
                response.failure(f"Got status {response.status_code}")
    
    @task(2)
    def get_learning_paths(self):
        """Test GET /learning-paths endpoint."""
        source = random.choice(self.topic_ids)
        target = random.choice(self.topic_ids)
        params = {
            "source_topic_id": source,
            "target_topic_id": target,
            "max_depth": 5
        }
        
        with self.client.get(
            "/api/v1/graph/learning-paths",
            params=params,
            headers=self.headers,
            catch_response=True,
            name="/api/v1/graph/learning-paths"
        ) as response:
            if response.status_code in [200, 404]:
                response.success()
            else:
                response.failure(f"Got status {response.status_code}")
    
    @task(1)
    def export_graph(self):
        """Test GET /export endpoint (lower frequency due to cost)."""
        params = {
            "limit": random.choice([10, 50, 100]),
            "node_types": random.choice([
                ["PERSON"],
                ["PAPER"],
                ["PERSON", "PAPER"],
                None
            ])
        }
        
        with self.client.get(
            "/api/v1/graph/export",
            params=params,
            headers=self.headers,
            catch_response=True,
            name="/api/v1/graph/export"
        ) as response:
            if response.status_code == 200:
                response.success()
            else:
                response.failure(f"Got status {response.status_code}")


# Event handlers for custom metrics
@events.test_start.add_listener
def on_test_start(environment, **kwargs):
    """Called when test starts."""
    print("=" * 60)
    print("GRAPH API LOAD TEST")
    print("=" * 60)
    print(f"Target: 100 RPS")
    print(f"Graph Size: 50K nodes")
    print(f"Start Time: {datetime.now()}")
    print("=" * 60)


@events.test_stop.add_listener
def on_test_stop(environment, **kwargs):
    """Called when test stops."""
    print("=" * 60)
    print("LOAD TEST COMPLETE")
    print(f"End Time: {datetime.now()}")
    print("=" * 60)
    
    # Print summary statistics
    stats = environment.stats
    print(f"\nTotal Requests: {stats.total.num_requests}")
    print(f"Total Failures: {stats.total.num_failures}")
    print(f"Average Response Time: {stats.total.avg_response_time:.2f}ms")
    print(f"Median Response Time: {stats.total.median_response_time:.2f}ms")
    print(f"95th Percentile: {stats.total.get_response_time_percentile(0.95):.2f}ms")
    print(f"99th Percentile: {stats.total.get_response_time_percentile(0.99):.2f}ms")
    print(f"Requests/sec: {stats.total.total_rps:.2f}")
    
    # Check if we met targets
    print("\n" + "=" * 60)
    print("TARGET VALIDATION")
    print("=" * 60)
    
    target_rps = 100
    target_latency_p95 = 500  # 500ms
    
    actual_rps = stats.total.total_rps
    actual_p95 = stats.total.get_response_time_percentile(0.95)
    
    print(f"RPS: {actual_rps:.2f} / {target_rps} {'✅' if actual_rps >= target_rps else '❌'}")
    print(f"P95 Latency: {actual_p95:.2f}ms / {target_latency_p95}ms {'✅' if actual_p95 <= target_latency_p95 else '❌'}")
    
    if stats.total.num_failures > 0:
        failure_rate = (stats.total.num_failures / stats.total.num_requests) * 100
        print(f"Failure Rate: {failure_rate:.2f}% {'❌' if failure_rate > 1 else '✅'}")


"""
USAGE:

1. Install locust:
   pip install locust

2. Run load test:
   locust -f backend/tests/test_load.py --host=http://localhost:8000 --users=100 --spawn-rate=10 --run-time=5m

   Parameters:
   --users=100         : 100 concurrent users
   --spawn-rate=10     : Spawn 10 users per second
   --run-time=5m       : Run for 5 minutes
   --headless          : Run without web UI
   --csv=results       : Save results to CSV

3. View results:
   - Web UI: http://localhost:8089
   - CSV files: results_stats.csv, results_failures.csv

4. Target validation:
   - 100 RPS sustained
   - P95 latency < 500ms
   - Failure rate < 1%
   - 50K nodes in graph
"""
