from locust import HttpUser, task, between

class ECommerceUser(HttpUser):
    wait_time = between(0.5, 2.0)

    @task(2)
    def check_user_health(self):
        """Monitor user service health endpoint."""
        self.client.get("/users/health")

    @task(2)
    def check_product_health(self):
        """Monitor product service health endpoint."""
        self.client.get("/products/health")

    @task(2)
    def check_order_health(self):
        """Monitor order service health endpoint."""
        self.client.get("/orders/health")

    @task(4)
    def browse_catalog(self):
        """Simulate a user browsing the product catalog."""
        self.client.get("/products")

