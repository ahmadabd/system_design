from locust import HttpUser, task, between

class APIUser(HttpUser):
    wait_time = between(1, 2)

    @task
    def test_root(self):
        self.client.get("/")

    @task(1)
    def check_health(self):
        self.client.get("/health")

    @task(3)
    def trace_demo(self):
        self.client.get("/trace-demo")
