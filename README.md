## Locast
>> ./.venv/bin/locust --headless -u 10 -r 2 --run-time 1m --host http://localhost
>> ./.venv/bin/locust

## wrk
>> wrk -t20 -c1000 -d30s http://172.28.0.100/trace-demo