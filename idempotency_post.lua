-- wrk load-testing script
-- Thread-safe generator using time, CPU clock, and counters to guarantee unique keys

wrk.method = "POST"
wrk.headers["Content-Type"] = "application/json"

-- Seed the random number generator using a combination of time and CPU clock precision
math.randomseed(os.time() + math.floor(os.clock() * 1000000))

-- Helper function to generate a random alphanumeric string
local function random_string(length)
    local chars = "abcdefghijklmnopqrstuvwxyz0123456789"
    local res = ""
    for i = 1, length do
        local r = math.random(1, #chars)
        res = res .. string.sub(chars, r, r)
    end
    return res
end

-- Thread-local request counter
local request_counter = 0

request = function()
    request_counter = request_counter + 1

    -- 1. Create a guaranteed unique X-Idempotency-Key
    -- Combines time, high-res clock, thread counter, and a random string
    local unique_key = string.format(
        "wrk-key-%d-%d-%d-%s",
        os.time(),
        math.floor(os.clock() * 1000000),
        request_counter,
        random_string(6)
    )
    wrk.headers["X-Idempotency-Key"] = unique_key

    -- 2. Generate a random username and email
    local rand_name = random_string(8) .. tostring(request_counter)
    local body = string.format(
        '{"username": "%s", "email": "%s@example.com", "password": "SecurePassword123"}',
        rand_name,
        rand_name
    )

    return wrk.format(nil, nil, nil, body)
end
