# Fireflies Connector Performance Improvements

## Changes Made

### 1. Adaptive Request Pacing
- Implemented dynamic delay adjustment based on response times
- Automatically increases delay when consecutive slow responses are detected (>5s)
- Gradually decreases delay when responses return to normal
- Initial delay increased gradually from baseline when slow responses occur

### 2. Adaptive Page Size
- Dynamically adjusts page size (number of records per request) based on performance
- Reduces page size when experiencing slow responses to reduce load
- Gradually increases page size when API responds quickly
- Minimum page size maintained at 5 to ensure reasonable throughput

### 3. Enhanced Logging
- Added detailed logging for slow and very slow responses
- Included page size and delay information in logs for debugging
- Added attempt number tracking for retry scenarios
- Added error logging for responses taking over 10 seconds

### 4. Consecutive Slow Response Tracking
- Implemented counter for consecutive slow responses
- Triggers pacing adjustments after 3 consecutive slow responses
- Tracks both pacing and page size adjustments separately

## Expected Performance Improvements

1. **Reduced Timeout Errors**: Adaptive pacing should reduce the likelihood of requests timing out
2. **Better API Responsiveness**: Dynamic page size adjustment should help maintain optimal throughput
3. **Improved Resource Usage**: Automatic pacing adjustments prevent overwhelming the API
4. **Better Monitoring**: Enhanced logging provides visibility into performance bottlenecks

## Configuration Parameters

- `BASE_PAGE_DELAY_SECONDS`: Initial delay between requests (0.1s)
- `MAX_PAGE_DELAY_SECONDS`: Maximum delay between requests (5.0s)
- `DEFAULT_PAGE_SIZE`: Default number of records per request (50)
- `MIN_PAGE_SIZE`: Minimum number of records per request (5)
- `slow_response_threshold`: Threshold for slow response tracking (5000ms)

## How It Works

1. Each GraphQL request is timed
2. If response time exceeds 5000ms, it's counted as a slow response
3. After 3 consecutive slow responses, the delay between requests is increased by 50%
4. Every 5th slow response triggers a page size reduction (by 25%)
5. When responses are fast for 5 consecutive requests, page size is increased (by 25% up to default)
6. Delay is gradually reduced when responses remain fast