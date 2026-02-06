# Fireflies Connector Directory Structure

## Files

```
__init__.py
actions.py
connector.py
constants.py
dedupe.py
errors.py
events.py
gql.py
participants.py
performance_improvements.md
pipeline.py
schema.py
selection.py
state.py
streaming.py
time_utils.py
utils_bridge.py
```

## File Descriptions

- `__init__.py`: Python package initialization file
- `actions.py`: Contains action-related functionality for the Fireflies connector
- `connector.py`: Main connector class implementing the standard Connector protocol
- `constants.py`: Configuration constants including API endpoints and default values
- `dedupe.py`: Deduplication logic for transcripts and other entities
- `errors.py`: Custom error definitions and handling
- `events.py`: Event logging and tracking functionality
- `gql.py`: GraphQL client with pacing and rate limit backoff logic (includes recent performance improvements)
- `participants.py`: Logic for extracting participant information
- `performance_improvements.md`: Documentation of recent performance improvements made to address slow GraphQL requests
- `pipeline.py`: Main pipeline execution logic for syncing Fireflies data
- `schema.py`: Schema-related functionality
- `selection.py`: Logic for handling stream selection
- `state.py`: State management functionality
- `streaming.py`: Implementation of transcript streaming with pagination
- `time_utils.py`: Time-related utility functions
- `utils_bridge.py`: Utility bridge functions including HTTP session management

## Notable Recent Changes

The `gql.py` file has been enhanced with adaptive pacing and page size reduction to address the reported issue of GraphQL requests taking 2+ minutes. These changes include:

1. Dynamic delay adjustment based on response times
2. Adaptive page size reduction when experiencing slow responses
3. Enhanced logging for performance monitoring
4. Consecutive slow response tracking