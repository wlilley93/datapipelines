# Getting Full Transcript Content in Fireflies Connector

## Current Issue
The transcripts list GraphQL query doesn't include the full transcript content field, causing a 400 Bad Request error when trying to add the `transcript` field directly to the query.

## Solution Approach
The Fireflies API likely separates the transcript listing from detailed transcript content retrieval for performance reasons. To get the full transcript content, we need to implement a two-stage approach:

1. Use the existing list endpoint to get transcript metadata
2. Make individual calls to get detailed transcript content

## Implementation Steps

### 1. Create a detailed transcript query
```graphql
query TranscriptDetail($id: String!) {
  transcript(id: $id) {
    id
    title
    date
    transcript  # This field likely contains the full transcript
    # other fields as needed
  }
}
```

### 2. Modify the streaming logic
Instead of adding transcript content to the list query, implement a separate resource that fetches detailed transcript content for each transcript ID:

```python
@dlt.resource(name="fireflies_transcript_details", write_disposition="merge", primary_key="id")
def transcript_details():
    # For each transcript ID from the list, fetch detailed content
    for transcript_bundle in transcripts_raw():
        transcript_id = transcript_bundle['transcript']['id']
        # Execute detailed query for this transcript
        # Yield the detailed transcript data
        yield detailed_transcript_data
```

### 3. Combine the data
The detailed transcripts can be merged with the base transcript data using the ID as the primary key.

## Note
This approach will require more API calls and may take longer to sync, but it will provide the full transcript content. Rate limiting will be important to consider with this approach.