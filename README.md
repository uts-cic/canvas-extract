# canvas-extract
Canvas extract discussions, groups 

## Node 8.10 Lambda function

This uses serverless to deploy lambda function to AWS and has all the environment variables set via serverless.yml. If you prefer thsi method create a new file serverless.yml
alternavtively this can be set via the .env file


Following evn variables need to be set via .env or via serverless.yml
```
CANVAS_API_HOST:
CANVAS_ACCESS_TOKEN:
CANVAS_FETCH: "discussion_topics"
LRS_URL: 
LRS_USERNAME:
LRS_PASSWORD:
DEFAULT_LRS_ACTOR_NAME: 
DEFAULT_LRS_ACTOR_EMAIL: 
```
