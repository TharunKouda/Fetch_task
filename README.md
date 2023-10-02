## Reading and Processing Data from AWS SQS Queue

This project is designed to fetch messages from an AWS Simple Queue Service (SQS) queue, process them, and insert them into a PostgreSQL database. Here's a step-by-step guide to how this works:

### 1. Dependencies:

We use several Python libraries. To install them, use:

pip install awscli-local boto3 psycopg2


### 2.Fetching Messages from SQS:

Using the `boto3` library, we establish a connection to our SQS queue. We use `awscli-local` to mock AWS services on our local machine:

sqs = boto3.client(
    'sqs',
    region_name='us-east-1', 
    endpoint_url='http://localhost:4566',
    aws_access_key_id='dummy_access_key', 
    aws_secret_access_key='dummy_secret_key'
)

The function `fetch_messages` retrieves up to 10 messages at once from the queue:

messages = fetch_messages(QUEUE_URL)


### 3. Processing Messages:

Each message from SQS has metadata. We're interested in the `Body`, which contains our actual data as a JSON string:

bodies = [json.loads(message['Body']) for message in messages]


To handle nested JSON structures, we have the `flatten_json` function that transforms nested keys into a flat structure:

flattened_bodies = [flatten_json(body) for body in bodies]


### 4. Data Masking:

For privacy reasons, we hash the `device_id` and `ip` fields of our data:

for body in flattened_bodies:
    body['device_id'] = mask_data(body['device_id'])
    body['ip'] = mask_data(body['ip'])


### 5. Storing Data in PostgreSQL:

We connect to a local PostgreSQL instance:

connection = psycopg2.connect(
    dbname="postgres",
    user="postgres",
    password="Tharun@2403",
    host="localhost",
    port="5432"
)


The function `insert_into_db` takes the processed data and inserts it into the `user_logins` table:

for body in flattened_bodies:
    insert_into_db(body, cursor)

Once all the data has been inserted, we commit the changes and close the database connection:

connection.commit()
cursor.close()
connection.close()

---
<img width="930" alt="image" src="https://github.com/TharunKouda/Fetch_task/assets/114703721/f8800338-72ea-4c75-84d3-c7db28a0d48d">


For loading the data to sink I used the local postGre data base as I found the connection errors while giving to the Postgres containers.

Here are potential reasons for not being able to connect to the PostgreSQL image:

Network Configuration: Docker containers have their own network by default. If the PostgreSQL container is not on the same network as other services trying to connect, it might cause connection issues.
Using "localhost" when trying to connect from the host machine might not work unless the container's ports are correctly mapped to the host.
Authentication Issues: The error message "password authentication failed for user 'postgres'" indicates a mismatch between the password provided and the one set for the PostgreSQL container. There might have been discrepancies in environment variables or configurations.
Firewall or Security Software: Security software on your machine, or built-in firewalls, might be blocking connections to Docker containers, especially if non-default ports are used.
Docker Daemon Issues: Sometimes, the Docker daemon might have issues or bugs that prevent proper networking or port forwarding. Restarting Docker can sometimes resolve these kinds of issues.







You can include this breakdown in your GitHub repository's README or in a separate documentation file to help users and collaborators understand how your code functions.
