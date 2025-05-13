# Setting Up PostgreSQL and Python API on Ubuntu with Docker

## Step 1: Install PostgreSQL on Ubuntu

```sh
sudo apt update && sudo apt upgrade -y
sudo apt install -y postgresql postgresql-contrib
sudo systemctl enable postgresql
sudo systemctl start postgresql
psql --version
```

## Step 2: Secure PostgreSQL

```sh
sudo -i -u postgres
psql
```

```sql
ALTER USER postgres PASSWORD 'Mahmoud1551997*+#@';
\q
exit
```

## Step 3: Allow Remote Connections (Optional)

Edit PostgreSQL configuration:

```sh
sudo nano /etc/postgresql/*/main/postgresql.conf
```

Modify the following line:

```ini
listen_addresses = '*'
```

Edit authentication settings:

```sh
sudo nano /etc/postgresql/*/main/pg_hba.conf
```

Add the following line:

```ini
host    all             all             0.0.0.0/0               md5
```


Look for these lines:

```ini
local   all             postgres                                peer
local   all             all                                     peer
```

Change peer to md5:

```ini
local   all             postgres                                md5
local   all             all                                     md5
```

Restart PostgreSQL:

```sh
sudo systemctl restart postgresql
```

## Step 4: Verify Connection

```sh
sudo systemctl status postgresql
psql -U postgres
psql -U postgres -W

```

```sh
sudo journalctl -xeu postgresql
```
---

# Using Docker and Docker Compose for PostgreSQL

## Step 1: Install Docker and Docker Compose

```sh
sudo apt update && sudo apt upgrade -y
sudo apt install -y docker.io
sudo systemctl enable --now docker
```

Install Docker Compose:

```sh
sudo curl -L "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose
```

Verify installations:

```sh
docker --version
docker-compose --version
```

## Step 2: Start PostgreSQL Container

```sh
docker-compose down -v
docker volume prune -f
```

```sh
docker-compose up -d
docker ps
docker logs postgres_container
```

## Step 3: Connect to PostgreSQL Inside Docker

```sh
docker exec -it postgres_container psql -U video_user -d video_db
psql -h localhost -U video_user -d video_db
```

## Step 4: Create `video_stream` Table

```sql
CREATE TABLE video_stream (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL,
    name TEXT NOT NULL,
    path TEXT NOT NULL,
    type TEXT NOT NULL
);
\q
```

## Step 5: Create `users` Table

```sql
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    username VARCHAR(50) UNIQUE NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    password TEXT NOT NULL
);
\q
```


---

# Setting Up Python API with Flask and PostgreSQL

## Step 1: Install Python and Dependencies

```sh
sudo apt install -y python3 python3-pip
pip3 install psycopg2-binary flask
```

## Step 2: Implement Python API

```python
from flask import Flask, request, jsonify
import psycopg2
import uuid

app = Flask(__name__)

# Database connection
def get_db_connection():
    return psycopg2.connect(
        dbname="video_db",
        user="video_user",
        password="Mahmoud1551997*+#@",
        host="localhost",
        port="5432"
    )

# Create a new stream entry
@app.route('/stream', methods=['POST'])
def create_stream():
    data = request.json
    conn = get_db_connection()
    cur = conn.cursor()
    stream_id = str(uuid.uuid4())

    cur.execute(
        "INSERT INTO video_stream (id, user_id, name, path, type) VALUES (%s, %s, %s, %s, %s)",
        (stream_id, data['user_id'], data['name'], data['path'], data['type'])
    )

    conn.commit()
    cur.close()
    conn.close()
    return jsonify({"message": "Stream created", "id": stream_id}), 201

# Get all streams
@app.route('/stream', methods=['GET'])
def get_all_streams():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT id, user_id, name, path, type FROM video_stream")
    streams = cur.fetchall()
    cur.close()
    conn.close()

    return jsonify([
        {"id": s[0], "user_id": s[1], "name": s[2], "path": s[3], "type": s[4]}
        for s in streams
    ])

# Update multiple streams
@app.route('/stream', methods=['PUT'])
def update_streams():
    data = request.json
    conn = get_db_connection()
    cur = conn.cursor()

    for stream in data:
        cur.execute(
            "UPDATE video_stream SET name=%s, path=%s, type=%s WHERE id=%s",
            (stream["name"], stream["path"], stream["type"], stream["id"])
        )

    conn.commit()
    cur.close()
    conn.close()
    return jsonify({"message": "Streams updated"}), 200

# Delete multiple streams
@app.route('/stream', methods=['DELETE'])
def delete_streams():
    data = request.json
    conn = get_db_connection()
    cur = conn.cursor()

    for stream_id in data["ids"]:
        cur.execute("DELETE FROM video_stream WHERE id=%s", (stream_id,))

    conn.commit()
    cur.close()
    conn.close()
    return jsonify({"message": "Streams deleted"}), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000)
```

## Step 3: Run the Flask API

```sh
python3 app.py
```

## Step 4: Test API with cURL or Postman

Create a new stream:

```sh
curl -X POST http://16.170.216.227/stream -H "Content-Type: application/json" -d '{
    "name": "Test Video",
    "path": "/videos/test.mp4",
    "type": "mp4"
}'
```

Retrieve all streams:

```sh
curl -X GET http://16.170.216.227/stream
```

Update a stream:

```sh
curl -X PUT http://16.170.216.227/stream -H "Content-Type: application/json" -d '[
    {
        "id": "615101ab-a700-4ea3-9e02-bcf0f1e35a45",
        "name": "Updated Video",
        "path": "/videos/updated.mp4",
        "type": "mp4"
    }
]'
```

Delete a stream:

```sh
curl -X DELETE http://16.170.216.227/stream -H "Content-Type: application/json" -d '{
    "ids": ["615101ab-a700-4ea3-9e02-bcf0f1e35a45"]
}'
```

