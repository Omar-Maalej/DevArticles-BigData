from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse
from kafka import KafkaConsumer
import json
import asyncio
import threading
from collections import defaultdict
import time

app = FastAPI()

# Mount static files (for JS libraries)
app.mount("/static", StaticFiles(directory="static"), name="static")

# Kafka consumer setup
consumer = KafkaConsumer(
    'tag_counts',
    bootstrap_servers=['localhost:9093'],
    auto_offset_reset='latest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Global variables for storing tag data
tag_data = defaultdict(int)
last_update_time = 0
update_interval = 2  # seconds

# HTML template with visualization
html = """
<!DOCTYPE html>
<html>
<head>
    <title>Real-time Tag Counts</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <script src="/static/websocket.js"></script>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; }
        .container { display: flex; }
        .chart-container { width: 70%; }
        .sidebar { width: 30%; padding-left: 20px; }
        .card { background: #f5f5f5; padding: 15px; margin-bottom: 15px; border-radius: 5px; }
        h1 { color: #333; }
        #update-time { color: #666; font-size: 0.9em; }
    </style>
</head>
<body>
    <h1>Real-time Tag Counts Visualization</h1>
    <p id="update-time">Last updated: Not yet received data</p>
    
    <div class="container">
        <div class="chart-container">
            <canvas id="tagChart"></canvas>
        </div>
        <div class="sidebar">
            <div class="card">
                <h3>Top Tags</h3>
                <div id="topTags"></div>
            </div>
            <div class="card">
                <h3>Statistics</h3>
                <div id="stats">
                    <p>Total tags tracked: <span id="totalTags">0</span></p>
                    <p>Total occurrences: <span id="totalOccurrences">0</span></p>
                </div>
            </div>
        </div>
    </div>

    <script>
        // Initialize chart
        const ctx = document.getElementById('tagChart').getContext('2d');
        const tagChart = new Chart(ctx, {
            type: 'bar',
            data: {
                labels: [],
                datasets: [{
                    label: 'Tag Counts',
                    data: [],
                    backgroundColor: 'rgba(54, 162, 235, 0.5)',
                    borderColor: 'rgba(54, 162, 235, 1)',
                    borderWidth: 1
                }]
            },
            options: {
                responsive: true,
                scales: {
                    y: {
                        beginAtZero: true
                    }
                },
                plugins: {
                    legend: {
                        display: false
                    },
                    title: {
                        display: true,
                        text: 'Tag Frequency Counts',
                        font: {
                            size: 16
                        }
                    }
                }
            }
        });

        // WebSocket connection
        const ws = new WebSocket(`ws://${window.location.host}/ws`);
        
        ws.onmessage = function(event) {
            const data = JSON.parse(event.data);
            document.getElementById('update-time').textContent = `Last updated: ${new Date().toLocaleTimeString()}`;
            
            // Update chart
            tagChart.data.labels = data.labels;
            tagChart.data.datasets[0].data = data.values;
            tagChart.update();
            
            // Update top tags list
            const topTagsDiv = document.getElementById('topTags');
            topTagsDiv.innerHTML = '';
            data.top_tags.forEach(tag => {
                const tagElement = document.createElement('p');
                tagElement.innerHTML = `<strong>${tag.tag}</strong>: ${tag.count}`;
                topTagsDiv.appendChild(tagElement);
            });
            
            // Update stats
            document.getElementById('totalTags').textContent = data.total_tags;
            document.getElementById('totalOccurrences').textContent = data.total_occurrences;
        };
    </script>
</body>
</html>
"""

@app.get("/")
async def get():
    return HTMLResponse(html)

# WebSocket endpoint
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    try:
        while True:
            # Send current tag data to client
            labels = list(tag_data.keys())
            values = list(tag_data.values())
            
            # Prepare top tags (sorted by count)
            sorted_tags = sorted(tag_data.items(), key=lambda x: x[1], reverse=True)[:10]
            top_tags = [{"tag": tag, "count": count} for tag, count in sorted_tags]
            
            data = {
                "labels": labels,
                "values": values,
                "top_tags": top_tags,
                "total_tags": len(tag_data),
                "total_occurrences": sum(tag_data.values()),
                "timestamp": time.time()
            }
            
            await websocket.send_json(data)
            await asyncio.sleep(update_interval)  # Update every 2 seconds
            
    except WebSocketDisconnect:
        print("Client disconnected")

# Kafka consumer thread
def consume_kafka_messages():
    global tag_data, last_update_time
    for message in consumer:
        tag = message.value['tag']
        count = message.value['count']
        tag_data[tag] = count
        last_update_time = time.time()

# Start Kafka consumer in a separate thread
def start_kafka_consumer():
    threading.Thread(target=consume_kafka_messages, daemon=True).start()

# Start the consumer when the app starts up
@app.on_event("startup")
async def startup_event():
    start_kafka_consumer()