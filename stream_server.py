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

# Kafka consumers
tag_consumer = KafkaConsumer(
    'tag_counts',
    bootstrap_servers=['localhost:9093'],
    auto_offset_reset='latest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

lang_consumer = KafkaConsumer(
    'language_distribution',
    bootstrap_servers=['localhost:9093'],
    auto_offset_reset='latest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Global state
tag_data = defaultdict(int)
language_data = defaultdict(int)
update_interval = 2  # seconds

@app.get("/")
async def get():
    return HTMLResponse("""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Real-time Analytics</title>
        <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
        <style>
            body { font-family: Arial; margin: 20px; }
            .container { display: flex; gap: 40px; flex-wrap: wrap; }
            .chart-box { width: 45%; }
        </style>
    </head>
    <body>
        <h1>Real-time Tag and Language Analytics</h1>
        <div class="container">
            <div class="chart-box">
                <h2>Tag Counts</h2>
                <canvas id="tagChart"></canvas>
            </div>
            <div class="chart-box">
                <h2>Language Distribution</h2>
                <canvas id="langChart"></canvas>
            </div>
        </div>

        <script>
            const tagChart = new Chart(document.getElementById('tagChart'), {
                type: 'bar',
                data: { labels: [], datasets: [{ label: 'Tags', data: [] }] },
                options: { scales: { y: { beginAtZero: true } }, responsive: true }
            });

            const langChart = new Chart(document.getElementById('langChart'), {
                type: 'pie',
                data: { labels: [], datasets: [{ label: 'Languages', data: [] }] },
                options: { responsive: true }
            });

            const ws = new WebSocket(`ws://${window.location.host}/ws/data`);
            ws.onmessage = (event) => {
                const data = JSON.parse(event.data);

                tagChart.data.labels = data.tags.labels;
                tagChart.data.datasets[0].data = data.tags.values;
                tagChart.update();

                langChart.data.labels = data.languages.labels;
                langChart.data.datasets[0].data = data.languages.values;
                langChart.update();
            };
        </script>
    </body>
    </html>
    """)

@app.websocket("/ws/data")
async def data_websocket(websocket: WebSocket):
    await websocket.accept()
    try:
        while True:
            tags_sorted = sorted(tag_data.items(), key=lambda x: x[1], reverse=True)[:10]
            tag_labels = [t[0] for t in tags_sorted]
            tag_values = [t[1] for t in tags_sorted]

            lang_sorted = sorted(language_data.items(), key=lambda x: x[1], reverse=True)
            lang_labels = [l[0] for l in lang_sorted]
            lang_values = [l[1] for l in lang_sorted]

            await websocket.send_json({
                "tags": {"labels": tag_labels, "values": tag_values},
                "languages": {"labels": lang_labels, "values": lang_values},
                "timestamp": time.time()
            })
            await asyncio.sleep(update_interval)
    except WebSocketDisconnect:
        print("WebSocket disconnected")

# Consumer threads
def consume_tags():
    for msg in tag_consumer:
        tag_data[msg.value['tag']] = msg.value['count']

def consume_languages():
    for msg in lang_consumer:
        language_data[msg.value['language']] = msg.value['count']

@app.on_event("startup")
async def start_consumers():
    threading.Thread(target=consume_tags, daemon=True).start()
    threading.Thread(target=consume_languages, daemon=True).start()
