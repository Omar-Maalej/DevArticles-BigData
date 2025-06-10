from kafka import KafkaProducer
import json
import time
import random

producer = KafkaProducer(
    bootstrap_servers='localhost:9093',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Some sample words
words = ['python', 'dev', 'spark', 'kafka', 'stream', 'ai', 'data', 'flask', 'fastapi', 'topic']

print("🚀 Sending fake word counts to 'top_words' topic...")

try:
    while True:
        word = random.choice(words)
        count = random.randint(1, 500)

        message = {
            'word': word,
            'count': count
        }

        print("🟢 Sent:", message)
        producer.send('top_words', key=word.encode('utf-8'), value=message)
        time.sleep(2)  # adjust to simulate real-time pace
except KeyboardInterrupt:
    print("🛑 Stopped.")
