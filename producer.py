import argparse
import asyncio
import httpx
from urllib.parse import urljoin, urlparse
import aio_pika
from bs4 import BeautifulSoup
import sys
import signal
from dotenv import load_dotenv
import os

load_dotenv()

# Переменные для RabbitMQ
RMQ_HOST = os.getenv('RABBITMQ_HOST', 'localhost')
RMQ_PORT = int(os.getenv('RABBITMQ_PORT', 5672))
RMQ_USER = os.getenv('RABBITMQ_USER', 'guest')
RMQ_PASS = os.getenv('RABBITMQ_PASSWORD', 'guest')
RMQ_QUEUE = os.getenv('RABBITMQ_QUEUE', 'urls')

async def get_page(url):
    try:
        async with httpx.AsyncClient() as client:
            res = await client.get(url, follow_redirects=True)
            res.raise_for_status()
            return parse_links_from_page(res.text, url)
    except httpx.RequestError as err:
        print(f"Error fetching {url}: {err}")
        return []

async def parse_links_from_page(page_content, url):
    soup = BeautifulSoup(page_content, 'html.parser')
    base_url = f"{urlparse(url).scheme}://{urlparse(url).netloc}"
    links = []
    
    print(f"Processing: {get_page_title(soup)} ({url})")

    for tag in soup.find_all(['a', 'img', 'video', 'audio']):
        link = tag.get('href') or tag.get('src')
        if link:
            full_url = urljoin(base_url, link)
            if full_url.startswith(base_url):
                links.append(full_url)
                print(f"Found link: {full_url}")
    
    return links

def get_page_title(soup):
    title_tag = soup.find('title')
    return title_tag.text if title_tag else "Untitled"

async def handle_message(channel, body):
    url = body.decode()
    print(f"Handling URL: {url}")
    links = await get_page(url)

    if links:
        for link in links:
            await push_to_queue(channel, link)
            print(f"Sent: {link}")
    else:
        print(f"No links found for {url}")

async def push_to_queue(channel, link):
    await channel.default_exchange.publish(
        aio_pika.Message(body=link.encode()),
        routing_key=RMQ_QUEUE
    )

async def start_consuming():
    connection = await aio_pika.connect_robust(
        f"amqp://{RMQ_USER}:{RMQ_PASS}@{RMQ_HOST}:{RMQ_PORT}/"
    )
    channel = await connection.channel()
    queue = await channel.declare_queue(RMQ_QUEUE, durable=True)

    async with queue.iterator() as iter:
        async for message in iter:
            async with message.process():
                await handle_message(channel, message.body)

def graceful_shutdown(sig, frame):
    print("Shutting down gracefully...")
    sys.exit(0)

signal.signal(signal.SIGINT, graceful_shutdown)

if __name__ == "__main__":
    asyncio.run(start_consuming())
