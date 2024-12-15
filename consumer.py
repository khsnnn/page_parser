import argparse
import httpx
from urllib.parse import urljoin, urlparse
import pika
from bs4 import BeautifulSoup
import sys
from dotenv import load_dotenv
import os

load_dotenv()

# Переменные окружения для RabbitMQ
RMQ_HOST = os.getenv('RABBITMQ_HOST', 'localhost')
RMQ_PORT = int(os.getenv('RABBITMQ_PORT', 5672))
RMQ_USER = os.getenv('RABBITMQ_USER', 'guest')
RMQ_PASS = os.getenv('RABBITMQ_PASSWORD', 'guest')
RMQ_QUEUE = os.getenv('RABBITMQ_QUEUE', 'urls')

def process_url(url):
    try:
        with httpx.Client() as client:
            response = client.get(url)
            response.raise_for_status()
            return extract_links_from_page(response.text, url)
    except httpx.RequestError as e:
        print(f"Error fetching {url}: {e}")
        return []

def extract_links_from_page(page_content, url):
    soup = BeautifulSoup(page_content, 'html.parser')
    base_url = f"{urlparse(url).scheme}://{urlparse(url).netloc}"
    links = []
    
    print(f"Processing page: {get_page_title(soup)} ({url})")

    # Извлечение ссылок
    for tag in soup.find_all(['a', 'img', 'video', 'audio']):
        resource = tag.get('href') or tag.get('src')
        if resource:
            full_url = urljoin(base_url, resource)
            if full_url.startswith(base_url):
                links.append(full_url)
                print(f"Link found: {full_url}")
                
    return links

def get_page_title(soup):
    title_tag = soup.find('title')
    return title_tag.text if title_tag else "No Title"

def send_links_to_rabbitmq(links, url):
    try:
        creds = pika.PlainCredentials(RMQ_USER, RMQ_PASS)
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=RMQ_HOST, port=RMQ_PORT, credentials=creds))
        channel = connection.channel()
        channel.queue_declare(queue=RMQ_QUEUE, durable=True)

        if links:
            for link in links:
                channel.basic_publish(
                    exchange='',
                    routing_key=RMQ_QUEUE,
                    body=link,
                    properties=pika.BasicProperties(delivery_mode=2)
                )
                print(f"Sent: {link}")
        else:
            print(f"No links found on {url}")

        connection.close()
    except pika.exceptions.AMQPConnectionError as err:
        print(f"RabbitMQ error: {err}")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('url', help="URL to fetch links from")
    args = parser.parse_args()
    
    links = process_url(args.url)
    send_links_to_rabbitmq(links, args.url)

if __name__ == "__main__":
    main()
