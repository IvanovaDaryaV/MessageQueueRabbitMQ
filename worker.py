import asyncio
import os
import pika
import aiohttp
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from dotenv import load_dotenv

# Загрузка переменных окружения
load_dotenv()

RABBITMQ_HOST = os.getenv('RABBITMQ_HOST', 'localhost')
RABBITMQ_QUEUE = os.getenv('RABBITMQ_QUEUE', 'links_queue')
TIMEOUT = int(os.getenv('QUEUE_TIMEOUT', 10))  # Таймаут в секундах

async def fetch_html(session, url):
    async with session.get(url) as response:
        return await response.text()

async def get_links(base_url, html):
    soup = BeautifulSoup(html, 'html.parser')
    links = []
    for a_tag in soup.find_all('a', href=True):
        href = a_tag['href']
        full_url = urljoin(base_url, href)
        if urlparse(full_url).netloc == urlparse(base_url).netloc:  # Внутренние ссылки
            links.append((a_tag.get_text(strip=True) or "No Title", full_url))
    return links

async def process_link(channel, session, link):
    print(f"Processing link: {link}")
    html = await fetch_html(session, link)
    links = await get_links(link, html)
    for title, link in links:
        print(f"Found link: {title} ({link})")
        channel.basic_publish(exchange='', routing_key=RABBITMQ_QUEUE, body=link)

async def worker():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()
    channel.queue_declare(queue=RABBITMQ_QUEUE)

    async with aiohttp.ClientSession() as session:
        while True:
            method_frame, header_frame, body = channel.basic_get(queue=RABBITMQ_QUEUE, auto_ack=True)
            if body:
                link = body.decode()
                await process_link(channel, session, link)
            else:
                print("Queue empty, waiting...")
                await asyncio.sleep(TIMEOUT)
                break

    connection.close()

if __name__ == "__main__":
    asyncio.run(worker())
