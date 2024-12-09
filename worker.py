import pika
import asyncio
import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urljoin

async def get_internal_links(url):
    domain = urlparse(url).netloc
    try:
        response = requests.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        links = set()

        for a_tag in soup.find_all('a', href=True):
            link = a_tag['href']
            link = urljoin(url, link)
            link_domain = urlparse(link).netloc
            if link_domain == domain:
                links.add(link)
        return links
    except requests.RequestException as e:
        print(f"Ошибка при запросе URL {url}: {e}")
        return set()

async def process_link(ch, method, properties, body):
    url = body.decode()
    print(f"Получена ссылка: {url}")

    # Получаем внутренние ссылки
    internal_links = await get_internal_links(url)

    # Отправляем новые ссылки обратно в очередь
    if internal_links:
        connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        channel = connection.channel()
        for link in internal_links:
            channel.basic_publish(exchange='',
                                  routing_key='links_queue',
                                  body=link)
            print(f"Новая ссылка отправлена в очередь: {link}")
        connection.close()

    ch.basic_ack(delivery_tag=method.delivery_tag)

async def consume():
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    # Объявляем очередь
    channel.queue_declare(queue='links_queue')

    # Таймаут в 10 секунд, после чего завершаем процесс, если очередь пуста
    def callback(ch, method, properties, body):
        loop.create_task(process_link(ch, method, properties, body))

    channel.basic_consume(queue='links_queue', on_message_callback=callback)

    print('Ожидание сообщений. Нажмите Ctrl+C для выхода.')

    try:
        await asyncio.wait_for(
            asyncio.to_thread(channel.start_consuming), timeout=10
        )
    except asyncio.TimeoutError:
        print("Таймаут истёк, завершение работы.")
        connection.close()

async def main():
    await consume()

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
