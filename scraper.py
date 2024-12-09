import sys
import requests
from bs4 import BeautifulSoup
import pika
from urllib.parse import urlparse, urljoin

def get_internal_links(url):
    domain = urlparse(url).netloc
    try:
        response = requests.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        links = set()

        for a_tag in soup.find_all('a', href=True):
            link = a_tag['href']
            # Сделаем ссылку абсолютной
            link = urljoin(url, link)
            link_domain = urlparse(link).netloc
            # Сохраняем только внутренние ссылки
            if link_domain == domain:
                links.add(link)
        return links
    except requests.RequestException as e:
        print(f"Ошибка при запросе URL {url}: {e}")
        return set()

def main():
    if len(sys.argv) != 2:
        print("Использование: python collect_links.py <URL>")
        sys.exit(1)

    url = sys.argv[1]
    internal_links = get_internal_links(url)

    # Установим соединение с RabbitMQ
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    # Объявляем очередь
    channel.queue_declare(queue='links_queue')

    for link in internal_links:
        # Отправляем каждую ссылку в очередь
        channel.basic_publish(exchange='',
                              routing_key='links_queue',
                              body=link)
        print(f"Ссылка отправлена в очередь: {link}")

    # Закрываем соединение
    connection.close()

if __name__ == "__main__":
    main()
