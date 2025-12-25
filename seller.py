import io
import logging.config
import os
import re
import zipfile
from environs import Env

import pandas as pd
import requests

logger = logging.getLogger(__file__)


def get_product_list(last_id, client_id, seller_token):
    """
    Получает список товаров магазина Ozon.

    Делает запрос к API Ozon Seller для получения списка товаров. Лимит запроса — 1000 товаров.

    Args:
        last_id (str): Идентификатор последнего товара на странице. При первом запросе должен быть пустой строкой.
        client_id (str): Идентификатор клиента.
        seller_token (str): API-ключ продавца.

    Returns:
        dict: Словарь с результатами запроса (ключ 'result'), содержащий список товаров.
    """
    url = "https://api-seller.ozon.ru/v2/product/list"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {
        "filter": {
            "visibility": "ALL",
        },
        "last_id": last_id,
        "limit": 1000,
    }
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def get_offer_ids(client_id, seller_token):
    """
    Получает список артикулов (offer_id) всех товаров.

    Последовательно запрашивает все страницы товаров через `get_product_list`
    и извлекает из них артикулы.

    Args:
        client_id (str): Идентификатор клиента.
        seller_token (str): API-ключ продавца.

    Returns:
        list: Список артикулов товаров (offer_id).
    """
    last_id = ""
    product_list = []
    while True:
        some_prod = get_product_list(last_id, client_id, seller_token)
        product_list.extend(some_prod.get("items"))
        total = some_prod.get("total")
        last_id = some_prod.get("last_id")
        if total == len(product_list):
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer_id"))
    return offer_ids


def update_price(prices: list, client_id, seller_token):
    """
    Обновляет цены товаров.

    Отправляет пакетный запрос в Ozon API для изменения цен.

    Args:
        prices (list): Список словарей с информацией о ценах. Каждый словарь должен содержать 'offer_id', 'price' и тд.
        client_id (str): Идентификатор клиента.
        seller_token (str): API-ключ продавца.

    Returns:
        dict: Ответ с результатами обновления.
    """
    url = "https://api-seller.ozon.ru/v1/product/import/prices"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"prices": prices}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def update_stocks(stocks: list, client_id, seller_token):
    """
    Обновляет остатки товаров.

    Отправляет пакетный запрос в Ozon API для изменения количества товара на складе.

    Args:
        stocks (list): Список словарей с информацией об остатках.
                       Каждый словарь должен содержать 'offer_id', 'stock' и тд.
        client_id (str): Идентификатор клиента.
        seller_token (str): API-ключ продавца.

    Returns:
        dict: Ответ API с результатами обновления.
    """
    url = "https://api-seller.ozon.ru/v1/product/import/stocks"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"stocks": stocks}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def download_stock():
    """
    Скачивает и обрабатывает файл с остатками.

    Загружает zip-архив с сайта TimeWorld, распаковывает его,
    читает Excel-файл и преобразует данные в список словарей.
    Временный файл удаляется после чтения.

    Returns:
        list: Список словарей, представляющих данные таблицы остатков.
    """
    casio_url = "https://timeworld.ru/upload/files/ostatki.zip"
    session = requests.Session()
    response = session.get(casio_url)
    response.raise_for_status()
    with response, zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        archive.extractall(".")
    excel_file = "ostatki.xls"
    watch_remnants = pd.read_excel(
        io=excel_file,
        na_values=None,
        keep_default_na=False,
        header=17,
    ).to_dict(orient="records")
    os.remove("./ostatki.xls")
    return watch_remnants


def create_stocks(watch_remnants, offer_ids):
    """
    Формирует список остатков для обновления.

    Сопоставляет данные из файла остатков с артикулами Ozon.
    Если товар есть в `offer_ids`, формирует запись с актуальным количеством.
    Если товара нет в файле, но он есть в Ozon, ставит остаток 0.

    Args:
        watch_remnants (list): Список данных об остатках.
        offer_ids (list): Список артикулов товаров из Ozon.

    Returns:
        list: Список словарей формата {'offer_id': str, 'stock': int}.
    """
    stocks = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append({"offer_id": str(watch.get("Код")), "stock": stock})
            offer_ids.remove(str(watch.get("Код")))
    for offer_id in offer_ids:
        stocks.append({"offer_id": offer_id, "stock": 0})
    return stocks


def create_prices(watch_remnants, offer_ids):
    """
    Формирует список цен для обновления.

    Сопоставляет данные из файла остатков с артикулами Ozon и формирует
    список цен в требуемом API формате.

    Args:
        watch_remnants (list): Список данных об остатках.
        offer_ids (list): Список артикулов товаров из Ozon.

    Returns:
        list: Список словарей с ценами для отправки в API.
    """
    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "auto_action_enabled": "UNKNOWN",
                "currency_code": "RUB",
                "offer_id": str(watch.get("Код")),
                "old_price": "0",
                "price": price_conversion(watch.get("Цена")),
            }
            prices.append(price)
    return prices


def price_conversion(price: str) -> str:
    """
    Очищает строку цены от лишних символов.

    Удаляет все нечисловые символы и отбрасывает дробную часть,
    оставляя только целое число в виде строки.

    Args:
        price (str): Исходная цена (например, "5'990.00").

    Returns:
        str: Очищенная цена (например, "5990").
    """
    return re.sub("[^0-9]", "", price.split(".")[0])


def divide(lst: list, n: int):
    """
     Для разбиения списка на части. Генератор.

    Args:
        lst (list): Исходный список.
        n (int): Размер одной части.

    Yields:
        list: Подсписок длиной `n` элементов.
    """
    for i in range(0, len(lst), n):
        yield lst[i: i + n]


async def upload_prices(watch_remnants, client_id, seller_token):
    """
    Выполняет полный цикл обновления цен.

    Получает артикулы, формирует цены и отправляет их пакетами по 1000 штук.

    Args:
        watch_remnants (list): Список данных об остатках.
        client_id (str): Идентификатор клиента.
        seller_token (str): API-ключ продавца.

    Returns:
        list: Список всех сформированных цен.
    """
    offer_ids = get_offer_ids(client_id, seller_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_price in list(divide(prices, 1000)):
        update_price(some_price, client_id, seller_token)
    return prices


async def upload_stocks(watch_remnants, client_id, seller_token):
    """
    Выполняет полный цикл обновления остатков.

    Получает артикулы, формирует данные об остатках и отправляет их пакетами по 100 штук.

    Args:
        watch_remnants (list): Список данных об остатках.
        client_id (str): Идентификатор клиента.
        seller_token (str): API-ключ продавца.

    Returns:
        tuple: Кортеж из двух списков, список товаров в наличии, полный список обновленных товаров.
    """
    offer_ids = get_offer_ids(client_id, seller_token)
    stocks = create_stocks(watch_remnants, offer_ids)
    for some_stock in list(divide(stocks, 100)):
        update_stocks(some_stock, client_id, seller_token)
    not_empty = list(filter(lambda stock: (stock.get("stock") != 0), stocks))
    return not_empty, stocks


def main():
    env = Env()
    seller_token = env.str("SELLER_TOKEN")
    client_id = env.str("CLIENT_ID")
    try:
        offer_ids = get_offer_ids(client_id, seller_token)
        watch_remnants = download_stock()
        stocks = create_stocks(watch_remnants, offer_ids)
        for some_stock in list(divide(stocks, 100)):
            update_stocks(some_stock, client_id, seller_token)
        prices = create_prices(watch_remnants, offer_ids)
        for some_price in list(divide(prices, 900)):
            update_price(some_price, client_id, seller_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
