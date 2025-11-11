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
    """Получить список товаров магазина озон. 

    Функция делает запрос к листу продуктов, с
    определенными параметрами, затем возвращет итоговый
    список продуктов. (за раз не более 1000)
    Args:
        last_id (str): Идентификатор последнего значения на странице
        (при первом запросе оставить пустым)
        client_id (str): Идентификационный номер клиента
        seller_token (str): токен продавца

    Returns:
        list: Возращает итоговый список c
        результатами запросов

    Example:
    >>> get_product_list(12344, 1234, '3d3f2e')


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
    """Получить артикулы товаров

    Получает список продуктов(по id клиента) и по нему
    возвращает артикулы этих товаров

    Args:
        client_id (str): Идентификационный номер клиента
        seller_token (str): токен продавца
    Returns:
        list: список артикулов товаров
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
    """Обновить цены товаров (До 100)

    Отправляет запрос в ozon на изменение цены
    в базе данных.

    Args:
        prices (list): Информация о ценах с товарами.
        Пример:
            "[ {"auto_action_enabled": "UNKNOWN",
            "auto_add_to_ozon_actions_list_enabled": "UNKNOWN",
            "currency_code": "RUB",
            "manage_elastic_boosting_through_price": true,
            "min_price": "800",
            "min_price_for_auto_actions_enabled": true,
            "net_price": "650",
            "offer_id": "",
            "old_price": "0",
            "price": "1448",
            "price_strategy_enabled": "UNKNOWN",
            "product_id": 1386,
            "quant_size": 1,
            "vat": "0.1"} ]
        client_id (str): Идентификационный номер клиента
        seller_token (str): токен продавца

    Returns:
        list: Данные с результатами изменения

    Example:
    Правильный вывод:
    >>> update_price(prices, client_id, seller_token)
    { "result": [ {
    "product_id": 1386,
    "offer_id": "PH8865",
    "updated": true,
    "errors": [ ] }]}

    Неправильный вывод:
    >>> update_price(prices, client_id, seller_token)
    { "code": 0,
    "details": [ {
        "typeUrl": "string",
        "value": "string" }],
    "message": "string"}
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
    """Обновить остатки товара

    Позволяет изменить информацию о количестве товара в наличии

    Args:
        stocks (list): информация о товарах на складе
        client_id (str): Идентификационный номер клиента
        seller_token (str): Токен продавца

    Returns:
        list: информация об изменениях
    Example:
    Правильный вывод:
    >>> update_stocks(stocks: list, client_id, seller_token)
    {"result": [ {
    "warehouse_id": 22142605386000,
    "product_id": 118597312,
    "offer_id": "PH11042",
    "updated": true,
    "errors": [ ] }]}

    Неправильный вывод:
    >>> update_stocks(stocks: list, client_id, seller_token)
    {"code": 0,
    "details": [
    { "typeUrl": "string",
    "value": "string"} ],
    "message": "string"}
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
    """Скачать файл ostatki с сайта casio
    
    Делает запрос на timeworld.ru, скачивает зип архип, разархивирует
    в текущую папку, переносит данные из таблицы(.xls) в словарь, с разбиением
    на столюбы и строки.
    Returns:
        dict: Список данные таблицы с товарами.
    Example:
    >>>download_stock()
    
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
    """Создает список с данными об остатках товара на складе.

    Создает список,
    в котором хранится артикул товара и его
    количество на складе, из словаря с
    данными из таблицы.
    Args:
    watch_remnatns (dict): Словарь с товарами
    offer_ids (list): Список артикулов

    Returns:
    list: Список с id и количеством товаров
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
    """Возвращает список информации о ценах.

    Исходя из предложенных артикулов, формирует
    список словарей, преобразует цену.

    Args:
        watch_remnatns (list): Список товаров
        offer_ids (list): Список артикулов

    Returns:
        list: Итоговый список цен
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
    """Удаляет незначимые нули, лишние символы из цены.

    Удаляет числа, которые мешают восприятию цены, а
    также ненужные символы, оставляя только цифры.

    Args:
        price (str): Цена продукта

    Returns:
        str: Возвращает цену, без лишних нулей
        и других символов

    Example:
    Правильный ввод:
    >>> print(price_conversion("5'990.00"))
    "5990"
    """
    return re.sub("[^0-9]", "", price.split(".")[0])


def divide(lst: list, n: int):
    """Разделить список на части

    Делит список на части, в которой по n-элементов.
    Args:
        lst (list): Список с вашими данными.
        n (int): Количество элементов в делимой части.
    Return:
        list: Итоговый список
    Example:
    >>> print(divide([1, 2, 3, 4], 2))
    [[1, 2], [3, 4]]
    """
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


async def upload_prices(watch_remnants, client_id, seller_token):
    """Обноваляет цены в базе данных.

    Составляет список с измененными ценами, делит этот
    списк по 1000 элементов, и отправляет запрос на изменение.
    Возвращает список актуальных цен.

    Args:
        watch_remnants (dict): Словарь данных c товарами
        client_id (str): Идентификационный номер клиента
        seller_token (str): Токен продавцв

    Returns:
        list: Список актуальных цен
    """
    offer_ids = get_offer_ids(client_id, seller_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_price in list(divide(prices, 1000)):
        update_price(some_price, client_id, seller_token)
    return prices


async def upload_stocks(watch_remnants, client_id, seller_token):
    """Отправляет данные о складе в базу данных ozon

    Создает список товаров склада, и частями отправляет в ozon обновленные
    данные товара на складе.

    Args:
    watch_remnants (dict): Словарь данных с товарами
    client_id (str): Идентификационный номер клиента
    seller_token (str): Токен продавца

    Returns:
    not_empty (list): Cписок товаров, которые есть в наличии
    stock (list): Список всех товаров
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
