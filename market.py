import datetime
import logging.config
from environs import Env
from seller import download_stock

import requests

from seller import divide, price_conversion

logger = logging.getLogger(__file__)


def get_product_list(page, campaign_id, access_token):
    """
    Получить список товаров в каталоге с информацией.

    Лимит 10 000 товаров в минуту, Для каждого товара возвращается
    Идентификатор текущей карточки(marketSku) и описание товара.

    Args:
        page (&): Идентификатор страницы c результатами.
        capmaign_id (str): Идентификационный номер компании.
        access_token (str): Уникальный токен доступа.

    Returns:
        Dict: Информация о товарах в каталоге.
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {
        "page_token": page,
        "limit": 200,
    }
    url = endpoint_url + f"campaigns/{campaign_id}/offer-mapping-entries"
    response = requests.get(url, headers=headers, params=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def update_stocks(stocks, campaign_id, access_token):
    """
    Передает данные об остатках товаров.

    Обновляет информацию на складе о количестве товаров.
    Для группы складов передавайте остатки только для одного любого склада.
    Информация для остальных складов в этой группе обновится автоматически.

    Args:
        stocks (list): Информация об остатках товара.
        campaign_id (str): Идентификационный номер компании.
        access_token (str): Индивидуальный токен доступа.
    Returns:
        dict: Ответ от сервера.
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {"skus": stocks}
    url = endpoint_url + f"campaigns/{campaign_id}/offers/stocks"
    response = requests.put(url, headers=headers, json=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object


def update_price(prices, campaign_id, access_token):
    """
    Обновляет полученные цена на сайте.

    Делает запрос на сайт с изменением цен, только по тем
    ценам, которые получает.

    Args:
        prices (list): Список цен на товары.
        campaign_id (str): Идентификационный номер компании.
        access_token (str): Уникальный токен доступа.
    Returns:
        Json: Ответ запроса.
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {"offers": prices}
    url = endpoint_url + f"campaigns/{campaign_id}/offer-prices/updates"
    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object


def get_offer_ids(campaign_id, market_token):
    """
    Получить артикулы товаров Яндекс маркета.

    Запршивает лист товаров, извлекает из него артикулы.

    Args:
        campaign_id(str): Идентификационный номер компании.
        market_token(str): Уникальный токен доступа.
    Returns:
        list: Список с артикулами(Sku) товаров.
    """
    page = ""
    product_list = []
    while True:
        some_prod = get_product_list(page, campaign_id, market_token)
        product_list.extend(some_prod.get("offerMappingEntries"))
        page = some_prod.get("paging").get("nextPageToken")
        if not page:
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer").get("shopSku"))
    return offer_ids


def create_stocks(watch_remnants, offer_ids, warehouse_id):
    """
    Формирует список товаров по артикулам.

    Создает список товаров, чьи id содержаться в offer_ids.
    Товар представлен ввиде словаря с данными:
    Количество, артикул, вид и тд.

    Args:
        watch_remnants (list): Полный список товаров со склада.
        offer_ids (list): Список артикулов, от яндекс маркета.
        warehouse_id (str): Идентификационный номер склада.
    Returns:
        list: Список товаров из offer_ids
    """
    # Уберем то, что не загружено в market
    stocks = list()
    date = str(datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z")
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append(
                {
                    "sku": str(watch.get("Код")),
                    "warehouseId": warehouse_id,
                    "items": [
                        {
                            "count": stock,
                            "type": "FIT",
                            "updatedAt": date,
                        }
                    ],
                }
            )
            offer_ids.remove(str(watch.get("Код")))
    # Добавим недостающее из загруженного:
    for offer_id in offer_ids:
        stocks.append(
            {
                "sku": offer_id,
                "warehouseId": warehouse_id,
                "items": [
                    {
                        "count": 0,
                        "type": "FIT",
                        "updatedAt": date,
                    }
                ],
            }
        )
    return stocks


def create_prices(watch_remnants, offer_ids):
    """
    Формирует список цен товаров из предложенных артикулов

    Создает список с информацией о товаре(Цена, артикул), сохраняя
    ввиде словаря. В списке только товары, id которых есть в offer_ids

    Args:
        watch_remnants(list): Полный список товаров со склада.
        offer_ids(list): Список id,
    Returns:
        list: Список актуальных цен
    """
    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "id": str(watch.get("Код")),
                # "feed": {"id": 0},
                "price": {
                    "value": int(price_conversion(watch.get("Цена"))),
                    # "discountBase": 0,
                    "currencyId": "RUR",
                    # "vat": 0,
                },
                # "marketSku": 0,
                # "shopSku": "string",
            }
            prices.append(price)
    return prices


async def upload_prices(watch_remnants, campaign_id, market_token):
    """
    Обновляет цены товаров на сайте.

    Создает список цен по offer_ids, и делает
    запросы на обновление цен (по 500 штук).

    Args:
        watch_remnants (list): Полный список товаров со склада.
        campaign_id (str): Идентификационный номер компании.
        market_token (str): Уникальный токен доступа.
    Returns:
        list: Список актуальных цен
    """
    offer_ids = get_offer_ids(campaign_id, market_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_prices in list(divide(prices, 500)):
        update_price(some_prices, campaign_id, market_token)
    return prices


async def upload_stocks(watch_remnants, campaign_id, market_token, warehouse_id):
    """
    Формирует данные о товарах складе и возвращает его текущее состояние.

    Создает список данных про товары определенной компании(campaign_id), затем
    частями отправляет эти данные в Яндекс. Возвращает два списка: в первом
    хранятся только товары имеющиеся в наличии, во втором все товары.

    Args:
        watch_remnants (list): Полный список товаров со склада.
        campaign_id (str): Идентификационный номер компании.
        market_token (str): Уникальный токен доступа.
        warehouse_id (str): Идентификационный номер склада.
    Returns:
        not_empty(list): Список только тех товаров,которые имеющются в наличии.
        stocks (list): Список товаров.
    """
    offer_ids = get_offer_ids(campaign_id, market_token)
    stocks = create_stocks(watch_remnants, offer_ids, warehouse_id)
    for some_stock in list(divide(stocks, 2000)):
        update_stocks(some_stock, campaign_id, market_token)
    not_empty = list(
        filter(lambda stock: (stock.get("items")[0].get("count") != 0), stocks)
    )
    return not_empty, stocks


def main():
    env = Env()
    market_token = env.str("MARKET_TOKEN")
    campaign_fbs_id = env.str("FBS_ID")
    campaign_dbs_id = env.str("DBS_ID")
    warehouse_fbs_id = env.str("WAREHOUSE_FBS_ID")
    warehouse_dbs_id = env.str("WAREHOUSE_DBS_ID")

    watch_remnants = download_stock()
    try:
        # FBS
        offer_ids = get_offer_ids(campaign_fbs_id, market_token)
        # Обновить остатки FBS
        stocks = create_stocks(watch_remnants, offer_ids, warehouse_fbs_id)
        for some_stock in list(divide(stocks, 2000)):
            update_stocks(some_stock, campaign_fbs_id, market_token)
        # Поменять цены FBS
        upload_prices(watch_remnants, campaign_fbs_id, market_token)

        # DBS
        offer_ids = get_offer_ids(campaign_dbs_id, market_token)
        # Обновить остатки DBS
        stocks = create_stocks(watch_remnants, offer_ids, warehouse_dbs_id)
        for some_stock in list(divide(stocks, 2000)):
            update_stocks(some_stock, campaign_dbs_id, market_token)
        # Поменять цены DBS
        upload_prices(watch_remnants, campaign_dbs_id, market_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
