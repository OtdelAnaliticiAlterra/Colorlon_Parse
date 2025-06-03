import aiohttp
import asyncio
from selectolax.parser import HTMLParser
import time
import pandas as pd
import openpyxl
import os
from dotenv import load_dotenv, find_dotenv
from telegram_bot_logger import TgLogger
from pathlib import Path
#
#
BASE_DIR = Path(__file__).resolve().parent

load_dotenv(find_dotenv())

CHATS_IDS = '\\\\TG-Storage01\\Аналитический отдел\\Проекты\\Python\\chats_ids.csv'

logger = TgLogger(
    name='Парсинг_Колорлон',
    token=os.environ.get('LOGGER_BOT_TOKEN'),
    chats_ids_filename=CHATS_IDS,
)


async def get_response(session, url, retries=3, flag=0):
    """Получение ответа от сервера с обработкой ошибок"""

    cookies = {
        'selected-shop-id': '3'
    }

    for attempt in range(retries):
        try:

            async with session.get(url, cookies=cookies, timeout=1000) as response:
                # Проверяем статус ответа
                if response.status == 500 or response.status == 502:
                    print(f"Server error (500): Retrying... for URL: {url}. Attempt {attempt + 1} of {retries}.")
                    await asyncio.sleep(2)
                    flag = 1
                    continue
                response.raise_for_status()
                return await response.text(), flag
        except (aiohttp.ClientTimeout, aiohttp.ClientError) as e:
            print(f"Network error occurred: {e}. Attempt {attempt + 1} of {retries}. Retrying...")
            await asyncio.sleep(2)
        except asyncio.TimeoutError:
            print(f"Timeout error occurred for URL: {url}. Attempt {attempt + 1} of {retries}. Retrying...")
            await asyncio.sleep(2)
        except aiohttp.ClientResponseError as e:
            print(f"ClientResponseError: {e.status} - {e.message} for URL: {url}")
            break  # Прерываем цикл для других ошибок статуса
        except Exception as e:
            print(f"An unexpected error occurred while requesting {url}: {e}")
            break
    return None, flag  # Возвращаем None и флаг

async def parse_categories(session):
    """Парсинг категорий товаров"""
    response_text, flag = await get_response(session, 'https://colorlon.ru/catalog/')
    if response_text is not None:
        parser = HTMLParser(response_text)

        cat_links = [f'https://colorlon.ru/catalog/{categories.attributes.get("data-code")}' for categories in
                     parser.css('div.menu-nav__nav.js-megamenu')]

        return cat_links
    return []

async def parse_products(session):
    """Парсинг информации о товарах"""
    supply_path = await parse_categories(session)
    supply_path = supply_path[2:14]
    # supply_path = ["https://colorlon.ru/catalog/stroitelnye-materialy"]
    good_links = []
    product_links = []
    article_list = []
    name_list = []
    price_list = []
    dratel = []
    for elem in supply_path:
        print(f"elem = {elem}")
        response_text,flag = await get_response(session, elem)

        if response_text is not None:
            parser = HTMLParser(response_text)
            subcats = [item.attributes.get("href") for item in parser.css("div.search__sections a")]
            for elem in subcats:
                print(elem)
                response_text,flag = await get_response(session,elem)
                parser = HTMLParser(response_text)
                page = [item.text() for item in parser.css("div.pagination__pages a")]
                if len(page) == 0:
                    for itm in parser.css("div.product-card__body a"):
                        ref = itm.attributes.get("href")
                        print(ref)
                        print(itm.text())
                        good_links.append(ref)
                        name_list.append(itm.text().split("            ")[1].split("\n")[0])
                else:
                    for el in page:
                        if el.isdigit():
                            page.append(el)
                    max_page = int(max(page))

                    for num in range(1,max_page+1):
                        response_text,flag = await get_response(session, f"{elem}?&page={num}&per_page=20")
                        parser = HTMLParser(response_text)
                        for itm in parser.css("div.product-card__body a"):
                            ref = itm.attributes.get("href")
                            print(ref)
                            print(itm.text().split("            ")[1].split("\n")[0])
                            good_links.append(ref)
                            name_list.append(itm.text().split("            ")[1].split("\n")[0])


    return good_links,  name_list


async def parse_inner_info(session):
    good_links, name_list = await parse_products(session)
    article_list = []
    price_list = []

    for elem in good_links:
        print(f"inner_elem = {elem}")
        response_text, flag = await get_response(session, elem)

        if response_text is None:  # Проверяем на None
            if flag == 1:  # Обработка ошибки 500
                print(f"Ошибка 500 при обработке {elem}. Переход к следующему элементу.")
            continue  # Переход к следующему элементу в good_links

        parser = HTMLParser(response_text)

        if parser.css("div.product__notstock"):
            continue  # Если товар отсутствует на складе, переходим к следующему элементу

        # for item in parser.css('div.product__buy-container div.product__price span.js-current-price'):
        for item in parser.css('div.product__top span.js-current-price'):
            price_list.append(item.text().replace(' ',''))
            print(item.text())
        for item in parser.css('div.product__main span.product__article'):
            if item.text() == '':
            # article_list.append(item.text().split(':')[1].replace(' ', '').split("\n")[0])
                article_list.append("-")
            else:
                article_list.append(item.text())


    return good_links, article_list, name_list, price_list
async def main():
    start = time.time()
    async with aiohttp.ClientSession() as session:
        product_links, article_list, name_list, price_list = await parse_inner_info(session)
        # await parse_inner_info(session)

        print(len(product_links))
        print(len(article_list))
        print(len(name_list))
        print(len(price_list))
        name_list = name_list[:len(article_list)]
        product_links = product_links[:len(article_list)]

        new_slovar = {
            "Код конкурента":  "01-01073977",
            "Конкурент": "Колорлон",
            "Артикул": article_list,
            "Наименование": name_list,
            "Вид цены": "Цена КолорлонНовосибирск",
            "Цена": price_list,
            "Ссылка": product_links
        }
        df = pd.DataFrame(new_slovar)
        file_path = "\\\\tg-storage01\\Аналитический отдел\\Проекты\\Python\\Парсинг конкрунтов\\Выгрузки\\Колорлон\\Выгрузка цен.xlsx"
        # file_path = "C:\\Users\\Admin\\Desktop\\Выгрузки парсеров\\Выгрузка цен.xlsx"
        if os.path.exists(file_path):
            os.remove(file_path)

        df.to_excel(file_path, sheet_name="Данные", index=False)
        print("Парсинг выполнен")
    end = time.time()
    print("Время", (end - start))


if __name__ == "__main__":
    # try:
        asyncio.run(main())
    # except Exception as e:
    #     logger.error(e)
    #     raise e
