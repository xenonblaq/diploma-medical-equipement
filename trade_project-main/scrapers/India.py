import pandas as pd                             
import numpy as np                              
import os                                       # Проверка/удаление временных CSV-файлов
import calendar                                 # Названия месяцев для прогресс-бара
import requests                                 
from bs4 import BeautifulSoup                   # Парсинг HTML (достать CSRF _token)
from tqdm import tqdm                          


class India:
    """
    Парсер торговли Индии с Россией (HS8) через сайт tradestat.commerce.gov.in (MEIDB).

    Что делает:
    - Открывает главную страницу, достает CSRF-токен (_token) для антипарсера
    - По каждому месяцу/году и направлению (export/import) отправляет POST-запрос
    - Для каждого месяца делает два запроса:
        valor=1 -> таблица со стоимостью (USD)  (по коду вы берете колонку df.columns[3])
        valor=2 -> таблица с количеством + единицей измерения (по коду df.columns[4])
    - Складывает результаты в два временных CSV:
        ./data/india_trade_usd.csv
        ./data/india_trade_KG.csv
    - Затем decor() делает merge двух CSV и приводит к общей схеме проекта.

    Внешние файлы:
    - НЕТ обязательных внешних справочников (кроме папки ./data для временных CSV).
    """

    india_params = {
        # Базовая страница: нужна чтобы получить CSRF токен
        "base_url": "https://tradestat.commerce.gov.in/",
        # Endpoint для импорта / экспорта (страна-wise all commodities)
        "im_url": "https://tradestat.commerce.gov.in/meidb/country_wise_all_commodities_import",
        "exdd_url": "https://tradestat.commerce.gov.in/meidb/country_wise_all_commodities_export",

        # Payload для импорта
        # ВАЖНО: '_token', Month, Year, ReportVal заполняются динамически
        "im_payload": {
            '_token': None,
            'cwcimMonth': None,
            'cwcimYear': None,
            'cwcimallcount': 344,          # 344 обычно = код РФ (country count / partner)
            'cwcimCommodityLevel': 8,      # уровень товара = HS8
            'cwcimReportVal': None,        # 1 или 2 (стоимость vs количество)
            'cwcimReportYear': 2           # вероятно тип периода/года (как в интерфейсе)
        },

        # Payload для экспорта (exdd)
        "exdd_payload": {
            '_token': None,
            'cwcexddMonth': None,
            'cwcexddYear': None,
            'cwcexallcount': 344,
            'cwcexddCommodityLevel': 8,
            'cwcexddReportVal': None,
            'cwcexddReportYear': 2
        },

        # Заголовки — имитация браузера
        "headers": {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
            "Accept-Encoding": "gzip, deflate, br",
            "Origin": "https://tradestat.commerce.gov.in",
            "Referer": "https://tradestat.commerce.gov.in/",
            "Connection": "keep-alive",
            "Content-Type": "application/x-www-form-urlencoded",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "same-origin",
            "Sec-Fetch-User": "?1",
            "Cache-Control": "max-age=0"
        },

        # Временные CSV:
        # 1) “usd” — стоимость
        # 2) “KG”  — количество + unit
        # Они в индийской таможне отдельно
        "CSV_PATH_INDIA_1": "./data/india_trade_usd.csv",
        "CSV_PATH_INDIA_2": "./data/india_trade_KG.csv"
    }

    def __init__(self, years):
        # years: список строк-годов, например ["2019",...,"2025"]
        self.years = years

    # Приведение к общей структуре 
    def decor(self):
        """
        1) Читает два временных CSV (стоимость и количество)
        2) Мерджит по HSCode, Commodity, ttype, year, month
        3) Формирует стандартные поля проекта (HS2/4/6/8, страны, направление)
        4) Возвращает DataFrame
        """

        # CSV со стоимостью (valor=1)
        data_1 = pd.read_csv(India.india_params["CSV_PATH_INDIA_1"])
        # CSV с количеством/единицей (valor=2)
        data_2 = pd.read_csv(India.india_params["CSV_PATH_INDIA_2"])

        # Объединяем стоимость и массу в одну строку
        data = pd.merge(data_1, data_2, on=["HSCode", "Commodity", "ttype", "year", "month"], how="inner")

        # Год/месяц -> отчетный период
        data["year"] = data["year"].astype(str)
        data["month"] = data["month"].astype(str).str.zfill(2)
        data['Отчетный период'] = "01." + data["month"] + '.' + data["year"]

        # Страны
        data["Страна-партнер"] = "Россия"
        data["Исходная страна"] = "Индия"

        # HS-коды
        data["Код товара (10 знаков)"] = np.nan
        data["Код товара (8 знаков)"] = data["HSCode"].astype(str).str.zfill(8)
        data["Код товара (6 знаков)"] = data["Код товара (8 знаков)"].str[:6]
        data["Код товара (2 знака)"] = data["Код товара (8 знаков)"].str[:2]
        data["Код товара (4 знака)"] = data["Код товара (8 знаков)"].str[:4]

        # Стоимость:
        # ВАЖНО: value_x в “миллионах USD”
        data["Значение (стоимость)"] = data["value_x"].astype(float) * 1000000

        # Масса:
        data["Значение (масса)"] = data["value_y"]

        data["Единицы стоимости"] = "USD"
        data["Единица объема"] = data["Unit"]

        # Доп. единицы отсутствуют
        data["ДЭИ, описание"] = np.nan
        data["Дополнительная единица измерения (ДЭИ)"] = np.nan

        # Направление
        data["Направление"] = data["ttype"].replace({"im": "Импорт", "exdd": "Экспорт"})

        # Фильтр нулей/NaN
        data = data[(data["Значение (стоимость)"] != 0) & (data["Значение (стоимость)"] != '0')]
        data = data[~data["Значение (стоимость)"].isna()]

        # Сортировка
        data = data.sort_values(
            by=['year', 'month', 'Направление'],
            ascending=[False, False, True],
            key=lambda col: col.astype(int) if col.name in ['year', 'month'] else col,
            ignore_index=True
        )

        # Финальные колонки
        data = data[[
            "Отчетный период", "Исходная страна", "Страна-партнер", "Направление",
            "Код товара (2 знака)", "Код товара (4 знака)", "Код товара (6 знаков)",
            "Код товара (8 знаков)", "Код товара (10 знаков)", "Значение (стоимость)",
            "Единицы стоимости", "Значение (масса)", "Единица объема",
            "Дополнительная единица измерения (ДЭИ)", "ДЭИ, описание"
        ]]

        data["Значение (стоимость) - ДЭИ"] = np.nan

        # Отчетный период -> datetime
        data['Отчетный период'] = pd.to_datetime(data['Отчетный период'], format='%d.%m.%Y')
        return data

    # Основной парсинг
    def parse(self) -> pd.DataFrame:
        """
        1) Удаляет временные CSV (если есть)
        2) Создает requests.Session() (важно для cookies/сессии + токен)
        3) GET на base_url -> BeautifulSoup -> вытаскивает _token
        4) Для ttype in ["exdd","im"], для каждого года/месяца:
            - valor=1: берет таблицу стоимости
            - valor=2: берет таблицу количества+unit
           и пишет в соответствующий CSV_PATH_INDIA_1/2
        5) decor() приводит к финальному виду
        6) Чистит временные CSV
        """

        india_params = India.india_params.copy()

        # Чистим временные CSV
        if os.path.exists(india_params["CSV_PATH_INDIA_1"]):
            os.remove(india_params["CSV_PATH_INDIA_1"])
        if os.path.exists(india_params["CSV_PATH_INDIA_2"]):
            os.remove(india_params["CSV_PATH_INDIA_2"])

        first_iter = True
        session = requests.Session()

        # Получаем CSRF токен
        response = session.get(india_params["base_url"]).text
        soup = BeautifulSoup(response, "lxml")

        # Если сайт поменяет имя input или структуру — token не найдется.
        token = soup.find(name="input", attrs={"name": "_token"}).get("value")

        india_params["exdd_payload"]["_token"] = token
        india_params["im_payload"]["_token"] = token

        # Парсим два направления: export (exdd) и import (im)
        for ttype in ["exdd", "im"]:
            for year in range(int(self.years[0]), int(self.years[-1]) + 1):

                pb = tqdm(range(1, 13))
                for month in pb:
                    pb.set_description(f"Собрираем {calendar.month_name[month]} {year} года, {ttype}")

                    # valor:
                    # 1 -> стоимость
                    # 2 -> количество/единица
                    for valor in [1, 2]:

                        # заполняем payload динамически
                        india_params[f"{ttype}_payload"][f"cwc{ttype}ReportVal"] = valor
                        india_params[f"{ttype}_payload"][f"cwc{ttype}Month"] = month
                        india_params[f"{ttype}_payload"][f"cwc{ttype}Year"] = year

                        # POST запрос
                        response = session.post(
                            url=india_params[f"{ttype}_url"],
                            headers=india_params["headers"],
                            data=india_params[f"{ttype}_payload"]
                        )

                        # Читаем HTML-таблицу
                        df = pd.read_html(response.text, index_col=0)[0]

                        # valor=1: стоимость
                        if valor == 1:
                            df["value"] = df[df.columns[3]]  
                            df = df[['HSCode', 'Commodity', 'value']]

                        # valor=2: количество + Unit
                        else:
                            df["value"] = df[df.columns[4]]  
                            df = df[['HSCode', 'Commodity', 'Unit', 'value']]

                        # добавляем метаданные
                        df["ttype"] = ttype
                        df["year"] = year
                        df["month"] = month

                        # удаляем Total
                        df = df[df["Commodity"] != "Total"]

                        # пишем в нужный CSV
                        df.to_csv(
                            india_params[f"CSV_PATH_INDIA_{valor}"],
                            mode='a',
                            index=False,
                            header=first_iter,
                            encoding="utf-8-sig"
                        )
                        
                        # дальше без заголовков
                        if valor == 2:
                            first_iter = False

        print("Парсинг успешно завершен. Перехожу к составлению и оформлению итоговой таблицы.")

        # оформляем финальную таблицу
        data = self.decor()

        # чистим временные CSV
        if os.path.exists(india_params["CSV_PATH_INDIA_1"]):
            os.remove(india_params["CSV_PATH_INDIA_1"])
        if os.path.exists(india_params["CSV_PATH_INDIA_2"]):
            os.remove(india_params["CSV_PATH_INDIA_2"])

        return data