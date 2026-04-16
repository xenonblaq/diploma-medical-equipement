import pandas as pd                             
import numpy as np                              
from tqdm import tqdm                          
import xml.etree.ElementTree as ET              # Парсинг XML (BIS API для курсов)
import requests                                
import os                                       # Проверка/удаление временных CSV
from pyjstat import pyjstat                     # Конвертация SDMX/JSON (pyjstat) -> DataFrame
import json                                     # json.loads для ответа Eurostat
from datetime import datetime                   # последняя дата EUR/USD
from requests.exceptions import ConnectionError # Исключение для разрыва соединения



class EU:
    """
    Парсер торговли (Молдова+Украина для Беларуси / список из 44 европейских стран для России) с партнером RU или BY
    через Eurostat COMEXT API (SDMX 3.0) + конвертация EUR -> USD по курсу BIS.

    Важные особенности:
    - Источник данных: Eurostat COMEXT (ds-059341)
    - Частота: M (месячные данные)
    - Метрики: VALUE_EUR и QUANTITY_KG
    - Коды товаров: HS6 (берутся из локального файла ./data/eu_codes.csv)  <-- обязательный файл
    - Периоды формируются вручную списком YYYY-MM от latest к earliest
    - В ответе коды товаров могут приходить как метки/лейблы -> делается reverse-map через dimension metadata
    - После сборки в EUR делается пересчет в USD по курсу EUR/USD (BIS WS_XRU)
    """

    EU_params = {
        "CSV_PATH_EU": "./data/EU_trade.csv",                 # временный сырой CSV (RU)
        "CSV_PATH_EU_BELARUS": "./data/EU_belarus_trade.csv"  # временный сырой CSV (BY)
    }

    def __init__(
        self,
        years,
        partner="RU",
        belarus=False,
        countries=['AL', 'AT', 'BA', 'BE', 'BG', 'CH', 'CY', 'CZ', 'DE', 'DK', 'EE', 'ES', 'FI', 'FR', 'GB', 'GE', 'GR',
                   'HR', 'HU', 'IE', 'IS', 'IT', 'LI', 'LT', 'LU', 'LV', 'MD', 'ME', 'MK', 'MT', 'NL', 'NO', 'PL', 'PT',
                   'RO', 'SE', 'SI', 'SK', 'TR', 'UA', 'XI', 'XK', 'XM', 'XS']
    ):
        self.years = years              # список строк-годов, напр. ["2019","2020",...,"2025"] (важен порядок, см. parse)
        self.belarus = belarus          # если True: партнер BY, а reporter фиксирован MD,UA (см. URL)
        self.countries = countries      # список reporter-кодов (страны)
        self.partner = partner          # партнер, по умолчанию RU

    # Получение курса EUR/USD из BIS
    def currency(self):
        """
        Тянет месячные значения курса EUR/USD из BIS (stats.bis.org),
        возвращает DataFrame:
            Отчетный период (01.MM.YYYY), Курс, Единицы стоимости (из атрибута CURRENCY)
        """

        end_period = datetime.today().strftime("%Y-%m")   # Сегодняшняя дата, например "2025-12"
        url = f"https://stats.bis.org/api/v1/data/BIS,WS_XRU,1.0/M.XM.EUR.A/all?startPeriod=2019-01&endPeriod={end_period}"
        resp = requests.get(url)
        resp.raise_for_status()

        root = ET.fromstring(resp.content)
        series = root.find('.//Series')
        currency = series.get('CURRENCY')

        records = []
        for obs in series.findall('Obs'):
            period = obs.get('TIME_PERIOD')        # формат YYYY-MM
            value = float(obs.get('OBS_VALUE'))    # курс
            records.append({
                'Отчетный период': period,
                'Курс': value,
                'Единицы стоимости': currency
            })

        df = pd.DataFrame(records)

        # Превращаем YYYY-MM -> 01.MM.YYYY
        df['Отчетный период'] = "01." + df['Отчетный период'].str[-2:] + '.' + df['Отчетный период'].str[:4]
        return df

    # Приведение результата общей структуре + конвертация в USD
    def decor(self):
        """
        1) Читает временный CSV (EU_trade или EU_belarus_trade)
        2) Склеивает VALUE_EUR и QUANTITY_KG в одну строку по ключам
        3) Собирает HS2/4/6 и стандартные поля
        4) Конвертирует EUR -> USD по курсу BIS (currency())
        5) Возвращает итоговый DataFrame в формате проекта
        """

        if self.belarus:
            data = pd.read_csv(EU.EU_params["CSV_PATH_EU_BELARUS"])
        else:
            data = pd.read_csv(EU.EU_params["CSV_PATH_EU"])

        # СКЛЕЙКА value (стоимость) и quantity (кг):
        data = pd.merge(
            data[data["INDICATORS"] == "VALUE_EUR"],
            data[data["INDICATORS"] == "QUANTITY_KG"],
            how="inner",
            on=['REPORTER', 'PARTNER', 'PRODUCT', 'FLOW', 'TIME_PERIOD']
        )

        # TIME_PERIOD формат YYYY-MM
        data["Year"] = data["TIME_PERIOD"].str[:4]
        data["Month"] = data["TIME_PERIOD"].str[5:].str.zfill(2)
        data['Отчетный период'] = "01." + data["Month"] + '.' + data["Year"]

        # Партнер фиксируем по флагу
        if self.belarus:
            data["Страна-партнер"] = "Беларусь"
        else:
            data["Страна-партнер"] = "Россия"

        # Исходная страна — это REPORTER
        data["Исходная страна"] = data["REPORTER"]

        # HS6
        data["Код товара (6 знаков)"] = data["PRODUCT"].astype(str).str.zfill(6)
        data["Код товара (2 знака)"] = data["Код товара (6 знаков)"].str[:2]
        data["Код товара (4 знака)"] = data["Код товара (6 знаков)"].str[:4]

        data["Код товара (10 знаков)"] = np.nan
        data["Код товара (8 знаков)"] = np.nan

        # value_x / value_y — результат merge, где:
        # value_x = VALUE_EUR, value_y = QUANTITY_KG
        data["Значение (стоимость)"] = data["value_x"]
        data["Значение (масса)"] = data["value_y"]

        data["Единицы стоимости"] = "EUR"
        data["Единица объема"] = "килограмм"
        data["ДЭИ, описание"] = np.nan
        data["Дополнительная единица измерения (ДЭИ)"] = np.nan

        # FLOW: IMPORT / EXPORT
        data["Направление"] = data["FLOW"].replace({"IMPORT": "Импорт", "EXPORT": "Экспорт"})

        # Фильтр нулей/NaN по стоимости
        data = data[(data["Значение (стоимость)"] != 0) & (data["Значение (стоимость)"] != '0')]
        data = data[~data["Значение (стоимость)"].isna()]

        # Приводим к float
        data["Значение (стоимость)"] = data["Значение (стоимость)"].astype(float)
        data["Значение (масса)"] = data["Значение (масса)"].astype(float)

        # Сортировка
        data = data.sort_values(
            by=['Year', 'Month', 'Направление'],
            ascending=[False, False, True],
            key=lambda col: col.astype(int) if col.name in ['Year', 'Month'] else col,
            ignore_index=True
        )

        # Оставляем нужные колонки
        data = data[[
            "Отчетный период", "Исходная страна", "Страна-партнер", "Направление",
            "Код товара (2 знака)", "Код товара (4 знака)", "Код товара (6 знаков)",
            "Код товара (8 знаков)", "Код товара (10 знаков)", "Значение (стоимость)",
            "Единицы стоимости", "Значение (масса)", "Единица объема",
            "Дополнительная единица измерения (ДЭИ)", "ДЭИ, описание"
        ]]

        # Конвертация EUR -> USD
        df = self.currency()

        # МЕРДЖИМ ПО ("Отчетный период", "Единицы стоимости")
        data = pd.merge(data, df, how='left', on=["Отчетный период", "Единицы стоимости"])
        # Делим на курс
        data.loc[data["Единицы стоимости"] != "USD", "Значение (стоимость)"] = data["Значение (стоимость)"] / data["Курс"]
        # Убираем курс и меняем валюту на USD
        data = data.drop(columns="Курс")
        data["Единицы стоимости"] = "USD"
        # Убираем дубли
        data = data.drop_duplicates()
        # Отчетный период -> datetime
        data['Отчетный период'] = pd.to_datetime(data['Отчетный период'], format='%d.%m.%Y')
        # В общей структуре есть колонка "стоимость по ДЭИ" — здесь ее нет
        data["Значение (стоимость) - ДЭИ"] = np.nan

        return data

    # Основной парсинг Eurostat COMEXT (SDMX 3.0)
    def parse(self) -> pd.DataFrame:
        """
        Алгоритм:
        1) Удаляет временные CSV (EU_trade / EU_belarus_trade)
        2) Формирует список периодов YYYY-MM (в обратном порядке: от последнего года к первому, все 12 месяцев)
        3) Загружает список HS6 из ./data/eu_codes.csv (обязательный внешний файл)
        4) Для flow 1 и 2 (импорт/экспорт в терминах API) делает батчи HS6 по 50 кодов
        5) Дергает Eurostat API (json format), парсит через pyjstat -> DataFrame, поправляет PRODUCT через reverse-map
        6) Дописывает куски в временный CSV
        7) decor() приводит к итоговой таблице (и конвертирует EUR->USD)
        8) Удаляет временный CSV
        """

        EU_params = EU.EU_params.copy()
        countries = ','.join(self.countries)

        # Удаление временных CSV
        if os.path.exists(EU_params["CSV_PATH_EU"]) and not self.belarus:
            os.remove(EU_params["CSV_PATH_EU"])
        if os.path.exists(EU_params["CSV_PATH_EU_BELARUS"]) and self.belarus:
            os.remove(EU_params["CSV_PATH_EU_BELARUS"])

        # Формирование period (список YYYY-MM через запятую)
        period = []
        # ВАЖНО:
        # years строятся как range(int(last), int(first)-1, -1)
        # То есть ожидается, что self.years = ["2019", ..., "2025"] (по возрастанию).
        # Если передать наоборот, получится пустой/неправильный диапазон.
        years = list(map(str, range(int(self.years[-1]), int(self.years[0]) - 1, -1)))

        for year in years:
            for month in range(12, 0, -1):
                period.append(year + '-' + str(month).zfill(2))
        period = ','.join(period)

        # Европейские HS6 из локального файла ./data/eu_codes.csv
        hs6 = pd.read_csv("./data/eu_codes.csv")
        hs6 = hs6[hs6["code"].str.len() == 6].sort_values(by='code')
        hs6 = pd.Series(hs6["code"]).drop_duplicates().tolist()

        first_iter = True

        # flow: "1" и "2"
        # ВАЖНО:
        # В COMEXT flow: 1=IMPORT, 2=EXPORT.
        for ttype in ["1", "2"]:
            pb = tqdm(range(50, len(hs6), 50))
            for idx in pb:
                pb.set_description(f"Собрано HS6 (батчей по 50 шт), flow={ttype}")
                hs = ','.join(hs6[idx - 50:idx])

                # URL зависит от belarus:
                # - belarus=True: reporter=MD,UA и partner=BY
                # - иначе: reporter=ваш список стран и partner=self.partner (по умолчанию RU)
                if self.belarus:
                    url = (
                        "https://ec.europa.eu/eurostat/api/comext/dissemination/sdmx/3.0/data/"
                        "dataflow/ESTAT/ds-059341/1.0/*.*.*.*.*.*"
                        f"?c[freq]=M&c[reporter]=MD,UA&c[partner]=BY&c[product]={hs}&c[flow]={ttype}"
                        f"&c[indicators]=QUANTITY_KG,VALUE_EUR&c[TIME_PERIOD]={period}&compress=false&format=json&lang=en"
                    )
                else:
                    url = (
                        "https://ec.europa.eu/eurostat/api/comext/dissemination/sdmx/3.0/data/"
                        "dataflow/ESTAT/ds-059341/1.0/*.*.*.*.*.*"
                        f"?c[freq]=M&c[reporter]={countries}&c[partner]={self.partner}&c[product]={hs}&c[flow]={ttype}"
                        f"&c[indicators]=QUANTITY_KG,VALUE_EUR&c[TIME_PERIOD]={period}&compress=false&format=json&lang=en"
                    )

                # Переиспользуем while True для ретрая коннекта
                while True:
                    try:
                        response = requests.get(url).text
                        break
                    except ConnectionError:
                        continue

                # Парсинг ответа
                js = json.loads(response)
                prod_meta = js['dimension']['product']['category']

                # reverse map label -> code
                # Потому что pyjstat может вернуть PRODUCT как label, а не как числовой код.
                rev_map = {label: code for code, label in prod_meta['label'].items()}

                df = pyjstat.Dataset.read(response).write('dataframe')
                df['PRODUCT'] = df['PRODUCT'].map(rev_map)
                df = df[~df["value"].isna()]
                df = df.iloc[:, 1:]

                # Выбираем путь для CSV
                path = EU_params["CSV_PATH_EU"]
                if self.belarus:
                    path = EU_params["CSV_PATH_EU_BELARUS"]

                # Аппендим в CSV
                df.to_csv(
                    path,
                    index=False,
                    mode="a",
                    header=first_iter,
                    encoding="utf-8-sig"
                )
                first_iter = False

        print("Парсинг успешно завершен. Перехожу к составлению и оформлению итоговой таблицы.")

        # Приводим к итоговой таблице и конвертируем в USD
        df = self.decor()
        # Чистим временные CSV
        if os.path.exists(EU_params["CSV_PATH_EU"]) and not self.belarus:
            os.remove(EU_params["CSV_PATH_EU"])
        if os.path.exists(EU_params["CSV_PATH_EU_BELARUS"]) and self.belarus:
            os.remove(EU_params["CSV_PATH_EU_BELARUS"])
        return df