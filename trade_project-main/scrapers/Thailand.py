import pandas as pd
import numpy as np
from tqdm import tqdm
from fake_useragent import UserAgent
import os
import calendar
import requests


class Thailand:
    """
    Парсер внешней торговли Таиланда по стране-партнеру (RU) с портала tradereport.moc.go.th.

    Источник отдает данные через JSON API (POST запросы) по отчету:
      - exports: /stat/reporthscodeexport02/result
      - imports: /stat/reporthscodeimport02/result

    Общая логика:
      1) В parse(): перебираем направления (экспорт/импорт), годы, месяцы и HS2 (01..99).
         Для каждого HS2 запрашиваем таблицу на уровне HS6 (hscodedigits=6) и сохраняем строки в CSV.
      2) В decor(): приводим CSV к стандартной структуре проекта (период, HS коды, единицы, сортировка).
    """

    thai_params = {
        # URL для экспорта (для импорта будет заменен в parse())
        "url": "https://tradereport.moc.go.th/stat/reporthscodeexport02/result",
        # Временный CSV для сырых данных из API
        "CSV_PATH_THAI": "./data/thai_trade.csv",
        # Метод запроса к API
        "method": "POST",
        # Базовый payload запроса (меняется в parse(): year, month, hscode, иногда url/referer)
        "payload": {
            # Страна-партнер: RU
            "country": {"id": "345", "text": "RU : RUSSIAN FEDERATION"},
            # Валюта отчета: USD
            "currency": {"id": "usd", "text": "USD"},
            # Код товара (здесь используется как HS2 в цикле, заменяется в parse())
            "hscode": "30",
            # Уровень детализации (HS6)
            "hscodedigits": "6",
            # Язык
            "lang": "en",
            # Месяц (заменяется в parse())
            "month": {"id": "5", "text": "May."},
            # Сортировка
            "sort": {"id": "value_desc", "text": "Value (Descending)"},
            # Год (заменяется в parse())
            "year": {"id": "2025"}
        },

        # Заголовки запроса (User-Agent подставляем случайный)
        "headers": {
            "Accept": "application/json, text/plain, */*",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
            "Connection": "keep-alive",
            "Content-Type": "application/json",
            "Origin": "https://tradereport.moc.go.th",
            "Referer": "https://tradereport.moc.go.th/en/stat/reporthscodeexport02",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
            "sec-ch-ua": '"Not)A;Brand";v="8", "Chromium";v="138", "Google Chrome";v="138"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"macOS"', # !!!!!! Наверное надо заменить, мб и так сработает, я не знаю, на ваших компах
            "User-Agent": "",  # заполним useragent-ом в parse()
            "Host": "tradereport.moc.go.th"
        }
    }

    def decor(self):
        """
        Приведение сырых данных из thai_trade.csv к стандартному формату проекта:
          - формируем Отчетный период "01.MM.YYYY"
          - заполняем страны, направление, HS коды (2/4/6)
          - приводим типы, сортируем, оставляем нужные колонки
          - конвертируем период в datetime
        """
        # Читаем сырые данные, которые собрал parse()
        data = pd.read_csv(Thailand.thai_params["CSV_PATH_THAI"])

        # Приведение периода к формату "01.MM.YYYY"
        data['Отчетный период'] = "01." + data["Month"].astype(str).str.zfill(2) + '.' + data["Year"].astype(str)

        # Заполняем фиксированные поля
        data["Страна-партнер"] = "Россия"
        data["Исходная страна"] = "Таиланд"

        # HS-коды: из API приходит ID (ожидается HS6)
        data["Код товара (10 знаков)"] = np.nan
        data["Код товара (8 знаков)"] = np.nan
        data["Код товара (6 знаков)"] = data["ID"].astype(str).str.zfill(6)
        data["Код товара (2 знака)"] = data["Код товара (6 знаков)"].str[:2]
        data["Код товара (4 знака)"] = data["Код товара (6 знаков)"].str[:4]

        # Значения
        data["Значение (стоимость)"] = data["ValueMonth"]
        data["Значение (масса)"] = data["QuantityMonth"]

        # Единицы
        data["Единицы стоимости"] = "USD"
        data["Единица объема"] = data["unit"]

        # Поля, которых нет в источнике
        data["ДЭИ, описание"] = np.nan
        data["Дополнительная единица измерения (ДЭИ)"] = np.nan

        # Направление
        data["Направление"] = data["type"].replace({"imports": "Импорт", "exports": "Экспорт"})

        # Убираем нули/NaN по стоимости
        data = data[(data["Значение (стоимость)"] != 0) & (data["Значение (стоимость)"] != '0')]
        data = data[~data["Значение (стоимость)"].isna()]

        # Сортировка (свежие периоды наверху)
        data = data.sort_values(
            by=['Year', 'Month', 'Направление'],
            ascending=[False, False, True],
            key=lambda col: col.astype(int) if col.name in ['Year', 'Month'] else col,
            ignore_index=True
        )

        # Стандартные поля проекта
        data["Значение (стоимость) - ДЭИ"] = np.nan

        # Финальный порядок колонок
        data = data[[
            "Отчетный период", "Исходная страна", "Страна-партнер", "Направление",
            "Код товара (2 знака)", "Код товара (4 знака)", "Код товара (6 знаков)",
            "Код товара (8 знаков)", "Код товара (10 знаков)", "Значение (стоимость)",
            "Единицы стоимости", "Значение (масса)", "Единица объема",
            "Дополнительная единица измерения (ДЭИ)", "ДЭИ, описание", "Значение (стоимость) - ДЭИ"
        ]]

        # Приводим числовые типы
        data["Значение (стоимость)"] = data["Значение (стоимость)"].astype(float)
        data["Значение (масса)"] = data["Значение (масса)"].astype(float)

        # На всякий случай убираем дубли (иногда API может вернуть пересечения)
        data = data.drop_duplicates()

        # Отчетный период -> datetime
        data['Отчетный период'] = pd.to_datetime(data['Отчетный период'], format='%d.%m.%Y')
        return data

    def __init__(self, years, months=range(1, 13)):
        """
        years: список лет строками, например ["2019", "2020", ..., "2025"]
        months: iterable месяцев (по умолчанию 1..12)
        """
        self.years = years
        self.months = months

    def parse(self) -> pd.DataFrame:
        """
        Основной цикл парсинга:
          - удаляем старый CSV
          - ставим случайный User-Agent
          - для exports и imports:
              * подставляем нужный URL и Referer
              * перебираем годы и месяцы
              * перебираем HS2 (01..99), запрашиваем данные
              * фильтруем строки RowType == 'N' и пишем в CSV
          - после парсинга: decor() и возврат итоговой таблицы
        """
        # Чистим предыдущий CSV, чтобы не смешивать результаты
        if os.path.exists(Thailand.thai_params["CSV_PATH_THAI"]):
            os.remove(Thailand.thai_params["CSV_PATH_THAI"])
        # Генерируем “живой” User-Agent (часто помогает против блокировок)
        user = UserAgent()
        # Берем копию параметров, чтобы не мутировать “класс-словарь”
        thai_params = Thailand.thai_params.copy()
        # Ставим случайный UA в headers
        thai_params["headers"]["User-Agent"] = user.random
        first_iter = True  # header=True только для первой записи в CSV

        # Направления
        for ttype in ["exports", "imports"]:

            # Для импорта нужно переключить endpoint и referer
            if ttype == "imports":
                thai_params["url"] = "https://tradereport.moc.go.th/stat/reporthscodeimport02/result"
                thai_params["headers"]["Referer"] = "https://tradereport.moc.go.th/en/stat/reporthscodeimport02"

            # Годы (из self.years)
            for year in self.years:
                # API ожидает year как объект {"id": "..."}
                thai_params["payload"]["year"] = {"id": year}

                # Месяцы (из self.months)
                for month in self.months:
                    # API ожидает month как объект {"id": <int>, "text": "Month."}
                    thai_params["payload"]["month"] = {"id": month, "text": calendar.month_name[month] + '.'}

                    # Перебор HS2 (01..99): запросы делаются на каждом HS2
                    for hs6 in tqdm(
                        range(1, 100),
                        desc="Собрано HS2, " + ttype + ', ' + str(month).zfill(2) + '.' + year
                    ):
                        # Устанавливаем текущий HS2 (2 знака) в payload
                        thai_params["payload"]["hscode"] = str(hs6).zfill(2)

                        # Повторяем запрос при сетевых проблемах
                        while True:
                            try:
                                # POST запрос: на вход headers + json payload
                                res = requests.post(
                                    thai_params["url"],
                                    headers=thai_params["headers"],
                                    json=thai_params["payload"]
                                )

                                # Пробуем распарсить JSON (если не JSON — просто выходим из этого HS2)
                                try:
                                    res = res.json()
                                except:
                                    break

                                # Берем только “нормальные” строки (RowType == 'N')
                                records = [r for r in res['records'] if r.get('RowType') == 'N']
                                if not records:
                                    # Если строк нет — данных по HS2/месяцу нет
                                    break

                                # Формируем датафрейм из записей
                                df = pd.DataFrame(records)

                                # Пытаемся вытащить единицу из ProductName (текст в скобках)
                                df['unit'] = df['ProductName'].str.extract(r'\(([^)]+)\)')

                                # Добавляем метаданные
                                df["Year"] = year
                                df["type"] = ttype
                                df["Month"] = str(month).zfill(2)

                                # Оставляем минимально нужные поля
                                df = df[['Year', 'Month', 'ID', "type", 'ValueMonth', 'QuantityMonth', 'unit']]

                                # Пишем в CSV (append)
                                df.to_csv(
                                    thai_params["CSV_PATH_THAI"],
                                    mode="a",
                                    index=False,
                                    header=first_iter,
                                    encoding="utf-8-sig",
                                )
                                first_iter = False
                                break

                            except:
                                # Любая сетевая/временная ошибка — повторяем текущий HS2
                                print("Connection error, repeat HS2")
                                continue

        print("Парсинг успешно завершен. Перехожу к составлению и оформлению итоговой таблицы.")

        # Приводим к стандарту
        data = self.decor()
        # Тоже достаточно долгий парсер
        # if os.path.exists(thai_params["CSV_PATH_THAI"]):
        #     os.remove(thai_params["CSV_PATH_THAI"])
        return data
