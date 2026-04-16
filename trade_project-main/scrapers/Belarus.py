import os                 # Работа с путями/проверками существования папок и файлов
import shutil             # Удаление папок целиком (rmtree)
import requests           
import pandas as pd       
import numpy as np        
from tqdm import tqdm     
from CIS import CIS       # Локальный модуль-агрегатор (нужен файл CIS.py + его зависимости)

import warnings
warnings.filterwarnings('ignore', category=UserWarning)  # Глушим UserWarning (часто от pandas)

class Belarus:
    """
    Парсер торговли Беларуси (belstat.gov.by) + корректировка через "остаток" от СНГ.

    Что делает:
    1) Скачивает Excel exim8-YYMM.xlsx за каждый год (где YY — последние 2 цифры года)
    2) Из Excel вырезает блок по месяцам и берет 2 колонки: exports и imports
    3) Превращает данные в длинный формат (month, ttype, value, year)
    4) decor(): приводит к стандартной схеме проекта (период, страны, валюта, направление и т.д.)
    5) residual(): вычитает из полученных “тотальных” данных по России объем, посчитанный парсером CIS
       (идея: Беларусь->Россия = общий объем - (Беларусь->страны СНГ?)
    """

    belarus_params = {
        # Маппинг русских названий месяцев (именительный падеж) -> номер месяца
        "months_ru_nominative": {
            "Январь": 1, "Февраль": 2, "Март": 3, "Апрель": 4,
            "Май": 5, "Июнь": 6, "Июль": 7, "Август": 8,
            "Сентябрь": 9, "Октябрь": 10, "Ноябрь": 11, "Декабрь": 12
        },

        # Временная папка для скачанных excel
        "path": "./data/files",
        # Временный CSV, куда накапливаются строки (потом удаляется)
        "CSV_PATH_BEL": "./data/belarus_trade.csv"
    }

    def __init__(self, years):
        # years: список лет (строки), например ["2021","2022","2023","2024","2025"]
        self.years = years

        # params для CIS: передаем years и флаг belarus=True
        # (логика CIS зависит от этого флага)
        self.params = {"years": self.years, "belarus": True}

    # Приведение результата к общей структуре
    def decor(self):
        """
        Читает временный CSV Belarus (month/year/exports/imports),
        делает "Отчетный период", добавляет стандартные поля и возвращает DataFrame.
        """
        # Читаем накопленные данные из CSV
        data = pd.read_csv(Belarus.belarus_params["CSV_PATH_BEL"])
        # Формируем отчетный период (первое число месяца)
        data['Отчетный период'] = "01." + data["month"].astype(str).str.zfill(2) + '.' + data["year"].astype(str)
        # Здесь партнер всегда Россия (по логике парсера)
        data["Страна-партнер"] = "Россия"
        # Исходная страна Беларусь
        data["Исходная страна"] = "Беларусь"

        # HS-кодов нет — заполняем NaN
        data["Код товара (10 знаков)"] = np.nan
        data["Код товара (8 знаков)"] = np.nan
        data["Код товара (6 знаков)"] = np.nan
        data["Код товара (2 знака)"] = np.nan
        data["Код товара (4 знака)"] = np.nan
        # value умножается на 1 000 000 (в исходнике в млн)
        data["Значение (стоимость)"] = data["value"] * 1000000
        # ДЭИ/масса/объем отсутствуют
        data["Значение (стоимость) - ДЭИ"] = np.nan
        data["Значение (масса)"] = np.nan
        # Валюта
        data["Единицы стоимости"] = "USD"
        # Единица объема и ДЭИ пустые
        data["Единица объема"] = np.nan
        data["ДЭИ, описание"] = np.nan
        data["Дополнительная единица измерения (ДЭИ)"] = np.nan

        # Приводим направления к единому виду:
        # - exports -> Экспорт
        # - imports -> Импорт
        data["Направление"] = data["ttype"].replace({"imports": "Импорт", "exports": "Экспорт"})
        # Убираем нулевые значения (и числом 0, и строкой '0')
        data = data[(data["Значение (стоимость)"] != 0) & (data["Значение (стоимость)"] != '0')]
        # Убираем NaN стоимости
        data = data[~data["Значение (стоимость)"].isna()]
        # Сортировка: год/месяц убывание, направление — по алфавиту
        data = data.sort_values(
            by=['year', 'month', 'Направление'],
            ascending=[False, False, True],
            key=lambda col: col.astype(int) if col.name in ['year', 'month'] else col,
            ignore_index=True
        )
        data = data[data["Направление"] != "Всего"]
        data = data[data["Код товара (6 знаков)"] != "00000n"]

        # Оставляем стандартные колонки в нужном порядке
        data = data[[
            "Отчетный период", "Исходная страна", "Страна-партнер", "Направление",
            "Код товара (2 знака)", "Код товара (4 знака)", "Код товара (6 знаков)",
            "Код товара (8 знаков)", "Код товара (10 знаков)", "Значение (стоимость)",
            "Единицы стоимости", "Значение (масса)", "Единица объема",
            "Дополнительная единица измерения (ДЭИ)", "ДЭИ, описание", "Значение (стоимость) - ДЭИ"
        ]]

        # Отчетный период -> datetime
        data["Отчетный период"] = pd.to_datetime(data["Отчетный период"], format="%d.%m.%Y")

        return data

    # Остаток (вычитание CIS)
    def residual(self, data, CIS_data):
        """
        Вычисляет “остаток”:
        - агрегирует CIS_data по (Отчетный период, Направление)
        - вычитает полученную сумму из data["Значение (стоимость)"]

        Важно:
        - CIS_data должен иметь колонки: "Отчетный период", "Направление",
          "Значение (стоимость)", "Значение (стоимость) - ДЭИ".
        """
        print("Вычленим остаток")
        # Приводим стоимость к float (если вдруг прочиталось как str)
        CIS_data["Значение (стоимость)"] = CIS_data["Значение (стоимость)"].astype(float)
        # sum = стоимость + стоимость по ДЭИ (если есть). NaN в ДЭИ заменяем на 0
        CIS_data["sum"] = CIS_data["Значение (стоимость)"] + CIS_data["Значение (стоимость) - ДЭИ"].replace({np.nan: 0})
        # Агрегируем сумму по месяцу и направлению
        CIS_data_by_month = CIS_data.groupby(["Отчетный период", "Направление"], as_index=False)["sum"].sum()

        # ВАЖНО: жестко заданный интервал дат (2021-01-01 .. 2025-06-01)
        # Это место надо менять, когда появятся новые месяцы/годы !!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        start_date = pd.to_datetime("01.01.2021", format="%d.%m.%Y")
        end_date = pd.to_datetime("01.06.2025", format="%d.%m.%Y")
        CIS_data_by_month = CIS_data_by_month[
            (CIS_data_by_month["Отчетный период"] <= end_date) &
            (CIS_data_by_month["Отчетный период"] >= start_date)
        ]

        # Присоединяем суммы CIS к основным данным по (период, направление)
        data = pd.merge(data, CIS_data_by_month, on=["Отчетный период", "Направление"], how="inner")
        # Вычитаем CIS-сумму из белорусской стоимости
        data["Значение (стоимость)"] = data["Значение (стоимость)"] - data["sum"]
        # Убираем вспомогательную колонку
        data = data.drop(columns="sum")
        return data

    # Основной парсинг (скачать excel -> вытащить месяцы -> melt -> CSV -> decor -> residual)
    def parse(self):
        """
        Главный метод:
        - готовит временную папку ./data/files
        - по годам скачивает exim8-YYMM.xlsx
        - вырезает блок по месяцам и берет exports/imports
        - melt в длинный формат и дописывает в belarus_trade.csv
        - decor() приводит к стандарту
        - удаляет временные файлы
        - считает CIS и вычитает residual()
        """

        # 1) Готовим временную папку
        if os.path.exists(Belarus.belarus_params["path"]):
            shutil.rmtree(Belarus.belarus_params["path"])
        os.makedirs(Belarus.belarus_params["path"])

        # 2) Удаляем временный CSV, если он остался с прошлого запуска
        if os.path.exists(Belarus.belarus_params["CSV_PATH_BEL"]):
            os.remove(Belarus.belarus_params["CSV_PATH_BEL"])

        first_iter = True  # header=True только для первой записи
        month = "12"       # по умолчанию берем декабрьский файл за год (полный год)

        # 3) Идем по годам
        pb1 = tqdm(range(int(self.years[0]), int(self.years[-1]) + 1))
        for year in pb1:
            pb1.set_description(f"Собираю {year} год")

            # В 2025 (в коде) берем только файл до июня
            # ВАЖНО: это место надо менять, когда появится новый файл (например, 2025-12) !!!!!!!
            if year == 2025:
                month = "06"

            # URL: exim8-YYMM.xlsx
            url = f"https://www.belstat.gov.by/upload-belstat/upload-belstat-excel/Oficial_statistika/{year}/exim8-{str(year)[-2:]}{month}.xlsx"

            # Скачиваем excel
            with open(f"./data/files/{year}.xlsx", "wb") as file:
                file.write(requests.get(url).content)

            # Читаем excel в pandas
            data = pd.read_excel(f"./data/files/{year}.xlsx")

            # 4) Вырезаем нужный диапазон строк в зависимости от года.
            # Тут очень хрупко: завязано на наличие строк-меток в первом столбце:
            # - "страны СНГ"
            # - "страны вне СНГ"
            # - "Российская Федерация" (особый случай 2021)
            if year != 2021:
                data = data.iloc[
                    data[data[data.columns[0]] == "страны СНГ"].index[0]:
                    data[data[data.columns[0]] == "страны вне СНГ"].index[0]
                ]
            else:
                data = data.iloc[
                    data[data[data.columns[0]] == "страны СНГ"].index[0]:
                    data[data[data.columns[0]] == "Российская Федерация"].index[0]
                ]

            # 5) Чистим первый столбец от пробелов
            data[data.columns[0]] = data[data.columns[0]].str.strip()

            # 6) Оставляем только строки, где в первом столбце стоят названия месяцев
            data = data[data[data.columns[0]].isin(Belarus.belarus_params["months_ru_nominative"].keys())]

            # 7) Берем: (месяц) + две последние “суммарные” колонки
            # ВАЖНО: здесь берется срез data.columns[-3:-1] — это тоже хрупко.
            data = data[[data.columns[0]] + list(data.columns[-3:-1])]

            # 8) Переименовываем колонки:
            # - month (название месяца)
            # - exports / imports (две колонки)
            data = data.rename(columns={
                data.columns[0]: "month",
                data.columns[-2]: "exports",
                data.columns[-1]: "imports"
            })

            # 9) Переводим месяц "Январь" -> 1 и т.д.
            data["month"] = data["month"].replace(Belarus.belarus_params["months_ru_nominative"])

            # 10) Превращаем wide -> long:
            # на выходе: month | ttype (exports/imports) | value
            data = pd.melt(
                data,
                id_vars=['month'],
                value_vars=['exports', 'imports'],
                var_name='ttype',
                value_name='value'
            )

            # Добавляем год
            data["year"] = year
            # 11) Дописываем в временный CSV
            data.to_csv(
                Belarus.belarus_params["CSV_PATH_BEL"],
                mode='a',
                index=False,
                header=first_iter,
                encoding="utf-8-sig"
            )
            first_iter = False
        # 12) Приводим результат к стандарту
        data = self.decor()
        # 13) Чистим временные файлы
        if os.path.exists(Belarus.belarus_params["path"]):
            shutil.rmtree(Belarus.belarus_params["path"])
        if os.path.exists(Belarus.belarus_params["CSV_PATH_BEL"]):
            os.remove(Belarus.belarus_params["CSV_PATH_BEL"])
        # 14) Считаем торговлю Беларуси с СНГ (через CIS) и вычитаем ее
        print("Переходим к парсингу торговли Беларуси с СНГ")
        return self.residual(
            data=data,
            CIS_data=CIS(**self.params).parse()
        )
