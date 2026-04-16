import pandas as pd
import numpy as np
import os
import shutil
import requests
from bs4 import BeautifulSoup


class Kyrgyzstan:

    kg_params = {
        # Страница стат.комитета КР, где лежат публикации/файлы по взаимной торговле с ЕАЭС
        "url": "https://www.stat.gov.kg/ru/publications/vzaimnaya-torgovlya-tovarami-kyrgyzskoj-respubliki-s-gosudarstvami-chlenami-evrazijskogo-ekonomicheskogo-soyuza/",
        # Временный CSV, куда складываем результаты парсинга по годам (потом decor() приводит к единому формату)
        "CSV_PATH_KG": "./data/kg_trade.csv"
    }

    def __init__(self, years=["2025"], belarus=False, final_month=10):
        # years: список лет строками, например ["2019","2020",...,"2025"]
        self.years = years
        # belarus: если True — берем торговлю с Беларусью, иначе — с Россией
        self.belarus = belarus
        self.final_month = final_month # сколько месяцев сейчас доступно

    def decor(self):
        """
        Приведение сырого CSV (который накапливается в parse()) к единому формату проекта:
        - разворачиваем годовые итоги на помесячные (делим значения на число месяцев)
        - собираем 'Отчетный период' в формате 01.MM.YYYY
        - проставляем страны, HS-коды, валюту и единицы
        - чистим нули/NaN, сортируем, приводим дату к datetime
        """

        def years_to_months(
            df: pd.DataFrame,
            year_col: str = "year",
            numeric_cols=("usd", "quantity"),
        ):
            """
            В исходных Excel по КР часто лежат НЕ помесячные данные, а итог за год.
            Эта функция:
            1) Для каждого года создает 12 (или 9 для 2025) строк — по месяцам.
            2) Делит числовые показатели (usd, quantity) на число месяцев,
               чтобы получить "среднемесячное" значение.
            ВАЖНО: это именно равномерное распределение по месяцам, а не реальные месячные данные.
            """
            df = df.copy()

            # Приводим числовые колонки, где "-" считаем пропуском
            for c in numeric_cols:
                df[c] = pd.to_numeric(df[c].replace("-", np.nan), errors="coerce")

            # Год приводим к Int (с поддержкой NA)
            df[year_col] = pd.to_numeric(df[year_col], errors="coerce").astype("Int64")

            # Для 2025 предполагается, что доступно final_month месяцев, иначе 12
            def months_for_year(y):
                if pd.isna(y):
                    return []
                y = int(y)
                return list(range(1, self.final_month)) if y == 2025 else list(range(1, 13))

            months_lists = df[year_col].apply(months_for_year)
            repeat_counts = months_lists.apply(len)

            # Размножаем строки: одна строка года -> N строк по месяцам
            out = df.loc[df.index.repeat(repeat_counts)].reset_index(drop=True)

            # Плоские массивы месяцев и лет под развернутую таблицу
            months_flat = (
                np.concatenate(months_lists.values) if repeat_counts.sum() > 0 else np.array([], dtype=int)
            )
            years_flat = np.repeat(df[year_col].to_numpy(), repeat_counts)

            out["month"] = months_flat
            out[year_col] = years_flat

            # Делим значения на кол-во месяцев в году (12 или 9)
            divisors = np.repeat(repeat_counts.to_numpy(), repeat_counts)
            for c in numeric_cols:
                out[c] = out[c] / divisors

            return out

        # Читаем накопленный в parse() CSV и разворачиваем годы в месяцы
        data = years_to_months(pd.read_csv(Kyrgyzstan.kg_params["CSV_PATH_KG"]))

        # Формируем отчетный период как дата "01.MM.YYYY"
        data["Отчетный период"] = (
            "01." + data["month"].astype(str).str.zfill(2) + "." + data["year"].astype(str)
        )

        # Партнер: Россия по умолчанию, Беларусь если belarus=True
        if self.belarus:
            data["Страна-партнер"] = "Беларусь"
        else:
            data["Страна-партнер"] = "Россия"

        # Исходная страна фиксирована
        data["Исходная страна"] = "Киргизия"

        # В источнике используются 4-значные коды (несмотря на название hs6)
        data["Код товара (10 знаков)"] = np.nan
        data["Код товара (8 знаков)"] = np.nan
        data["Код товара (6 знаков)"] = np.nan
        data["Код товара (4 знака)"] = data["hs6"].astype(str).str.zfill(4)
        data["Код товара (2 знака)"] = data["Код товара (4 знака)"].str[:2]

        # Стоимость (уже в USD), масса отсутствует
        data["Значение (стоимость)"] = data["usd"]
        data["Значение (масса)"] = np.nan

        data["Единицы стоимости"] = "USD"
        data["Единица объема"] = np.nan

        # ДЭИ и количество (quantity) берём из источника
        data["ДЭИ, описание"] = data["unit"]
        data["Дополнительная единица измерения (ДЭИ)"] = data["quantity"]

        # Направление из parse(): "Экспорт" / "Импорт"
        data["Направление"] = data["ttype"]

        # Убираем нулевые/пустые значения стоимости
        data = data[(data["Значение (стоимость)"] != 0) & (data["Значение (стоимость)"] != "0")]
        data = data[~data["Значение (стоимость)"].isna()]

        # Сортировка по году/месяцу убыв., направление стабильно
        data = data.sort_values(
            by=["year", "month", "Направление"],
            ascending=[False, False, True],
            key=lambda col: col.astype(int) if col.name in ["year", "month"] else col,
            ignore_index=True,
        )

        # Убираем агрегаты/мусорные коды
        data = data[data["Направление"] != "Всего"]
        data = data[data["Код товара (4 знака)"] != "000n"]

        # Приводим к единому набору колонок в проекте
        data = data[
            [
                "Отчетный период",
                "Исходная страна",
                "Страна-партнер",
                "Направление",
                "Код товара (2 знака)",
                "Код товара (4 знака)",
                "Код товара (6 знаков)",
                "Код товара (8 знаков)",
                "Код товара (10 знаков)",
                "Значение (стоимость)",
                "Единицы стоимости",
                "Значение (масса)",
                "Единица объема",
                "Дополнительная единица измерения (ДЭИ)",
                "ДЭИ, описание",
            ]
        ]

        # В проекте есть отдельная колонка под стоимость по ДЭИ — здесь ее нет
        data["Значение (стоимость) - ДЭИ"] = np.nan

        # Приводим период к datetime
        data['Отчетный период'] = pd.to_datetime(data['Отчетный период'], format='%d.%m.%Y')

        # Чистим временный CSV, чтобы не оставлять следы парсинга
        if os.path.exists(Kyrgyzstan.kg_params["CSV_PATH_KG"]):
            os.remove(Kyrgyzstan.kg_params["CSV_PATH_KG"])

        return data

    def parse(self) -> pd.DataFrame:
        """
        Парсер:
        1) Скачивает Excel-файлы по годам со страницы публикации (кнопки "Скачать").
        2) Для каждого года читает 2 листа: экспорт и импорт (таб.2 и таб.3).
        3) У разных лет разная структура заголовков -> отдельная логика rename/iloc.
        4) Фильтрует строки по стране-партнеру (Россия/Беларусь).
        5) Пишет сырой результат в CSV, затем decor() приводит к финальному виду.
        """
        # Если есть старый CSV — удаляем
        if os.path.exists(Kyrgyzstan.kg_params["CSV_PATH_KG"]):
            os.remove(Kyrgyzstan.kg_params["CSV_PATH_KG"])

        # Собираем ссылки "год -> ссылка на xlsx"
        years_links = {}
        response = requests.get(Kyrgyzstan.kg_params["url"]).text
        soup = BeautifulSoup(response, "lxml")
        links = soup.find_all(name="a", attrs={"title": "Скачать"})
        for link in links:
            # Из текста ссылки берем год (последние 4 цифры перед "г.")
            years_links[link.text[: link.text.find("г.")][-4:]] = "https://www.stat.gov.kg" + link.get("href")

        first_iter = True

        # Проходим по нужному диапазону лет
        for year in range(int(self.years[0]), int(self.years[-1]) + 1):

            # Временная папка для скачанного файла
            if not os.path.exists("./files"):
                os.makedirs("./files")

            # Скачиваем xlsx за год
            with open(f"./files/{year}.xlsx", "wb") as file:
                file.write(requests.get(years_links[f"{year}"]).content)

            # В файле два листа: экспорт и импорт
            for ttype in ["Экспорт", "Импорт"]:
                if ttype == "Экспорт":
                    # Таблица экспорта (4 знака)
                    df = pd.read_excel(f"./files/{year}.xlsx", sheet_name="таб.2-Экспорт-ЕАЭС(4 зн) ")
                else:
                    # Таблица импорта (4 знака)
                    df = pd.read_excel(f"./files/{year}.xlsx", sheet_name="таб.3-Импорт-ЕАЭС(4зн) ")

                # До 2023 структура заголовков другая (именованные длинные колонки)
                if year < 2023:
                    if ttype == "Экспорт":
                        df = df.rename(
                            columns={
                                'Таблица 2: Экспорт товаров из Кыргызской  Республики в разрезе "товар-страна ЕАЭС" на уровне 4 знаков  ТН ВЭД ЕАЭС': "hs6",
                                "Unnamed: 1": "country",
                                "Unnamed: 2": "unit",
                                "Unnamed: 3": "quantity",
                                "Unnamed: 5": "usd",
                            }
                        )
                    else:
                        df = df.rename(
                            columns={
                                'Таблица 3:  Импорт товаров  в  Кыргызскую  Республику  в разрезе "товар-страна ЕАЭС" на уровне 4 знаков  ТН ВЭД ЕАЭС': "hs6",
                                "Unnamed: 1": "country",
                                "Unnamed: 2": "unit",
                                "Unnamed: 3": "quantity",
                                "Unnamed: 5": "usd",
                            }
                        )
                    # Пропускаем служебные строки шапки
                    df = df.iloc[6:]
                else:
                    # С 2023 структура проще: нужные поля лежат в Unnamed:0..5
                    df = df.rename(
                        columns={
                            "Unnamed: 0": "hs6",
                            "Unnamed: 1": "country",
                            "Unnamed: 2": "unit",
                            "Unnamed: 3": "quantity",
                            "Unnamed: 5": "usd",
                        }
                    )
                    # Пропускаем служебные строки
                    df = df.iloc[7:]

                # Оставляем только нужные колонки
                df = df[["hs6", "country", "unit", "quantity", "usd"]]

                # hs6 и unit тянем вниз (в таблице они часто заполнены только в первой строке блока)
                df["hs6"].fillna(method="ffill", inplace=True)
                df["unit"].fillna(method="ffill", inplace=True)

                # Фильтруем по партнеру
                if self.belarus:
                    df = df[df["country"] == "Беларусь"]
                else:
                    df = df[df["country"] == "Россия"]

                # Добавляем признаки направления и года
                df["ttype"] = ttype
                df["year"] = year

                # Стоимость в файлах в тысячах USD -> умножаем на 1000
                df["usd"] = df["usd"] * 1000

                # Пишем в сырой CSV
                df.to_csv(
                    Kyrgyzstan.kg_params["CSV_PATH_KG"],
                    mode="a",
                    header=first_iter,
                    index=False,
                    encoding="utf-8-sig",
                )
                first_iter = False

            # Удаляем временную папку с xlsx
            shutil.rmtree("./files/")

        # Дальше приводим сырой CSV к финальному виду и возвращаем готовый датафрейм
        return self.decor()
