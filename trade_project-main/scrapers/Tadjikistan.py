import pandas as pd
import numpy as np
import os
import shutil
import subprocess
import requests
from zipfile import ZipFile, BadZipFile
from pathlib import Path

import warnings
warnings.filterwarnings('ignore', category=UserWarning)


class Tadjikistan:
    """
    Парсер внешней торговли Таджикистана.
    Источник: сайт таможни Таджикистана (tamognia.tj), данные по кварталам.
    Логика:
      1) Скачиваем архивы по каждому кварталу (kv1..kv4) для каждого года.
      2) Извлекаем Excel и читаем лист "Таблица-7" (с вариациями имени листа).
      3) Фильтруем строки по стране-партнеру (RU или BY).
      4) Складываем все квартальные данные во временный CSV.
      5) decor(): распределяет квартальные значения по месяцам (делит на 3), приводит к единому формату проекта.
    """

    taj_params = {
        # Временный CSV, в который накапливаем “сырые” квартальные данные перед декором
        "CSV_PATH_TAJ": "./data/taj_trade.csv"
    }

    def __init__(self, years=["2025"], belarus=False):
        """
        years: список лет строками, например ["2019","2020",...,"2025"] или ["2025"]
        belarus: если True — берем партнера BY, иначе RU
        """
        self.years = years
        self.belarus = belarus

    def decor(self):
        """
        Приведение “квартальных” данных к помесячным и к стандартной структуре проекта.

        В исходных файлах Таджикистана значения идут поквартально (kv = 'YYYYkvQ').
        Здесь мы:
          - раскладываем каждый квартал на 3 месяца (равномерно делим показатели на 3),
          - собираем 'Отчетный период' в виде "01.MM.YYYY",
          - строим HS-коды и унифицируем колонки (стоимость/масса/ДЭИ/направление).
        """

        def quarters_to_months(
            df: pd.DataFrame,
            kv_col: str = "kv",
            numeric_cols=("usd", "kg", "quantity"),
        ):
            """
            Перевод квартальных данных в помесячные:
              - из kv (формат: '2025kv1') вытаскиваем год и номер квартала
              - каждому кварталу соответствует 3 месяца: 1-3, 4-6, 7-9, 10-12
              - каждую строку повторяем 3 раза (на 3 месяца)
              - числовые показатели делим на 3 (равномерное распределение по месяцам)
            """
            df = df.copy()

            # Чистим числовые поля: '-' -> NaN -> numeric
            for c in numeric_cols:
                df[c] = pd.to_numeric(df[c].replace("-", np.nan), errors="coerce")

            # Парсим kv: 'YYYYkvQ' -> year, q
            yq = df[kv_col].str.extract(r'(?P<year>\d{4})kv(?P<q>[1-4])').astype({"year": int, "q": int})
            df = df.join(yq)

            # Стартовые месяцы кварталов
            start_map = {1: 1, 2: 4, 3: 7, 4: 10}

            # Для каждого квартала получаем список месяцев (3 месяца)
            months_lists = df["q"].map(start_map).apply(lambda m: [m, m + 1, m + 2])
            # Для каждого квартала год повторяется трижды
            years_lists = df["year"].apply(lambda y: [y, y, y])

            # Повторяем каждую строку 3 раза
            out = df.loc[df.index.repeat(3)].reset_index(drop=True)

            # Разворачиваем списки месяцев/лет в плоские массивы
            out["month"] = np.concatenate(months_lists.to_numpy())
            out["year"] = np.concatenate(years_lists.to_numpy())

            # Делим квартальные значения на 3, чтобы получить “условно помесячные”
            for c in numeric_cols:
                out[c] = out[c] / 3

            return out

        # Читаем накопленный временный CSV и раскладываем кварталы на месяцы
        data = quarters_to_months(pd.read_csv(Tadjikistan.taj_params["CSV_PATH_TAJ"]))

        # Формируем отчетный период
        data['Отчетный период'] = "01." + data["month"].astype(str).str.zfill(2) + '.' + data["year"].astype(str)

        # Партнер: RU или BY
        if self.belarus:
            data["Страна-партнер"] = "Беларусь"
        else:
            data["Страна-партнер"] = "Россия"

        # Заполняем “шапку” данных
        data["Исходная страна"] = "Таджикистан"

        # HS-коды: в файле hs6, судя по коду, последние 2 символа “лишние” -> отрезаем
        data["Код товара (10 знаков)"] = np.nan
        data["Код товара (8 знаков)"] = np.nan
        data["Код товара (6 знаков)"] = data["hs6"].astype(str).str[:-2].str.zfill(6)
        data["Код товара (2 знака)"] = data["Код товара (6 знаков)"].str[:2]
        data["Код товара (4 знака)"] = data["Код товара (6 знаков)"].str[:4]

        # Значения и единицы
        data["Значение (стоимость)"] = data["usd"]
        data["Значение (масса)"] = data["kg"]
        data["Единицы стоимости"] = "USD"
        data["Единица объема"] = "килограмм"

        # ДЭИ (доп. единица измерения)
        data["ДЭИ, описание"] = data["unit"]
        data["Дополнительная единица измерения (ДЭИ)"] = data["quantity"]

        # Направление (импорт/экспорт)
        data["Направление"] = data["ttype"].replace({"ИМПОРТ": "Импорт", "ЭКСПОРТ": "Экспорт"})

        # Убираем нули/NaN по стоимости
        data = data[(data["Значение (стоимость)"] != 0) & (data["Значение (стоимость)"] != '0')]
        data = data[~data["Значение (стоимость)"].isna()]

        # Сортировка: новые даты вверх
        data = data.sort_values(
            by=['year', 'month', 'Направление'],
            ascending=[False, False, True],
            key=lambda col: col.astype(int) if col.name in ['year', 'month'] else col,
            ignore_index=True
        )

        # Убираем агрегаты
        data = data[data["Направление"] != "Всего"]
        data = data[data["Код товара (6 знаков)"] != "00000n"]

        # Финальный набор колонок проекта
        data = data[[
            "Отчетный период", "Исходная страна", "Страна-партнер", "Направление",
            "Код товара (2 знака)", "Код товара (4 знака)", "Код товара (6 знаков)",
            "Код товара (8 знаков)", "Код товара (10 знаков)", "Значение (стоимость)",
            "Единицы стоимости", "Значение (масса)", "Единица объема",
            "Дополнительная единица измерения (ДЭИ)", "ДЭИ, описание"
        ]]

        # Запасная колонка проекта — здесь не рассчитывается
        data["Значение (стоимость) - ДЭИ"] = np.nan

        # Приводим период к datetime
        data['Отчетный период'] = pd.to_datetime(data['Отчетный период'], format='%d.%m.%Y')

        # Чистим временный CSV
        if os.path.exists(Tadjikistan.taj_params["CSV_PATH_TAJ"]):
            os.remove(Tadjikistan.taj_params["CSV_PATH_TAJ"])

        return data

    def parse(self) -> pd.DataFrame:
        """
        Парсинг данных по годам и кварталам.
        Для каждого (year, kv):
          - скачиваем архив (zip, а для 2019 — rar)
          - распаковываем в ./files
          - ищем Excel и читаем лист “Таблица-7” (с несколькими вариантами названия)
          - приводим колонки к унифицированным именам (в зависимости от года/квартала меняется структура)
          - фильтруем по стране (RU или BY)
          - добавляем kv=YYYYkvQ и сохраняем в CSV_PATH_TAJ
        После завершения вызываем decor() и возвращаем финальный DataFrame.
        """

        # Удаляем старый временный CSV, если остался
        if os.path.exists(Tadjikistan.taj_params["CSV_PATH_TAJ"]):
            os.remove(Tadjikistan.taj_params["CSV_PATH_TAJ"])

        first_iter = True  # флаг для заголовка CSV при первой записи

        # Перебираем годы в указанном диапазоне (years = ["2019",...,"2025"])
        for year in range(int(self.years[0]), int(self.years[-1]) + 1):
            # Перебираем кварталы 1..4
            for kv in range(1, 5):

                # Создаем временную папку под распаковку
                if not os.path.exists("./files"):
                    os.makedirs("./files")

                # В 2019 данные лежат в rar, в остальные годы — в zip
                prefix = "zip"
                if year == 2019:
                    prefix = "rar"

                # Скачиваем архив квартала
                with open(f"./files/{year}kv{kv}.{prefix}", "wb") as file:

                    if year == 2019:
                        # 2019: rar-архив, распаковываем bsdtar
                        file.write(requests.get(
                            f"https://tamognia.tj/images/stories/img_text/Omor/statistika/Stat vnesh torg/{year}kv{kv}.rar"
                        ).content)
                        subprocess.run(
                            ["bsdtar", "-xf", f"./files/{year}kv{kv}.rar", "-C", './files'],
                            check=True
                        )

                    else:
                        # 2020+ : zip-архив
                        file.write(requests.get(
                            f"https://tamognia.tj/images/stories/img_text/Omor/statistika/Stat vnesh torg/{year}kv{kv}.zip"
                        ).content)

                        # Пытаемся распаковать zip; если архив битый — пропускаем
                        try:
                            with ZipFile(f"./files/{year}kv{kv}.zip", "r") as zf:
                                zf.extractall("./files/")
                        except BadZipFile:
                            os.remove(f"./files/{year}kv{kv}.zip")
                            continue

                # В разные годы структура распаковки разная:
                # - 2019-2023: файлы оказываются прямо в ./files/
                # - 2024+ чаще распаковывается в подпапку ./files/YYYYkvQ/
                if year in [2019, 2020, 2021, 2022, 2023]:
                    base = Path(f"./files/")
                else:
                    base = Path(f"./files/{year}kv{kv}/")

                # Находим Excel-файлы и читаем нужный лист.
                # Названия листов гуляют, поэтому много try/except.
                for files in base.glob("*.xls*"):
                    try:
                        df = pd.read_excel(files, sheet_name="Таблица-7")
                    except ValueError:
                        try:
                            df = pd.read_excel(files, sheet_name="Таблица7")
                        except ValueError:
                            try:
                                df = pd.read_excel(files, sheet_name="таблица_7")
                            except ValueError:
                                try:
                                    df = pd.read_excel(files, sheet_name="Табл.7")
                                except ValueError:
                                    try:
                                        df = pd.read_excel(files, sheet_name="Таблица 7")
                                    except ValueError:
                                        df = pd.read_excel(files, sheet_name="Таб.7")

                # Структура столбцов зависит от года/квартала:
                # в некоторых файлах есть kg, в некоторых — нет (тогда проставляем '-')
                if (year not in [2019, 2020, 2021, 2022, 2023]) and (year != 2024 and kv != 4):
                    # “новая” структура: usd/kg/quantity лежат в Unnamed:10/11/12
                    df = df.rename(columns={
                        "Экспорт и импорт Республики Таджикистан": "country",
                        "Unnamed: 3": "ttype",
                        "Unnamed: 4": "hs6",
                        "Unnamed: 5": "unit",
                        "Unnamed: 10": "usd",
                        "Unnamed: 11": "kg",
                        "Unnamed: 12": "quantity"
                    })
                else:
                    # “старая” структура: kg отсутствует, quantity в Unnamed:9, usd в Unnamed:10
                    df = df.rename(columns={
                        "Экспорт и импорт Республики Таджикистан": "country",
                        "Unnamed: 3": "ttype",
                        "Unnamed: 4": "hs6",
                        "Unnamed: 5": "unit",
                        "Unnamed: 10": "usd",
                        "Unnamed: 9": "quantity"
                    })
                    df["kg"] = '-'  # массы нет — ставим заглушку

                # Срезаем “шапку” таблицы (служебные строки)
                df = df.iloc[5:]

                # Оставляем только нужные колонки
                df = df[["country", "ttype", "hs6", "unit", "usd", "kg", "quantity"]]

                # Протягиваем country/ttype вниз (в исходнике они часто даны блоками)
                df["country"].fillna(method='ffill', inplace=True)
                df["ttype"].fillna(method='ffill', inplace=True)

                # Маркер квартала (потом нужен для превращения в месяцы)
                df["kv"] = f"{year}kv{kv}"

                # Фильтруем партнера: BY или RU (в файле коды стран)
                if self.belarus:
                    df = df[df["country"] == "BY"]
                else:
                    df = df[df["country"] == "RU"]

                # Убираем “Всего” по hs6
                df = df[df["hs6"] != "Всего"]

                # Пишем в накопительный CSV
                df.to_csv(
                    Tadjikistan.taj_params["CSV_PATH_TAJ"],
                    mode='a',
                    header=first_iter,
                    index=False,
                    encoding="utf-8-sig"
                )
                first_iter = False

                # Удаляем временную папку распаковки, чтобы не копить мусор
                shutil.rmtree(f"./files/")

        # После сбора всех кварталов — декорируем и возвращаем итоговую таблицу
        return self.decor()
