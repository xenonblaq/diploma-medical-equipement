import pandas as pd
import numpy as np
import os
import requests
from bs4 import BeautifulSoup
from pathlib import Path
from tqdm import tqdm
import subprocess
import shutil

class Kazakhstan:
    kaz_params = {
        # Страница с архивами по внешней торговле
        "url": "https://stat.gov.kz/ru/industries/economy/foreign-market/spreadsheets/?year=&name=40108&period=&type=",

        # Куда временно складываем скачанные .rar и распакованные папки/файлы
        "KAZ_PATH": "./data/",

        # Временный CSV, который собирается в parse() и затем читается в decor()
        "CSV_PATH_KAZ": "./data/kaz_trade.csv"
    }

    def __init__(self, years, belarus=False):
        # belarus=True означает: парсим торговлю Казахстана с Беларусью,
        # иначе — с Россией (логика фильтрации в parse()/decor() зависит от этого)
        self.belarus = belarus

        # years — список лет строками, например ["2021","2022","2023","2024","2025"]
        # используется для выбора архива в цикле (через counter) и в итоговой таблице
        self.years = years

    def decor(self):
        """
        Приводит сырой временный CSV (собранный parse()) к единому формату проекта:
        - формирует 'Отчетный период'
        - выставляет страны (исходная/партнер)
        - нормализует HS6 -> HS2/HS4
        - собирает Импорт/Экспорт из двух колонок (Export Dollar/Import Dollar)
        - чистит нули/NaN
        - приводит типы, умножает стоимость на 1000 (если единица в тыс. USD)
        - возвращает итоговый DataFrame в нужном порядке колонок
        """

        # Читаем временный CSV, накопленный в parse()
        data = pd.read_csv(Kazakhstan.kaz_params["CSV_PATH_KAZ"])

        # Собираем отчетный период в виде "01.MM.YYYY"
        data['Отчетный период'] = "01." + data['Month'].astype(str).str.zfill(2) + '.' + data["Year"].astype(str)

        # Партнер зависит от режима парсинга
        if self.belarus:
            data["Страна-партнер"] = "Беларусь"
        else:
            data["Страна-партнер"] = "Россия"

        # Исходная страна фиксированная
        data["Исходная страна"] = "Казахстан"

        # Нормализуем HS6:
        # иногда код может читаться как "0101.21" и т.п., поэтому убираем точку и добиваем нулями
        data["Код товара (6 знаков)"] = data["Код товара (6 знаков)"].astype(str).str.replace('.', '')
        data["Код товара (6 знаков)"] = data["Код товара (6 знаков)"].str.zfill(6)

        # HS2/HS4 получаем из HS6
        data["Код товара (2 знака)"] = data["Код товара (6 знаков)"].str[:2]
        data["Код товара (4 знака)"] = data["Код товара (6 знаков)"].str[:4]

        # Единицы по источнику: стоимость в USD, масса/объем в тоннах
        data["Единицы стоимости"] = "USD"
        data["Единица объема"] = "тонн"

        # Определяем направление.
        # В исходных таблицах экспорт и импорт могут лежать в разных колонках:
        # если Export Dollar == 0, то это строка импорта (и наоборот).
        data["Export Dollar"] = data["Export Dollar"].astype(str)
        data["Import Dollar"] = data["Import Dollar"].astype(str)

        # Если экспортной стоимости нет/0 -> считаем это импортом
        data.loc[
            (data["Export Dollar"] == '0') | (data["Export Dollar"] == '0.0') |
            (data["Export Dollar"] == '')  | (data["Export Dollar"] == 'nan'),
            "Направление"
        ] = "Импорт"

        # Если импортной стоимости нет/0 -> считаем это экспортом
        data.loc[
            (data["Import Dollar"] == '0') | (data["Import Dollar"] == '0.0') |
            (data["Import Dollar"] == '')  | (data["Import Dollar"] == 'nan'),
            "Направление"
        ] = "Экспорт"

        # Бывает случай, когда по строке есть и экспорт и импорт одновременно
        # (или направление не определилось). Тогда "Направление" = NaN.
        # Для таких строк мы размножаем строку на 2:
        #  - одна версия будет "Экспорт" (Import Dollar = 0)
        #  - другая версия будет "Импорт" (Export Dollar = 0)
        mask = (data['Направление'].isna())
        data_split = data[mask]
        data_rest = data[~mask]

        # Клон под экспорт
        df_export = data_split.copy()
        df_export['Import Dollar'] = '0'
        df_export['Направление'] = 'Экспорт'

        # Клон под импорт
        df_import = data_split.copy()
        df_import['Export Dollar'] = '0'
        df_import['Направление'] = 'Импорт'

        # Склеиваем обратно
        data = pd.concat([data_rest, df_export, df_import], ignore_index=True)

        # Заполняем унифицированные поля "Значение (стоимость)/(масса)/ДЭИ" в зависимости от направления
        data.loc[data["Направление"] == "Импорт", "Значение (стоимость)"] = data["Import Dollar"]
        data.loc[data["Направление"] == "Экспорт", "Значение (стоимость)"] = data["Export Dollar"]

        data.loc[data["Направление"] == "Импорт", "Значение (масса)"] = data["Import quantity 1"]
        data.loc[data["Направление"] == "Экспорт", "Значение (масса)"] = data["Export quantity 1"]

        data.loc[data["Направление"] == "Импорт", "Дополнительная единица измерения (ДЭИ)"] = data["Import quantity 2"]
        data.loc[data["Направление"] == "Экспорт", "Дополнительная единица измерения (ДЭИ)"] = data["Export quantity 2"]

        # Фильтруем мусор: нули/пустые/NaN по стоимости
        data = data[
            (data["Значение (стоимость)"] != 0) &
            (data["Значение (стоимость)"] != '0') &
            (data["Значение (стоимость)"] != '0.0') &
            (data["Значение (стоимость)"] != 'nan')
        ]
        data = data[~data["Значение (стоимость)"].isna()]

        # Приводим типы и масштабы:
        # стоимость * 1000
        data["Значение (стоимость)"] = data["Значение (стоимость)"].astype(float) * 1000
        data["Значение (масса)"] = data["Значение (масса)"].astype(float)
        data["Дополнительная единица измерения (ДЭИ)"] = data["Дополнительная единица измерения (ДЭИ)"].astype(float)

        # Нормализуем коды HS
        data["Код товара (2 знака)"] = data["Код товара (2 знака)"].astype(str).str.zfill(2)
        data["Код товара (4 знака)"] = data["Код товара (4 знака)"].astype(str).str.zfill(4)
        data["Код товара (6 знаков)"] = data["Код товара (6 знаков)"].astype(str).str.zfill(6)

        # HS8/HS10 в этом источнике нет
        data["Код товара (8 знаков)"] = np.nan
        data["Код товара (10 знаков)"] = np.nan

        # Сортируем по убыванию Year/Month и стабильному порядку направления
        data = data.sort_values(
            by=['Year', 'Month', 'Направление'],
            ascending=[False, False, True],
            key=lambda col: col.astype(int) if col.name in ['Year', 'Month'] else col,
            ignore_index=True
        )

        # Поле "стоимость по ДЭИ" отсутствует
        data["Значение (стоимость) - ДЭИ"] = np.nan

        # Оставляем строго нужные колонки и порядок
        data = data[[
            "Отчетный период", "Исходная страна", "Страна-партнер", "Направление",
            "Код товара (2 знака)", "Код товара (4 знака)", "Код товара (6 знаков)",
            "Код товара (8 знаков)", "Код товара (10 знаков)", "Значение (стоимость)",
            "Единицы стоимости", "Значение (масса)", "Единица объема",
            "Дополнительная единица измерения (ДЭИ)", "ДЭИ, описание", "Значение (стоимость) - ДЭИ"
        ]]

        # Приводим период к datetime
        data['Отчетный период'] = pd.to_datetime(data['Отчетный период'], format='%d.%m.%Y')
        return data    

    def parse(self) -> pd.DataFrame:
        """
        1) Скачивает страницу со списком архивов (rar) от stat.gov.kz
        2) Собирает ссылки на архивы
        3) По очереди скачивает архив, распаковывает
        4) В распакованных папках ищет файлы "таб_9_00*"
        5) Читает Excel, вырезает блок по нужной стране (RU или BY) и сохраняет в временный CSV
        6) После цикла вызывает decor() и возвращает финальный DataFrame
        """

        # Забираем HTML страницы
        response = requests.get(url=Kazakhstan.kaz_params["url"]).text
        soup = BeautifulSoup(response, "lxml")

        # На странице ссылки лежат в div.divTableCell -> a[href]
        elements = soup.find_all("div", class_="divTableCell")

        # Собираем href, удаляем дубли через dict.fromkeys
        archive_links = list(dict.fromkeys([
            el.find("a").get("href")
            for el in elements
            if el.find("a") and el.find("a").get("href") != ''
        ]))

        # Флаг для записи заголовков в CSV только один раз
        first_iter = True

        # counter = -1 означает старт с последнего элемента self.years:
        # self.years[-1] -> первый в обработке (обычно самый свежий год)
        counter = -1

        # Если временный CSV уже есть — удаляем
        if os.path.exists(Kazakhstan.kaz_params["CSV_PATH_KAZ"]):
            os.remove(Kazakhstan.kaz_params["CSV_PATH_KAZ"])

        # Идем по архивам с прогресс-баром
        pbar = tqdm(archive_links)
        for url in pbar:

            # Ограничиваемся количеством лет из self.years
            # abs(counter) растет: -1, -2, -3 ...
            if abs(counter) > len(self.years):
                break

            # Обновляем текст прогресса — какой год сейчас парсим
            pbar.set_description(f"Парсим {self.years[counter]} год")

            # Скачиваем rar в ./data/kaz_trade_YYYY.rar
            rar_path = os.path.join(
                Kazakhstan.kaz_params["KAZ_PATH"],
                f"kaz_trade_{self.years[counter]}.rar"
            )
            with open(rar_path, "wb") as file:
                response = requests.get("https://stat.gov.kz" + url)
                file.write(response.content)

            # Запоминаем список папок ДО распаковки, чтобы понять, какая новая папка появилась
            base = Path(Kazakhstan.kaz_params["KAZ_PATH"])
            before = set(p.name for p in base.iterdir() if p.is_dir())

            # Распаковываем rar (bsdtar должен быть установлен) !!!!!!!!!!!!!!!!!!!
            subprocess.run(
                ["bsdtar", "-xf", rar_path, "-C", Kazakhstan.kaz_params["KAZ_PATH"]],
                check=True
            )

            # Список папок ПОСЛЕ распаковки
            after = set(p.name for p in base.iterdir() if p.is_dir())

            # Находим новую папку, появившуюся после распаковки
            new_dirs = after - before
            unpacked_folder = base / new_dirs.pop()

            # Создаем служебную папку, куда соберем все "таб_9_00*" из подпапок
            dst_dir = base / f"tab_9_00_collection_{self.years[counter]}"
            dst_dir.mkdir(exist_ok=True)

            # Внутри распакованной папки обычно много подпапок (по месяцам/разделам)
            # Копируем из каждой подпапки файлы "таб_9_00*" в одну коллекцию
            for subdir in unpacked_folder.iterdir():
                if not subdir.is_dir():
                    continue
                for file in subdir.glob("таб_9_00*"):
                    # Чтобы не перезаписывались одинаковые имена, добавляем префикс подпапки
                    new_name = f"{subdir.name}_{file.name}"
                    shutil.copy2(file, dst_dir / new_name)

            # Чистим: удаляем распакованную "сырую" папку и сам rar
            shutil.rmtree(unpacked_folder)
            os.remove(rar_path)

            # Теперь читаем все Excel из dst_dir
            excel_files = base / f"tab_9_00_collection_{self.years[counter]}"
            for excel_path in excel_files.glob("*.xls*"):

                # Читаем Excel
                df = pd.read_excel(excel_path)

                # Вырезаем блок строк по стране:
                # - для Russia: между "РОССИЯ" и "БЕЛАРУСЬ"
                # - для Belarus: между "БЕЛАРУСЬ" и "АРМЕHИЯ" (как в исходном файле)
                if not self.belarus:
                    df = df.iloc[
                        df[df['Unnamed: 1'] == "РОССИЯ"].index[0] + 1:
                        df[df['Unnamed: 1'] == "БЕЛАРУСЬ"].index[0]
                    ]
                else:
                    df = df.iloc[
                        df[df['Unnamed: 1'] == "БЕЛАРУСЬ"].index[0] + 1:
                        df[df['Unnamed: 1'] == "АРМЕHИЯ"].index[0]
                    ]

                # Переименовываем колонки в понятные имена
                df = df.rename(columns={
                    df.columns[0]: "Код товара (6 знаков)",
                    "Unnamed: 2": "ДЭИ, описание",
                    "Unnamed: 3": "Export quantity 1",
                    "Unnamed: 4": "Export quantity 2",
                    "Unnamed: 5": "Export Dollar",
                    "Unnamed: 6": "Import quantity 1",
                    "Unnamed: 7": "Import quantity 2",
                    "Unnamed: 8": "Import Dollar"
                })

                # Проставляем год
                df["Year"] = self.years[counter]

                # Месяц вытаскивается из пути excel_path через срез строки
                df["Month"] = str(excel_path)[30: 32]

                # Пишем в общий CSV по всем файлам/месяцам
                df.to_csv(
                    Kazakhstan.kaz_params["CSV_PATH_KAZ"],
                    mode="a",
                    index=False,
                    header=first_iter,
                    encoding="utf-8-sig",
                )
                first_iter = False

            # Удаляем папку с собранными excel за этот год и двигаемся к следующему году
            shutil.rmtree(excel_files)
            counter -= 1

        print("Парсинг успешно завершен. Перехожу к составлению и оформлению итоговой таблицы.")

        # Приводим к финальному формату
        data = self.decor()

        # Чистим временный CSV
        if os.path.exists(Kazakhstan.kaz_params["CSV_PATH_KAZ"]):
            os.remove(Kazakhstan.kaz_params["CSV_PATH_KAZ"])

        return data
