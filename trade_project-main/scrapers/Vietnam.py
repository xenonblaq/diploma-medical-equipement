import PyPDF2
import numpy as np
import pandas as pd
import os
import calendar
from datetime import datetime
import time

import warnings
warnings.filterwarnings('ignore', category=FutureWarning)

import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
import requests
from tqdm import tqdm

import shutil


class Vietnam:
    """
    Парсер внешней торговли Вьетнама (агрегаты по странам) с сайта Vietnam Customs.

    Источник:
      - Страница со "Scheduled data (2009 - Year to date)" и PDF-отчетами по:
          "country/territory main imports (Month Year)"
          "country/territory main exports (Month Year)"

    Что делаем:
      1) parse():
         - через Selenium открываем страницу со списком PDF
         - для каждого направления (imports/exports), каждого года и месяца:
             * пытаемся найти ссылку на нужный PDF на текущей странице
             * если не нашли — кликаем "NextPage" и пробуем еще раз (до 2 страниц)
             * если все равно нет — ставим '-' (данных нет)
         - скачиваем PDF, парсим текст PyPDF2, ищем строку "Russian Federation"
         - вытаскиваем числовые значения (monthly / year_to_date) и сохраняем в список df
         - по завершении чистим временную папку ./files
      2) decor():
         - превращаем список df в DataFrame
         - пытаемся восстановить пропуски monthly/year_to_date из разностей
         - формируем общий формат проекта (Отчетный период, страны, направление, стоимость в USD)
         - сортируем и приводим период к datetime
    """

    vietnam_params = {
        # Домашняя страница (нужна, чтобы корректно "прогрелся" сайт/сессия)
        "url_1": "https://www.customs.gov.vn/index.jsp?ngon_ngu=en",
        # Страница со списком Scheduled data и ссылками на PDF
        "url_2": "https://www.customs.gov.vn/index.jsp?pageId=5002&group=undefined&category=Scheduled%20data%20(2009%20-%20Year%20to%20date)",
        # Словарь для сохранения найденных ссылок на PDF по направлениям/периодам
        "links_by_flow_and_year": {
            "exports": {},
            "imports": {},
        }
    }

    def __init__(self, years, version_main=142):
        """
        years: список лет строками, например ["2019","2020",...,"2025"]
        """
        self.years = years
        self.version_main = version_main

    def decor(self, df) -> pd.DataFrame:
        """
        Приведение сырых извлечений из PDF к единому формату проекта.

        На вход:
          df: список строк вида [monthly, year_to_date, period, ttype]
              где period = f"{month}{year}" (пример "12025" для Jan 2025),
              ttype ∈ {"imports","exports"}.
        """
        # Собираем DF из накопленного списка
        data = pd.DataFrame(df, columns=["monthly", "year_to_date", "period", "ttype"])

        # --- Восстановление пропусков ---
        # Если year_to_date пропущен, пытаемся восстановить его из соседних значений:
        # (берем предыдущий year_to_date и вычитаем предыдущий monthly)
        data["year_to_date"] = data["year_to_date"].where(
            data["year_to_date"].notna(),
            data["year_to_date"].shift(1) - data["monthly"].shift(1)
        )

        # Если monthly пропущен, пытаемся восстановить из разности year_to_date:
        # monthly = текущий ytd - следующий ytd
        data["monthly"] = data["monthly"].where(
            data["monthly"].notna(),
            data["year_to_date"] - data["year_to_date"].shift(-1)
        )

        # --- Разбор периода ---
        # period хранится как строка вида "12025" (месяц без нулей + год)
        data["Year"] = data["period"].str[-4:].astype(int)
        data["Month"] = data["period"].str[:1].astype(int)

        # Формируем стандартный период 01.MM.YYYY
        data['Отчетный период'] = (
            "01." + data["Month"].astype(str).str.zfill(2) + '.' + data["Year"].astype(str)
        )

        # Метаданные проекта
        data["Страна-партнер"] = "Россия"
        data["Исходная страна"] = "Вьетнам"

        # HS-разбивки нет — NaN
        data["Код товара (10 знаков)"] = np.nan
        data["Код товара (8 знаков)"] = np.nan
        data["Код товара (6 знаков)"] = np.nan
        data["Код товара (2 знака)"] = np.nan
        data["Код товара (4 знака)"] = np.nan

        # Стоимость = monthly (USD), остальные измерения отсутствуют
        data["Значение (стоимость)"] = data["monthly"]
        data["Значение (масса)"] = np.nan
        data["Единицы стоимости"] = "USD"
        data["Единица объема"] = np.nan
        data["ДЭИ, описание"] = np.nan
        data["Дополнительная единица измерения (ДЭИ)"] = np.nan

        # Перевод направления в термины проекта
        data["Направление"] = data["ttype"].replace({"imports": "Импорт", "exports": "Экспорт"})

        # Чистим нулевые/пустые значения стоимости
        data = data[(data["Значение (стоимость)"] != 0) & (data["Значение (стоимость)"] != '0')]
        data = data[~data["Значение (стоимость)"].isna()]

        # Сортировка: новые периоды сверху, Экспорт/Импорт внутри периода
        data = data.sort_values(
            by=['Year', 'Month', 'Направление'],
            ascending=[False, False, True],
            key=lambda col: col.astype(int) if col.name in ['Year', 'Month'] else col,
            ignore_index=True
        )

        # Финальная раскладка колонок по стандарту проекта
        data = data[[
            "Отчетный период", "Исходная страна", "Страна-партнер", "Направление",
            "Код товара (2 знака)", "Код товара (4 знака)", "Код товара (6 знаков)",
            "Код товара (8 знаков)", "Код товара (10 знаков)", "Значение (стоимость)",
            "Единицы стоимости", "Значение (масса)", "Единица объема",
            "Дополнительная единица измерения (ДЭИ)", "ДЭИ, описание"
        ]]

        # В этом источнике нет "стоимость - ДЭИ"
        data["Значение (стоимость) - ДЭИ"] = np.nan

        # Приводим период к datetime
        data['Отчетный период'] = pd.to_datetime(data['Отчетный период'], format='%d.%m.%Y')
        return data

    def parse(self) -> pd.DataFrame:
        """
        Основной метод сбора:
          - Сканирует таблицу ссылок на PDF по месяцам/годам, скачивает PDF
          - Извлекает строку по "Russian Federation" и парсит числовые значения
          - Возвращает финальный DataFrame в формате проекта
        """
        df = []
        # Копия словаря ссылок (на будущее: можно кэшировать найденные URL)
        links_by_flow_and_year = Vietnam.vietnam_params["links_by_flow_and_year"].copy()

        # Selenium-драйвер (undetected) — сайт может быть чувствителен к ботам
        driver = uc.Chrome(version_main=self.version_main)

        # Переходим на нужные страницы
        driver.get(Vietnam.vietnam_params["url_1"])
        driver.get(Vietnam.vietnam_params["url_2"])

        # Ждем, пока станет доступна навигация "FirstPage"
        WebDriverWait(driver, 60).until(
            EC.element_to_be_clickable((By.ID, "aFirstPage"))
        )

        # Два направления: импорт и экспорт
        for ttype in ["imports", "exports"]:
            # Идем по годам от последнего к первому (включительно)
            for year in range(int(self.years[-1]), int(self.years[0]) - 1, -1):

                # По месяцам с декабря к январю
                pb1 = tqdm(range(12, 0, -1))
                for month in pb1:
                    pb1.set_description(f"Собираю {month}.{year}, {ttype}")

                    # Не пытаемся брать будущие месяцы в 2025
                    if year == 2025 and month > int(datetime.now().strftime("%m")) - 1:
                        continue

                    # Пытаемся найти ссылку на PDF максимум на 2 страницах (текущая + next)
                    cnt = 0
                    while cnt != 2:
                        try:
                            # В некоторых месяцах ссылка может быть в 5-й ячейке строки
                            links_by_flow_and_year[f"{ttype}"][f"{month}{year}"] = (
                                driver.find_elements(
                                    By.XPATH,
                                    f"//td[contains(text(), 'country/territory main {ttype} ({calendar.month_name[month]} {year})')]/../td"
                                )[4]
                                .find_element(By.TAG_NAME, "a")
                                .get_attribute("href")
                            )
                            break
                        except:
                            try:
                                # Альтернативный индекс ячейки (3-я), если структура строки отличается
                                links_by_flow_and_year[f"{ttype}"][f"{month}{year}"] = (
                                    driver.find_elements(
                                        By.XPATH,
                                        f"//td[contains(text(), 'country/territory main {ttype} ({calendar.month_name[month]} {year})')]/../td"
                                    )[2]
                                    .find_element(By.TAG_NAME, "a")
                                    .get_attribute("href")
                                )
                                break
                            except:
                                # Если уже сделали один переход и снова не нашли — считаем, что ссылки нет
                                if cnt == 1:
                                    links_by_flow_and_year[f"{ttype}"][f"{month}{year}"] = '-'
                                    break

                                # Переходим на следующую страницу списка и пробуем еще раз
                                driver.find_element(By.ID, "aNextPage").click()
                                cnt += 1
                                time.sleep(3)
                                continue

                    # Создаем временную папку для PDF, если ее нет
                    if not os.path.exists("./files"):
                        os.makedirs("./files")

                    # Если ссылка найдена — скачиваем PDF и парсим
                    if links_by_flow_and_year[f"{ttype}"][f"{month}{year}"] != '-':
                        pdf_path = f"./files/{month}{year}.pdf"

                        # Скачиваем PDF
                        with open(pdf_path, "wb") as file:
                            file.write(
                                requests.get(links_by_flow_and_year[f"{ttype}"][f"{month}{year}"]).content
                            )

                        # Извлекаем текст со всех страниц PDF
                        text_lines = []
                        with open(pdf_path, "rb") as f:
                            reader = PyPDF2.PdfReader(f)
                            for page in reader.pages:
                                text = page.extract_text()
                                if text:
                                    text_lines.extend(text.splitlines())

                        # Ищем строку, где упоминается Russian Federation
                        index = next(i for i, line in enumerate(text_lines) if "Russian Federation" in line)

                        # Пробуем вытащить числа из этой строки
                        tmp = text_lines[index].strip().replace(',', '').split('  ')

                        # Иногда строка = только название страны, тогда берем несколько строк ниже
                        if tmp == ['Russian Federation']:
                            tmp = text_lines[index: index + 3]

                        # Чистим запятые
                        text_lines_clean = [i.replace(',', '') for i in tmp]

                        # Для старых лет (<=2021) формат может быть другой: разбиваем по пробелам
                        if year <= 2021:
                            text_lines_clean = text_lines_clean[0].split(' ')
                            # удаляем первый элемент (обычно страна/метка)
                            text_lines_clean.pop(0)

                        # Удаляем первый элемент (обычно "Russian Federation" или метку)
                        text_lines_clean.pop(0)

                        # Превращаем оставшееся в числа
                        values = list(map(float, text_lines_clean))

                        # Добавляем period и направление
                        values.append(f"{month}{year}")
                        values.append(f"{ttype}")

                        # Кладем в общий список
                        df.append(values)

                        # Удаляем PDF после обработки
                        os.remove(pdf_path)

                    else:
                        # Если ссылки нет — кладем NaN на monthly/year_to_date
                        df.append([np.nan, np.nan, f"{month}{year}", f"{ttype}"])

            # После завершения направления возвращаемся на первую страницу списка
            driver.find_element(By.ID, "aFirstPage").click()
            time.sleep(3)

        # Чистим временную папку целиком
        if os.path.exists("./files"):
            shutil.rmtree("./files")

        # Приводим в единый формат проекта
        return self.decor(df)
