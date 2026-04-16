import pandas as pd
import numpy as np
import os
import time
from tqdm import tqdm

import warnings
warnings.filterwarnings('ignore', category=FutureWarning) # игнорируем бесконечные предупреждения

import undetected_chromedriver as uc # Selenium с undetected оберткой и ниже его различные необходимые классы
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support.ui import Select

from selenium.common.exceptions import TimeoutException
from selenium.common.exceptions import NoSuchElementException


class Korea:
    korea_params = {
        # Главная страница сервиса tradeData Korea (англ. интерфейс)
        "url": "https://tradedata.go.kr/cts/index_eng.do#tabHsSgn1",

        # Временный CSV (сохраняем "сырые" страницы таблицы, потом decor() приведет к единому формату)
        "CSV_PATH_KOREA": "./data/korea_trade.csv",

        # Ожидаемые колонки в таблице на сайте (для парсинга HTML-таблицы)
        "df_columns": [
            "Period", "H.S Code", "Items", "Country",
            "Export Weight", "Export Value",
            "Import Weight", "Import Value",
            "Balance"
        ]
    } 

    def __init__(self, years: list, version_main=142):
        """
        years: список лет строками, например ["2019","2020","2021","2022","2023","2024","2025"].
        Также загружаем справочник HS из Excel (берем только колонку Code).
        """
        self.years = years
        # Версия хрома
        self.version_main = version_main
        self.hs4 = pd.read_excel("./data/HSCodeandDescription.xlsx")[["Code"]]

    def decor(self) -> pd.DataFrame:
        """
        Приведение сырого CSV (собранного Selenium-парсером) к единому формату проекта:
        - формируем 'Отчетный период'
        - выставляем страны
        - раскладываем HS6 -> HS2/HS4/HS6
        - определяем направление (Импорт/Экспорт) и раскладываем значения стоимости/массы
        - чистим, приводим типы, сортируем
        - переводим стоимость/массу в числовые значения (стоимость * 1000)
        """
        data = pd.read_csv(Korea.korea_params["CSV_PATH_KOREA"])
        # Убираем агрегированную строку TOTAL (она не нужна в детальной таблице)
        data = data[data["Period"] != "TOTAL"]
        # Период на сайте выглядит как YYYY-MM (или YYYYMM), здесь берется:
        #   - год: первые 4 символа
        #   - месяц: последние 2 символа
        # и собирается дата "01.MM.YYYY"
        data['Отчетный период'] = "01." + data['Period'].astype(str).str[-2:] + '.' + data['Period'].astype(str).str[:4]

        # Фиксируем страны для итогового датафрейма
        data["Страна-партнер"] = "Россия"
        data["Исходная страна"] = "Республика Корея"

        # HS-коды: на сайте выводится "H.S Code" (фактически HS6)
        data["Код товара (2 знака)"] = data["H.S Code"].str[:2]
        data["Код товара (4 знака)"] = data["H.S Code"].str[:4]
        data["Код товара (6 знаков)"] = data["H.S Code"]

        # HS8/HS10 у этого источника нет
        data["Код товара (8 знаков)"] = np.nan
        data["Код товара (10 знаков)"] = np.nan

        # Единицы измерения: стоимость в USD, масса/объем в тоннах
        data["Единицы стоимости"] = "USD"
        data["Единица объема"] = "тонн"

        # ДЭИ отсутствует (заполняем NaN)
        data["ДЭИ, описание"] = np.nan
        data["Дополнительная единица измерения (ДЭИ)"] = np.nan

        # Определяем направление:
        # логика такая же, как в других парсерах: если Export Value = 0 -> это импорт, если Import Value = 0 -> это экспорт.
        data["Export Value"] = data["Export Value"].astype(str)
        data["Import Value"] = data["Import Value"].astype(str)

        data.loc[(data["Export Value"] == '0') | (data["Export Value"] == '0.0'), "Направление"] = "Импорт"
        data.loc[(data["Import Value"] == '0') | (data["Import Value"] == '0.0'), "Направление"] = "Экспорт"

        # Если по строке одновременно есть и импорт, и экспорт (или направление не определилось),
        # "Направление" останется NaN. Тогда размножаем строку на две версии: экспорт и импорт.
        mask = (data['Направление'].isna())
        data_split = data[mask]
        data_rest = data[~mask]

        # Версия под экспорт: обнуляем импортные значения
        df_export = data_split.copy()
        df_export['Import Value'] = '0'
        df_export['Направление'] = 'Экспорт'

        # Версия под импорт: обнуляем экспортные значения
        df_import = data_split.copy()
        df_import['Export Value'] = '0'
        df_import['Направление'] = 'Импорт'

        # Склеиваем обратно
        data = pd.concat([data_rest, df_export, df_import], ignore_index=True)

        # Заполняем унифицированные поля "стоимость" и "масса" в зависимости от направления
        data.loc[data["Направление"] == "Импорт", "Значение (стоимость)"] = data["Import Value"]
        data.loc[data["Направление"] == "Экспорт", "Значение (стоимость)"] = data["Export Value"]

        data.loc[data["Направление"] == "Импорт", "Значение (масса)"] = data["Import Weight"]
        data.loc[data["Направление"] == "Экспорт", "Значение (масса)"] = data["Export Weight"]

        # Для сортировки выделяем год/месяц из Period
        data["Year"] = data['Period'].astype(str).str[:4]
        data["Month"] = data['Period'].astype(str).str[5:]

        # Сортировка: год/месяц по убыванию, направление стабильно
        data = data.sort_values(
            by=['Year', 'Month', 'Направление'],
            ascending=[False, False, True],
            key=lambda col: col.astype(int) if col.name in ['Year','Month'] else col,
            ignore_index=True
        )

        # Чистим числа: на сайте значения часто с запятыми, также стоимость обычно в тысячах USD
        data['Значение (стоимость)'] = data['Значение (стоимость)'].str.replace(',', '').astype(float) * 1000
        data['Значение (масса)'] = data['Значение (масса)'].str.replace(',', '').astype(float)

        # Еще раз фиксируем отсутствие HS8/HS10
        data['Код товара (8 знаков)'] = np.nan
        data['Код товара (10 знаков)'] = np.nan

        # Оставляем только нужные колонки и порядок
        data = data[[
            "Отчетный период", "Исходная страна", "Страна-партнер", "Направление",
            "Код товара (2 знака)", "Код товара (4 знака)", "Код товара (6 знаков)",
            "Код товара (8 знаков)", "Код товара (10 знаков)",
            "Значение (стоимость)", "Единицы стоимости", "Значение (масса)",
            "Единица объема", "Дополнительная единица измерения (ДЭИ)", "ДЭИ, описание"
        ]]

        # В проекте есть отдельная колонка для стоимости по ДЭИ (здесь ее нет)
        data["Значение (стоимость) - ДЭИ"] = np.nan

        # Делаем период datetime
        data['Отчетный период'] = pd.to_datetime(data['Отчетный период'], format='%d.%m.%Y')
        return data

    def parse(self) -> pd.DataFrame:
        """
        Selenium-парсер:
        1) Открывает сайт
        2) Переходит на вкладку "by H.S Code and Country"
        3) Батчами по 100 HS4 добавляет коды в селектор
        4) Выбирает страну Russia (один раз) и период (год)
        5) Запускает поиск, постранично собирает таблицу, пишет в CSV
        6) После каждого батча очищает список кодов (allDel)
        7) При Timeout обновляет страницу и повторяет текущий батч
        """
        opts = Options()
        opts.page_load_strategy = "eager"  # не ждем полной загрузки всех ресурсов, быстрее стартуем

        driver = uc.Chrome(version_main=self.version_main, options=opts)

        # Увеличенные таймауты, т.к. сайт может быть медленным
        driver.command_executor.set_timeout(300)
        driver.set_page_load_timeout(60)                         
        driver.set_script_timeout(60)

        wait = WebDriverWait(driver, 240)

        # Открываем страницу
        driver.get(Korea.korea_params["url"])

        # Переходим на вкладку "by H.S Code and Country"
        page = wait.until(EC.presence_of_element_located((By.XPATH, "//li/a[@title='by H.S Code and Country']")))
        driver.execute_script("arguments[0].click();", page)

        # Готовим список HS4:
        # берём из Excel, оставляем только 4-значные коды
        hs4 = self.hs4
        hs4 = hs4[hs4["Code"].str.len() == 4]
        hs4 = pd.Series(hs4["Code"]).tolist()

        # Небольшая пауза, чтобы интерфейс/скрипты сайта успели инициализироваться
        time.sleep(10)

        # Если CSV уже есть — удаляем, чтобы начать заново
        first_iter = True
        if os.path.exists(Korea.korea_params["CSV_PATH_KOREA"]):
            os.remove(Korea.korea_params["CSV_PATH_KOREA"])

        # Проходим по годам
        pbar = tqdm(self.years)
        for year in pbar:
            pbar.set_description(f"Парсим {year} год")

            # Батчим HS4 по 100 кодов (ограничение UI/производительности)
            for idx in tqdm(
                range(100, len(hs4), 100),
                total=len(hs4) // 100,
                desc="Собрано HS4 (батчей по 100 шт)",
                unit="batch"
            ):
                while True:
                    try:
                        # 1) Добавляем 100 HS4 кодов в список выбранных кодов на сайте
                        for i in range(idx - 100, idx):
                            hs6_input = wait.until(EC.element_to_be_clickable((By.ID, "ETS0200015Q_hsSgn")))
                            hs6_input.send_keys(hs4[i])

                            plus = driver.find_element(By.CLASS_NAME, "cateNumBtn")

                            # Ждем, что значение действительно появилось в инпуте, потом кликаем "+"
                            wait.until(EC.text_to_be_present_in_element_value((By.ID, "ETS0200015Q_hsSgn"), hs4[i]))
                            plus.click()

                            # Проверяем, что код попал в список выбранных
                            wait.until(EC.presence_of_element_located((
                                By.XPATH,
                                f'//ul[@id="selectdHsSgnList"]//p[@class="itemDel1" and text()="{hs4[i]}"]'
                            )))
                            hs6_input.clear()

                        # 2) Первичная настройка фильтров (делаем один раз):
                        # - включаем подуровни HS
                        # - выбираем страну Russia (RU)
                        if first_iter:
                            driver.find_element(By.ID, "ETS0200015Q_subHsSgn").click()
                            driver.find_element(By.ID, "ETS0200015Q_btnCou").click()
                            driver.find_element(By.ID, "ETS0200015Q_cntyNameInput").send_keys("Russia")
                            driver.find_element(By.XPATH, "//li[@value='RU']/input").click()
                            driver.find_element(By.CLASS_NAME, "foot_btn_app").click()

                        # 3) Выбираем периодичность MON (месячные данные)
                        driver.execute_script("arguments[0].click();", driver.find_element(By.XPATH, "//li/input[@value='MON']"))

                        # 4) Выбираем интервал дат:
                        # Для 2025 у сайта, судя по вашей логике, другой формат значений или список месяцев неполный.
                        if year != "2025":
                            select = Select(driver.find_element(By.ID, "ETS0200015Q_formYearMonthPc"))
                            select.select_by_value(f"{year}01 ")
                            select = Select(driver.find_element(By.ID, "ETS0200015Q_toYearMonthPc"))
                            select.select_by_value(f"{year}12 ")
                        else:
                            select = Select(driver.find_element(By.ID, "ETS0200015Q_formYearMonthPc"))
                            select.select_by_value(f"{year}01")
                            select = Select(driver.find_element(By.ID, "ETS0200015Q_toYearMonthPc"))
                            # select_by_index(0) — обычно "последний доступный месяц"
                            select.select_by_index(0)

                        # 5) Запускаем поиск
                        driver.find_element(By.CLASS_NAME, "btnSearch").click()

                        # Ждем исчезновения лоадера
                        wait.until(EC.invisibility_of_element_located((By.CSS_SELECTOR, ".loadingWrap")))

                        # 6) Ставим отображение по 100 строк на страницу
                        select = Select(driver.find_element(By.ID, "ETS0200015Q_showPagingLine"))
                        select.select_by_value("100")

                        # 7) Определяем количество страниц по пагинации
                        pages_num = len(driver.find_elements(By.XPATH, "//ul[@class='pagination_lst']/li"))

                        # 8) Проходим по страницам и вытаскиваем таблицу
                        for page_idx in range(1, pages_num + 1):
                            try:
                                page = driver.find_element(By.XPATH, f"//ul[@class='pagination_lst']/li[{page_idx}]")
                            except NoSuchElementException:
                                continue

                            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", page)
                            driver.execute_script("arguments[0].click();", page)

                            wait.until(EC.invisibility_of_element_located((By.CSS_SELECTOR, ".loadingWrap")))

                            # Достаём текст таблицы JS-скриптом (быстрее, чем много find_elements)
                            table_rows = driver.execute_script(
                                """
                                const rows = [...arguments[0].querySelectorAll('tr')];
                                return rows
                                .map(r => [...r.querySelectorAll('th,td')]
                                            .map(c => c.textContent.replace(/\\u00A0/g,' ').trim())
                                            .filter(Boolean))
                                .filter(r => r.length && !/^total:/i.test(r[0]));
                                """,
                                driver.find_element(By.CSS_SELECTOR, "table"),
                            )

                            # table_rows[0..1] — обычно шапка/служебные строки, поэтому берём с [2:]
                            data = pd.DataFrame(table_rows[2:], columns=Korea.korea_params["df_columns"])

                            # Сохраняем в CSV (в режиме append)
                            data.to_csv(
                                Korea.korea_params["CSV_PATH_KOREA"],
                                mode="a",
                                index=False,
                                header=first_iter,
                                encoding="utf-8-sig",
                            )
                            first_iter = False

                        # 9) Очищаем выбранные HS коды перед следующим батчем
                        driver.find_element(By.CLASS_NAME, "allDel").click()
                        break  # выходим из while True (батч успешно обработан)

                    except TimeoutException:
                        # Если сайт завис на "Loading", обновляем страницу и возвращаемся на нужную вкладку,
                        # после чего повторяем текущий батч (while True).
                        print("Слишком долгий 'Loading'. Обновляю страницу и заново собираю текущие 100 кодов.")
                        driver.refresh()
                        page = wait.until(EC.presence_of_element_located((By.XPATH, "//li/a[@title='by H.S Code and Country']")))
                        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", page)
                        driver.execute_script("arguments[0].click();", page)
                        time.sleep(5)

        print("Парсинг успешно завершен. Перехожу к составлению и оформлению итоговой таблицы.")
        data = self.decor()

        # Опять же долгий парсер, на всякий случай сохраняем
        # if os.path.exists(Korea.korea_params["CSV_PATH_KOREA"]):
        #     os.remove(Korea.korea_params["CSV_PATH_KOREA"])

        return data
