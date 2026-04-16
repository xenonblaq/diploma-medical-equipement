import numpy as np
import pandas as pd
import os
from pathlib import Path
import time
from tqdm import tqdm

import warnings
warnings.filterwarnings('ignore', category=FutureWarning) 

import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from selenium.common.exceptions import TimeoutException
from selenium.common.exceptions import WebDriverException
from selenium.common.exceptions import SessionNotCreatedException
from urllib3.exceptions import ReadTimeoutError

from bs4 import BeautifulSoup
import requests

class Mexico:
    """
    Парсер внешней торговли Мексики (Banxico / Cubo Comercio Exterior).
    Логика:
      1) Через Selenium скачиваем Excel-выгрузки по стоимости (USD) и по объему (Volumen).
      2) Складываем их во временные CSV (USD и KG).
      3) decor() разворачивает таблицу (melt), мержит USD+KG и приводит к общему формату проекта.
    """

    mexico_params = {
        # Страница Banxico с таблицей по объемам (Volumen)
        "url_volumen": "https://www.banxico.org.mx/CuboComercioExterior/Volumen/seriesproducto",
        # Страница Banxico с таблицей по стоимости в долларах (Valor en dolares)
        "url_dolares": "https://www.banxico.org.mx/CuboComercioExterior/ValorDolares/seriesproducto",
        # Временный CSV под выгрузку стоимости (USD)
        "CSV_PATH_MEXICO_USD": "./data/mexico_trade_usd.csv",
        # Временный CSV под выгрузку объема/веса (Volumen)
        "CSV_PATH_MEXICO_KG": "./data/mexico_trade_kg.csv",
    }

    def decor(self):
        """
        Приведение временных выгрузок (USD + Volumen) к единому формату проекта:
          - читаем 2 CSV (стоимость и объем)
          - превращаем “широкий формат” месяцев в “длинный” (unpivot/melt)
          - мержим USD и KG по year/month/hs10/ttype
          - собираем стандартные колонки: Отчетный период, страны, HS-коды, стоимость/масса и т.д.
          - сортируем и приводим период к datetime
        """
        # Читаем сырые выгрузки (до приведения к формату проекта)
        data_usd = pd.read_csv(Mexico.mexico_params["CSV_PATH_MEXICO_USD"])
        data_kg = pd.read_csv(Mexico.mexico_params["CSV_PATH_MEXICO_KG"])

        # Маппинг названий месяцев на испанском в номер месяца
        month_map = {
            'Enero': 1, 'Febrero': 2, 'Marzo': 3, 'Abril': 4,
            'Mayo': 5, 'Junio': 6, 'Julio': 7, 'Agosto': 8,
            'Septiembre': 9, 'Octubre': 10, 'Noviembre': 11, 'Diciembre': 12
        }

        def unpivot(df: pd.DataFrame) -> pd.DataFrame:
            """
            Banxico Excel обычно приходит как “широкая таблица”:
              TIGIE | ... | Enero 2024 | Febrero 2024 | ...
            Эта функция превращает ее в “длинный формат”:
              year | month | hs10 | ttype | value
            """
            df = df.copy()

            # melt: колонки месяцев -> строки
            df_long = df.melt(
                id_vars=['TIGIE', 'ttype'],     # что оставляем как идентификаторы
                var_name='month_year',          # имя колонки с "Месяц Год"
                value_name='value'              # значение
            )

            # Разделяем "Enero 2024" -> month_name="Enero", year="2024"
            df_long[['month_name', 'year']] = (
                df_long['month_year'].str.split(pat=' ', n=1, expand=True)
            )
            df_long['year'] = df_long['year'].astype(int)

            # Переводим название месяца в номер
            df_long['month'] = df_long['month_name'].map(month_map)

            # TIGIE содержит товарный код; берем первые 10 символов как hs10
            df_long['hs10'] = df_long['TIGIE'].str[:10]

            return df_long[['year', 'month', 'hs10', 'ttype', 'value']]

        # Делаем long-таблицы по USD и по Volumen
        long_usd = unpivot(data_usd)
        long_kg = unpivot(data_kg)

        # Мержим две длинные таблицы: value_x = USD, value_y = KG/Volumen
        data = pd.merge(long_usd, long_kg, how="inner", on=["year", "month", "hs10", "ttype"])

        # Чистим пустые значения стоимости
        data = data[~data['value_x'].isna()]

        # Убираем строки, где hs10 заканчивается на "-" (обычно это агрегаты/неполные коды)
        data = data[data["hs10"].str[-1] != '-']

        # Собираем отчетный период в формате "01.MM.YYYY"
        data['Отчетный период'] = "01." + data["month"].astype(str).str.zfill(2) + '.' + data["year"].astype(str)

        # Проставляем страны
        data["Страна-партнер"] = "Россия"
        data["Исходная страна"] = "Мексика"

        # Собираем HS-коды разных разрядностей из hs10
        data["Код товара (10 знаков)"] = data["hs10"].astype(str).str.zfill(10)
        data["Код товара (8 знаков)"] = data["Код товара (10 знаков)"].str[:8]
        data["Код товара (6 знаков)"] = data["Код товара (10 знаков)"].str[:6]
        data["Код товара (2 знака)"] = data["Код товара (10 знаков)"].str[:2]
        data["Код товара (4 знака)"] = data["Код товара (10 знаков)"].str[:4]

        # Значения: стоимость из USD-таблицы, масса/объем из Volumen-таблицы
        data["Значение (стоимость)"] = data["value_x"]
        data["Значение (масса)"] = data["value_y"]

        data["Единицы стоимости"] = "USD"
        data["Единица объема"] = np.nan
        data["ДЭИ, описание"] = np.nan
        data["Дополнительная единица измерения (ДЭИ)"] = np.nan

        # Направление: imports/exports -> Импорт/Экспорт
        data["Направление"] = data["ttype"].replace({"imports": "Импорт", "exports": "Экспорт"})

        # Убираем нули и NaN по стоимости
        data = data[(data["Значение (стоимость)"] != 0) & (data["Значение (стоимость)"] != '0')]
        data = data[~data["Значение (стоимость)"].isna()]

        # Сортируем: сначала новые годы/месяцы, затем направление
        data = data.sort_values(
            by=['year', 'month', 'Направление'],
            ascending=[False, False, True],
            key=lambda col: col.astype(int) if col.name in ['year', 'month'] else col,
            ignore_index=True
        )

        # Оставляем только стандартный набор колонок проекта
        data = data[[
            "Отчетный период", "Исходная страна", "Страна-партнер", "Направление",
            "Код товара (2 знака)", "Код товара (4 знака)", "Код товара (6 знаков)",
            "Код товара (8 знаков)", "Код товара (10 знаков)", "Значение (стоимость)",
            "Единицы стоимости", "Значение (масса)", "Единица объема",
            "Дополнительная единица измерения (ДЭИ)", "ДЭИ, описание"
        ]]

        # В проекте предусмотрена колонка “стоимость по ДЭИ” — здесь ее нет
        data["Значение (стоимость) - ДЭИ"] = np.nan

        # Приводим период к datetime
        data['Отчетный период'] = pd.to_datetime(data['Отчетный период'], format='%d.%m.%Y')

        return data

    def __init__(self, years, version_main=142):
        # years: список лет строками, например ["2019","2020",...,"2025"]
        self.years = years
        self.version_main = version_main # Версия хрома

    def parse(self):
        """
        Основной парсинг:
          - находит рабочий прокси (через free-proxy-list)
          - открывает Banxico в Chrome (undetected_chromedriver) через этот прокси
          - для каждой метрики (USD, Volumen) и направления (exports/imports):
              * выбирает фильтры в интерфейсе (периоды, регионы, РФ, уровень детализации “продукты”)
              * сохраняет Excel через контекстное меню (“quick print to excel”)
              * читает Excel и сохраняет в CSV (USD или KG)
          - после выгрузок вызывает decor(), удаляет временные CSV и возвращает итоговый df
        """
        mexico_params = Mexico.mexico_params.copy()

        # Удаляем старые временные CSV, если они остались от прошлого запуска
        if os.path.exists(mexico_params["CSV_PATH_MEXICO_KG"]):
            os.remove(mexico_params["CSV_PATH_MEXICO_KG"])
        if os.path.exists(mexico_params["CSV_PATH_MEXICO_USD"]):
            os.remove(mexico_params["CSV_PATH_MEXICO_USD"])

        # По умолчанию начинаем с страницы “стоимость в долларах”
        TARGET = mexico_params["url_dolares"]

        # Папка, куда Chrome будет скачивать Excel !!!!!!!!!!!!!!! ЗАМЕНИТЬ НА СВОЙ ПУТЬ ДО data
        DOWNLOAD_DIR = Path("/Users/ivanandreev/Desktop/sber/structured/data")
        DOWNLOAD_DIR.mkdir(exist_ok=True)

        # Настройки автоскачивания файлов без подтверждений
        prefs = {
            "download.default_directory": str(DOWNLOAD_DIR),
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True
        }

        # Берем список прокси со страницы free-proxy-list
        proxies = BeautifulSoup(requests.get("https://free-proxy-list.net/ru/").text, "lxml")
        proxies = proxies.find_all(name="tr")
        proxies_list = [
            proxies[i].find_all(name='td')[0].text + ':' + proxies[i].find_all(name='td')[1].text
            for i in range(1, 100)
        ]
        best_proxy = None

        # Две метрики: ValorDolares (USD) и Volumen (объем)
        for valor in ["ValorDolares", "Volumen"]:
            if valor == "Volumen":
                print("Переходим к сбору статистики по объему")
                TARGET = mexico_params["url_volumen"]

            # 1) Поиск рабочего прокси
            for proxy in tqdm(proxies_list, desc="Ищем прокси"):
                if best_proxy:
                    # если уже нашли хороший прокси — повторно используем его
                    proxy = best_proxy

                dead_proxy_flag = False

                # Несколько попыток проверить прокси и открыть страницу
                for tries in range(3):
                    try:
                        # Пробуем открыть страницу простым requests через прокси
                        r = requests.get(
                            TARGET,
                            proxies={"http": f"http://{proxy}", "https": f"http://{proxy}"},
                            headers={"User-Agent": "Mozilla/5.0"},
                            timeout=60,
                        )
                        print("Прокси найден:", proxy)

                        # Конфиг Chrome: прокси + автоскачивания
                        opts = uc.ChromeOptions()
                        opts.add_argument(f"--proxy-server={proxy}")
                        opts.add_experimental_option("prefs", prefs)

                        # Иногда uc.Chrome падает из-за версии/сессии — делаем повтор
                        while True:
                            try:
                                driver = uc.Chrome(version_main=self.version_main, options=opts, use_subprocess=True)
                                break
                            except SessionNotCreatedException:
                                continue

                        # Таймауты webdriver
                        driver.command_executor.set_timeout(300)
                        driver.set_page_load_timeout(300)
                        driver.set_script_timeout(300)

                        try:
                            driver.get(TARGET)

                            # Ждем загрузки интерфейса (узел "Periodos")
                            for tries in range(3):
                                try:
                                    WebDriverWait(driver, 30).until(
                                        EC.presence_of_element_located((By.XPATH, "//div[@title='Periodos']"))
                                    )
                                    dead_proxy_flag = False
                                    print("Подключение успешно.")
                                    break
                                except TimeoutException:
                                    # если не дождались — пробуем refresh, на 3й попытке считаем прокси “мертвым”
                                    if tries == 2:
                                        print("Прокси мертв. Продолжаем поиск.")
                                    dead_proxy_flag = True
                                    driver.refresh()
                                    continue
                            break

                        except (WebDriverException, ReadTimeoutError):
                            # если webdriver не может открыть страницу — прокси плохой
                            dead_proxy_flag = True
                            print("Прокси мертв. Продолжаем поиск.")
                            best_proxy = None
                            driver.close()
                            break

                    except ConnectionResetError:
                        # прокси живой, но рвет соединение — пробуем еще
                        print("Прокси работает, но соединение разорвано. Попробуем еще раз")
                        continue
                    except OSError:
                        # иногда на уровне ОС сетевые ошибки
                        best_proxy = None
                        r = None
                        break

                # Если requests ок и интерфейс в Selenium поднялся — фиксируем прокси
                if r and r.ok and not dead_proxy_flag:
                    best_proxy = proxy
                    break

            # 2) Выгрузка по направлениям: exports / imports
            first_iter = True
            for ttype in ["exports", "imports"]:

                # БЛОК IMPORTS: выбираем импорт и сразу сохраняем Excel
                if ttype == "imports":
                    # Переходим в “Importación” в дереве “Tipo Operación”
                    if valor == "Volumen":
                        WebDriverWait(driver, 300).until(
                            EC.element_to_be_clickable((By.XPATH, "(//div[contains(text(), 'Exportac')])[2]"))
                        ).click()
                    else:
                        WebDriverWait(driver, 300).until(
                            EC.element_to_be_clickable((By.XPATH, "//div[contains(text(), 'Exportac')]"))
                        ).click()

                    driver.find_element(By.ID, "node_[Tipo Operación].[Tipos de Operación].[Importación]").click()
                    time.sleep(10)

                    # Скачивание Excel через контекстное меню
                    save_flag = False
                    while not save_flag:
                        time.sleep(10)
                        element = WebDriverWait(driver, 30).until(
                            EC.presence_of_element_located((By.XPATH, "(//div[@class='layout-element-title-text'])[1]"))
                        )
                        ActionChains(driver).context_click(element).perform()

                        try:
                            WebDriverWait(driver, 30).until(
                                EC.element_to_be_clickable((By.XPATH, "(//span[@class='menu-item-title'])[2]"))
                            ).click()
                        except TimeoutException:
                            continue

                        driver.find_element(By.XPATH, "//div[@class='sub-menu-wrapper']") \
                              .find_element(By.XPATH, "//div[@class='menu-item-img quick-print-to-excel']") \
                              .click()

                        # Ждем появления файла в папке загрузок
                        timer = 60
                        file_name = "CE Volumen Producto*.xlsx" if valor == "Volumen" else "CE Producto*.xlsx"
                        while timer != 0:
                            matches = list(DOWNLOAD_DIR.glob(file_name))
                            time.sleep(1)
                            timer -= 1
                            if matches:
                                save_flag = True
                                break

                    # Определяем куда сохранять (KG или USD)
                    path = mexico_params["CSV_PATH_MEXICO_KG"] if valor == "Volumen" else mexico_params["CSV_PATH_MEXICO_USD"]

                    # Читаем Excel, добавляем ttype и пишем в CSV
                    df = pd.read_excel(list(DOWNLOAD_DIR.glob(file_name))[0])
                    df["ttype"] = "imports"
                    df.to_csv(
                        path,
                        mode="a",
                        index=False,
                        header=first_iter,
                        encoding="utf-8-sig",
                    )
                    first_iter = False

                    # Удаляем скачанный Excel
                    for file in DOWNLOAD_DIR.glob(file_name):
                        os.remove(file)
                        print("Удален:", file)
                        break

                    break

                # БЛОК EXPORTS: выбираем периоды, регион (РФ), детализируем продукты, скачиваем Excel
                # Открываем фильтр периодов (разные XPATH для Volumen и USD)
                if valor == "Volumen":
                    WebDriverWait(driver, 60).until(
                        EC.element_to_be_clickable((By.XPATH, "(//div[@class='drop-down-text'])[1]"))
                    ).click()
                else:
                    WebDriverWait(driver, 60).until(
                        EC.element_to_be_clickable((By.XPATH, "(//div[@class='drop-down-text'])[3]"))
                    ).click()

                # Выбираем все годы 2019..2025 (в коде жестко прошито) !!!!!!!!!!!!!!!!!!!!!!!!!!!! потом на 2026 переставить
                for year in range(2025, 2018, -1):
                    year_select = WebDriverWait(driver, 60).until(
                        EC.presence_of_element_located((
                            By.XPATH,
                            f"//div[@id='node_[Fecha].[Periodos].[{year}]']/div/div/div/div/input"
                        ))
                    )
                    driver.execute_script("arguments[0].scrollIntoView({block:'end'});", year_select)
                    time.sleep(1)
                    driver.find_element(By.XPATH, f"//div[@id='node_[Fecha].[Periodos].[{year}]']/div/div/div/div/input").click()

                # Открываем выбор регионов/географии
                if valor == "Volumen":
                    WebDriverWait(driver, 20).until(
                        EC.element_to_be_clickable((By.XPATH, "(//div[contains(text(), 'Todas las regiones')])[2]"))
                    ).click()
                else:
                    WebDriverWait(driver, 20).until(
                        EC.element_to_be_clickable((By.XPATH, "(//div[contains(text(), 'Todas las regiones')])[1]"))
                    ).click()

                # Выбираем “Europa” -> “Federación Rusa” (иногда элементы не кликаются сразу)
                while True:
                    try:
                        WebDriverWait(driver, 20).until(
                            EC.element_to_be_clickable((By.XPATH, "//div[@id='node_[Localización].[Regiones].[Todas las regiones]']/div/div/div/div"))
                        ).click()
                        WebDriverWait(driver, 20).until(
                            EC.element_to_be_clickable((By.XPATH, "//div[@id='node_[Localización].[Regiones].[Europa]']/div/div/div/div"))
                        ).click()
                        break
                    except TimeoutException:
                        if valor == "Volumen":
                            driver.find_element(By.XPATH, "(//div[contains(text(), 'Todas las regiones')])[2]").click()
                        else:
                            driver.find_element(By.XPATH, "//div[contains(text(), 'Todas las regiones')]").click()
                        continue

                # Выбираем страну “Federación Rusa”
                while True:
                    try:
                        WebDriverWait(driver, 20).until(
                            EC.element_to_be_clickable((By.ID, "node_[Localización].[Regiones].[Federación Rusa]"))
                        ).click()
                        break
                    except TimeoutException:
                        # если список “уехал” — скроллим к последнему элементу
                        el = WebDriverWait(driver, 10).until(
                            EC.element_to_be_clickable((By.XPATH, "(//div[@class='treex-node treeNode defaultChipColorClassHoverAction'])[last()]"))
                        )
                        driver.execute_script("arguments[0].scrollIntoView({block:'start'});", el)
                        time.sleep(1)
                        continue

                time.sleep(10)

                # Открываем выбор “Todos los productos” и проваливаемся на уровень детализации
                if valor == "Volumen":
                    element = WebDriverWait(driver, 60).until(
                        EC.element_to_be_clickable((By.XPATH, "(//div[contains(text(), 'Todos los productos')])[2]"))
                    )
                else:
                    element = WebDriverWait(driver, 60).until(
                        EC.element_to_be_clickable((By.XPATH, "//div[contains(text(), 'Todos los productos')]"))
                    )

                ActionChains(driver).context_click(element).perform()

                # Drill-to-level -> выбираем нужный уровень (по индексу в списке)
                WebDriverWait(driver, 30).until(
                    EC.element_to_be_clickable((By.XPATH, "//div[@class='menu-item-img drill-to-level']"))
                ).click()
                driver.find_element(By.XPATH, "//div[@class='sub-menu-wrapper']") \
                      .find_elements(By.XPATH, "//span[@class='menu-item-title']")[8] \
                      .click()

                # Закрываем баннер/оверлей (если мешает)
                driver.find_element(By.XPATH, "//span[@class='banner-titulo']").click()

                # Скачиваем Excel для exports
                save_flag = False
                while not save_flag:
                    time.sleep(10)
                    element = WebDriverWait(driver, 30).until(
                        EC.presence_of_element_located((By.XPATH, "(//div[@class='layout-element-title-text'])[1]"))
                    )
                    ActionChains(driver).context_click(element).perform()

                    try:
                        WebDriverWait(driver, 30).until(
                            EC.element_to_be_clickable((By.XPATH, "(//span[@class='menu-item-title'])[2]"))
                        ).click()
                    except TimeoutException:
                        continue

                    driver.find_element(By.XPATH, "//div[@class='sub-menu-wrapper']") \
                          .find_element(By.XPATH, "//div[@class='menu-item-img quick-print-to-excel']") \
                          .click()

                    # Ждем файл
                    timer = 60
                    file_name = "CE Volumen Producto*.xlsx" if valor == "Volumen" else "CE Producto*.xlsx"

                    while timer != 0:
                        matches = list(DOWNLOAD_DIR.glob(file_name))
                        time.sleep(1)
                        timer -= 1
                        if matches:
                            save_flag = True
                            break

                    # Читаем Excel и пишем в CSV
                    df = pd.read_excel(list(DOWNLOAD_DIR.glob(file_name))[0])
                    df["ttype"] = "exports"

                    path = mexico_params["CSV_PATH_MEXICO_KG"] if valor == "Volumen" else mexico_params["CSV_PATH_MEXICO_USD"]

                    df.to_csv(
                        path,
                        mode="a",
                        index=False,
                        header=first_iter,
                        encoding="utf-8-sig",
                    )
                    first_iter = False

                    # Удаляем скачанный Excel
                    for file in DOWNLOAD_DIR.glob(file_name):
                        os.remove(file)
                        print("Удален:", file)
                        break

        # После двух метрик (USD + Volumen) приводим к финальному виду
        data = self.decor()

        # Чистим временные CSV
        if os.path.exists(mexico_params["CSV_PATH_MEXICO_KG"]):
            os.remove(mexico_params["CSV_PATH_MEXICO_KG"])
        if os.path.exists(mexico_params["CSV_PATH_MEXICO_USD"]):
            os.remove(mexico_params["CSV_PATH_MEXICO_USD"])

        print("Парсинг успешно завершен. Перехожу к составлению и оформлению итоговой таблицы.")
        return data
