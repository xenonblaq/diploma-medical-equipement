import pandas as pd
import numpy as np
import os
import time
from tqdm import tqdm

import warnings
warnings.filterwarnings('ignore', category=FutureWarning)

import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support.ui import Select
from selenium.common.exceptions import UnexpectedAlertPresentException
from selenium.common.exceptions import WebDriverException
from selenium.common.exceptions import TimeoutException

from transformers import pipeline # OCR для решения каптчи


class Taiwan:
    """
    Парсер внешней торговли Тайваня (портал sw.nat.gov.tw).
    Особенность: сайт использует капчу, поэтому в коде подключен OCR (TrOCR),
    который пытается распознать картинку капчи и автоматически подставить ответ.

    Общая логика:
      1) Через Selenium настраиваем параметры отчета (импорт/экспорт, период, страна RU, статистика по HS).
      2) Идем батчами по HS6 (по 100 кодов) и отправляем запрос.
      3) Решаем капчу через OCR, забираем таблицу постранично, сохраняем в CSV.
      4) decor() приводит CSV к общему стандарту проекта (колонки, типы, сортировка, period->datetime).
    """

    taiwan_params = {
        # Страница отчета
        "url": "https://portal.sw.nat.gov.tw/APGA/GA30E",
        # Временный CSV для “сырого” результата парсинга
        "CSV_PATH_TAIWAN": "./data/taiwan_trade.csv"
    }

    def __init__(self, years, version_main=142):
        """
        years: список лет строками, например ["2019","2020",...,"2025"] или ["2025"].
        Используется для выбора START_YEAR / END_YEAR на форме сайта.
        """
        self.years = years
        self.version_main = version_main # Версия хрома

    def decor(self):
        """
        Приведение “сырого” CSV к стандартному формату проекта:
          - вычисляем Year/Month из поля Time
          - формируем Отчетный период "01.MM.YYYY"
          - строим HS коды (2/4/6), остальное (8/10) = NaN
          - приводим стоимость и массу к числам (USD*1000, KGM)
          - нормализуем направление Imports/Exports -> Импорт/Экспорт
          - сортируем и оставляем нужные колонки
        """
        # Читаем CSV, который насобирали в parse()
        data = pd.read_csv(Taiwan.taiwan_params["CSV_PATH_TAIWAN"])

        # Достаём год/месяц из строки Time (ожидается формат типа "YYYY-MM" или "YYYY-MM-..")
        data["Year"] = data["Time"].str[:4]
        data["Month"] = data["Time"].str[5:].str.zfill(2)

        # Делаем общий формат периода (первое число месяца)
        data['Отчетный период'] = "01." + data["Month"] + '.' + data["Year"]

        # Фиксированные поля проекта
        data["Страна-партнер"] = "Россия"
        data["Исходная страна"] = "Тайвань"

        # HS-коды: на сайте возвращается HS6 в Commodity Code
        data["Код товара (10 знаков)"] = np.nan
        data["Код товара (8 знаков)"] = np.nan
        data["Код товара (6 знаков)"] = data["Commodity Code"].astype(str).str.zfill(6)
        data["Код товара (2 знака)"] = data["Код товара (6 знаков)"].str[:2]
        data["Код товара (4 знака)"] = data["Код товара (6 знаков)"].str[:4]

        # Стоимость: поле приходит строкой, бывает с пробелами/запятыми; значение в тысячах долларов
        data["Значение (стоимость)"] = data["Value (USD$ 1,000)"].str.replace(' ', '')
        data["Значение (стоимость)"] = data["Значение (стоимость)"].str.replace(',', '')
        data["Значение (стоимость)"] = data["Значение (стоимость)"].astype(float) * 1000

        # Масса: KGM, тоже чистим строку от пробелов/запятых
        data["Значение (масса)"] = data["Weight (KGM)"].str.replace(' ', '')
        data["Значение (масса)"] = data["Значение (масса)"].str.replace(',', '')
        data["Значение (масса)"] = data["Значение (масса)"].astype(float)

        # Единицы и поля проекта
        data["Единицы стоимости"] = "USD"
        data["Единица объема"] = "килограмм"
        data["ДЭИ, описание"] = np.nan
        data["Дополнительная единица измерения (ДЭИ)"] = np.nan
        data["Значение (стоимость) - ДЭИ"] = np.nan

        # Нормализация направления
        data["Направление"] = data["Imports / Exports"].replace({"Imports": "Импорт", "Exports": "Экспорт"})

        # Фильтрация нулей/NaN по стоимости
        data = data[(data["Значение (стоимость)"] != 0) & (data["Значение (стоимость)"] != '0')]
        data = data[~data["Значение (стоимость)"].isna()]

        # Сортировка: последние месяцы/годы — выше
        data = data.sort_values(
            by=['Year', 'Month', 'Направление'],
            ascending=[False, False, True],
            key=lambda col: col.astype(int) if col.name in ['Year', 'Month'] else col,
            ignore_index=True
        )

        # Финальный набор колонок
        data = data[[
            "Отчетный период", "Исходная страна", "Страна-партнер", "Направление",
            "Код товара (2 знака)", "Код товара (4 знака)", "Код товара (6 знаков)",
            "Код товара (8 знаков)", "Код товара (10 знаков)", "Значение (стоимость)",
            "Единицы стоимости", "Значение (масса)", "Единица объема",
            "Дополнительная единица измерения (ДЭИ)", "ДЭИ, описание", "Значение (стоимость) - ДЭИ"
        ]]

        # Приводим дату к datetime
        data['Отчетный период'] = pd.to_datetime(data['Отчетный период'], format='%d.%m.%Y')
        return data

    def parse(self) -> pd.DataFrame:
        """
        Основной парсинг через Selenium + OCR капчи.

        По шагам:
          - чистим старый CSV
          - поднимаем OCR-пайплайн (TrOCR printed)
          - запускаем Chrome (undetected_chromedriver) и открываем страницу
          - настраиваем чекбоксы (импорт/экспорт, тип отчета, период, страна RU)
          - выбираем статистику “Statistics5” и “EXPORT_TYPE_1”
          - батчами по 100 HS6 отправляем запрос:
              * вводим коды
              * решаем капчу (скриншот + OCR)
              * ждем таблицу и собираем все страницы
              * сохраняем в CSV
          - после батчей делаем decor() и возвращаем результат
        """

        # Удаляем старый CSV, чтобы не подмешать прошлые результаты
        if os.path.exists(Taiwan.taiwan_params["CSV_PATH_TAIWAN"]):
            os.remove(Taiwan.taiwan_params["CSV_PATH_TAIWAN"])

        no_data_flag = False   # флаг "нет данных по запросу"
        first_iter = True      # нужен для header=True только при первой записи CSV

        # OCR-модель для распознавания капчи
        ocr = pipeline("image-to-text", model="microsoft/trocr-base-printed")

        # Настройки браузера (eager = не ждать всех ресурсов)
        opts = Options()
        opts.page_load_strategy = "eager"

        # Запуск Chrome через undetected_chromedriver
        driver = uc.Chrome(version_main=self.version_main, options=opts)
        driver.command_executor.set_timeout(300)
        driver.set_page_load_timeout(60)
        driver.set_script_timeout(60)

        # Открываем страницу
        driver.get(Taiwan.taiwan_params["url"])

        # Включаем сразу импорт и экспорт (итоговые)
        driver.find_element(By.ID, "ImportTotal").click()
        driver.find_element(By.ID, "ExportTotal").click()

        # Выбираем тип отчета (REPORT_TYPE_0)
        driver.find_element(By.ID, "REPORT_TYPE_0").click()

        time.sleep(3)

        # Селекты периода
        select_start = Select(driver.find_element(By.ID, "START_YEAR"))
        select_month_start = Select(driver.find_element(By.ID, "START_MONTH"))
        select_finish = Select(driver.find_element(By.ID, "END_YEAR"))
        select_month_finish = Select(driver.find_element(By.ID, "END_MONTH"))

        # Границы периода берём из списка years
        start_year = self.years[0]
        finish_year = self.years[-1]

        # Старт: январь start_year
        select_start.select_by_value(start_year)
        select_month_start.select_by_value("1")

        # Финиш: finish_year, месяц:
        #  - если 2025: берем последний доступный месяц в селекте (данные могут быть не за весь год)
        #  - иначе: декабрь
        select_finish.select_by_value(finish_year)
        if finish_year == "2025":
            select_month_finish.select_by_index(len(select_month_finish.options) - 1)
        else:
            select_month_finish.select_by_value("12")

        # Выбор страны: COUNTRY_TYPE_2 -> Europe -> RU
        country = driver.find_element(By.ID, "COUNTRY_TYPE_2")
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", country)
        country.click()

        WebDriverWait(driver, 60).until(
            EC.element_to_be_clickable((By.XPATH, "//option[@value='Europe']"))
        ).click()

        WebDriverWait(driver, 60).until(
            EC.element_to_be_clickable((By.XPATH, "//option[@value='RU']"))
        ).click()

        # Нажимаем кнопки в интерфейсе (по индексам элементов; зависит от верстки сайта)
        driver.find_elements(By.XPATH, "//a[@href='javascript:void(0)']/span[@class='ui-button-text']")[4].click()
        driver.find_elements(By.XPATH, "//a[@href='javascript:void(0)']/span[@class='ui-button-text']")[7].click()

        # Переключаем тип статистики и экспортный режим (как в исходной логике)
        driver.find_element(By.ID, "Statistics5").click()
        driver.find_element(By.ID, "EXPORT_TYPE_1").click()

        # Загружаем список HS6 (из локального файла проекта)
        hs6 = (
            pd.Series(pd.read_csv("./data/HSCodeandDescription.csv")['Code'])
            .astype(str)
            .str.zfill(6)
            .tolist()
        )

        # Идем батчами по 100 кодов
        pb1 = tqdm(range(100, len(hs6), 100), total=5614 // 100, unit="batch")
        for idx in pb1:
            pb1.set_description("Собрано HS6 (батчей по 100 штук)")

            # Формируем строку кодов для ввода в goodsCodeValue
            request = ', '.join(hs6[idx - 100: idx])

            # Выбираем ввод HS-кодов вручную и вводим батч
            driver.find_element(By.ID, "HS_TYPE_2").click()
            driver.find_element(By.ID, "goodsCodeValue").send_keys(request)

            # Пытаемся пройти капчу и собрать таблицу
            while True:
                try:
                    # Находим картинку капчи
                    captcha = driver.find_element(By.ID, "captchaPic")
                    time.sleep(1)

                    # Делаем скриншот капчи (иногда WebDriverException — пробуем ещё)
                    while True:
                        try:
                            captcha.screenshot("./data/captcha.png")
                            break
                        except WebDriverException:
                            time.sleep(2)
                            continue

                    # OCR: распознаем текст капчи
                    captcha = ocr("./data/captcha.png")
                    driver.find_element(By.NAME, "searchInfo.Captcha").send_keys(
                        captcha[0]["generated_text"].strip()
                    )

                    # Отправляем форму
                    driver.find_element(By.ID, "FORM_CHECK").click()

                    # Собираем таблицу со всех страниц
                    data = []
                    while True:
                        time.sleep(1)
                        try:
                            # Ждем исчезновения блокера (загрузка)
                            WebDriverWait(driver, 60).until(
                                EC.invisibility_of_element((By.ID, "blocker"))
                            )
                            time.sleep(1)

                            # Ждем таблицу
                            table = WebDriverWait(driver, 60).until(
                                EC.presence_of_element_located((By.XPATH, "//table[@id='viewList']"))
                            )

                            # Если pager показывает 0 строк — значит данных нет
                            if driver.find_element(By.ID, "sp_1_viewPager").text == '0':
                                no_data_flag = True
                                break
                        except TimeoutException:
                            # Не дождались — трактуем как “нет данных/ошибка”
                            no_data_flag = True
                            break

                        # Вытаскиваем строки таблицы JS-ом (чистим пробелы/nbsp)
                        table_rows = driver.execute_script(
                            """
                            const rows = [...arguments[0].querySelectorAll('tr')];
                            return rows
                              .map(r => [...r.querySelectorAll('th,td')]
                                            .map(c => c.textContent.replace(/\\u00A0/g,' ').trim())
                                            .filter(Boolean))
                              .filter(r => r.length && !/^total:/i.test(r[0]));
                            """,
                            table,
                        )

                        # Копим строки
                        for row in table_rows:
                            data.append(row)

                        # Переходим на следующую страницу, если возможно
                        try:
                            driver.find_element(By.ID, "next_viewPager").click()
                            time.sleep(1)
                            WebDriverWait(driver, 60).until(
                                EC.invisibility_of_element((By.ID, "blocker"))
                            )
                        except:
                            # Следующей страницы нет — выходим
                            break

                    # Если дошли сюда — попытка завершена
                    break

                except UnexpectedAlertPresentException:
                    # Если сайт ругнулся алертом (часто из-за капчи) — обновляем капчу и пробуем снова
                    driver.find_element(By.XPATH, "//a[@title='Refresh CAPTCHA']").click()
                    continue

            # Удаляем локальную картинку капчи (чтобы не копились файлы)
            if os.path.exists("./data/captcha.png"):
                os.remove("./data/captcha.png")

            # Если данных не было — возвращаемся назад, чистим поле и идем дальше
            if no_data_flag:
                no_data_flag = False
                driver.back()

                WebDriverWait(driver, 60).until(
                    EC.element_to_be_clickable((By.ID, "goodsCodeValue"))
                ).clear()
                continue

            # Чистим значения от пометки (preliminary)
            data = [[el.replace('(preliminary)', '').strip() for el in row] for row in data]

            # Собираем DataFrame с фиксированными колонками
            data = pd.DataFrame(
                data,
                columns=[
                    "Imports / Exports", "Time", "Country(Area)", "Commodity Code",
                    "Description of Good", "Value (USD$ 1,000)", "Weight (KGM)"
                ]
            )

            # Сохраняем батч в CSV
            data.to_csv(
                Taiwan.taiwan_params["CSV_PATH_TAIWAN"],
                mode="a",
                index=False,
                header=first_iter,
                encoding="utf-8-sig",
            )
            first_iter = False

            # Возвращаемся назад к форме, чистим поле кодов и идем к следующему батчу
            driver.back()
            WebDriverWait(driver, 60).until(
                EC.element_to_be_clickable((By.ID, "goodsCodeValue"))
            ).clear()

        print("Парсинг успешно завершен. Перехожу к составлению и оформлению итоговой таблицы.")

        # Декорируем (приведение к стандарту) и чистим временный CSV
        data = self.decor()
        if os.path.exists(Taiwan.taiwan_params["CSV_PATH_TAIWAN"]):
            os.remove(Taiwan.taiwan_params["CSV_PATH_TAIWAN"])
        return data
