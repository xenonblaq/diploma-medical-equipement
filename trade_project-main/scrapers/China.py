from tqdm import tqdm                 
import pandas as pd                  
import numpy as np                    

import os                             # Удаление/проверка файлов (CSV_PATH_CHINA)

import warnings
warnings.filterwarnings('ignore', category=FutureWarning)  # Глушим FutureWarning (часто от pandas)

import undetected_chromedriver as uc  # ChromeDriver с обходом антибот-защиты
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait


class China:
    """
    Парсер Китая с сайта таможенной статистики stats.customs.gov.cn (английская версия).
    ВАЖНО: это не “чистый API”, а сайт с антиботом/капчей, поэтому используется браузер через Selenium.

    Что делает (в общих чертах):
    1) Читает список HS-кодов из локального файла ./data/Commodity.csv (обязательный внешний файл)
    2) Открывает сайт в Chrome (undetected_chromedriver)
    3) Просит пользователя руками:
        - выбрать "Select Commodity"
        - ввести "01"
        - нажать "Enqueyri"
        - решить капчу
      (то есть это полуавтоматический парсер)
    4) Дальше циклы: year × ttype(экспорт/импорт) × батчи HS8 (по 16 кодов)
    5) На каждый батч отправляет POST-форму через execute_script (имитация запроса)
    6) Ждет загрузки результата и собирает строки из DOM (через find_elements и XPath)
    7) Пишет результат в временный CSV ./data/china_trade.csv (потом decor() приводит к стандарту)
    """

    china_params = {
        "CSV_PATH_CHINA": "./data/china_trade.csv",

        # selectTableState зависит от года — это критично для корректного ответа.
        # Если сайт меняет структуру таблиц — эти значения надо обновлять. !!!!!!!!!!!!!!!!!!!!
        "tables": {
            "2019": "2",
            "2020": "2",
            "2021": "2",
            "2022": "2",
            "2023": "3",
            "2024": "1",
            "2025": "1"
        },

        "get_url": "http://stats.customs.gov.cn/indexEn",
        "post_url": "http://stats.customs.gov.cn/queryDataForEN/getQueryDataListByWhere"
    }

    def __init__(self, years=["2025"], version_main=142):
        self.years = years
        self.version_main = version_main

    # Приведение результата к общей структуре
    def decor(self):
        """
        Читает накопленный CSV (сырой результат из selenium),
        приводит к структуре проекта:
        - строит даты
        - строит HS2/4/6/8
        - парсит числовые значения (Value USD, Quantity, Supp. Quantity)
        - ставит страны и направления
        """

        data = pd.read_csv(China.china_params["CSV_PATH_CHINA"])

        # Направление по ttype:
        # '1' -> Импорт, '0' -> Экспорт
        data["Направление"] = data["ttype"].astype(str).replace({'1': "Импорт", '0': "Экспорт"})

        # Period приходит как YYYYMM (пример: 202312)
        data["Year"] = data['Period'].astype(str).str[:4]
        data["Month"] = data['Period'].astype(str).str[4:]

        # Отчетный период формируем как 01.MM.YYYY
        data['Отчетный период'] = "01." + data["Month"] + '.' + data["Year"]

        # Партнер/источник фиксированы
        data["Страна-партнер"] = "Россия"
        data["Исходная страна"] = "Китай"

        # HS-уровни (в источнике HS8)
        data["Код товара (10 знаков)"] = np.nan
        data["Код товара (8 знаков)"] = data["HS Code"].astype(str).str.zfill(8)
        data["Код товара (6 знаков)"] = data["Код товара (8 знаков)"].str[:6]
        data["Код товара (2 знака)"] = data["Код товара (8 знаков)"].str[:2]
        data["Код товара (4 знака)"] = data["Код товара (8 знаков)"].str[:4]

        # Числа в источнике часто с запятыми "1,234"
        # "-" заменяем на NaN
        data["Значение (стоимость)"] = (
            data['Value USD']
            .str.replace(',', '')
            .str.replace('-', 'NaN')
            .astype(float)
        )

        data["Значение (масса)"] = (
            data['Quantity']
            .astype(str)
            .str.replace(',', '')
            .str.replace('-', 'NaN')
            .astype(float)
        )

        data["Единица объема"] = data["Unit"]  # тут это скорее “единица количества/массы”, но оставлено как в шаблоне
        data["ДЭИ, описание"] = data["Supp. Unit"]

        data["Единицы стоимости"] = "USD"

        data["Дополнительная единица измерения (ДЭИ)"] = (
            data['Supp. Quantity']
            .astype(str)
            .str.replace(',', '')
            .str.replace('-', 'NaN')
            .astype(float)
        )

        # Фильтруем нули/NaN по стоимости
        data = data[(data["Значение (стоимость)"] != 0) & (data["Значение (стоимость)"] != '0')]
        data = data[~data["Значение (стоимость)"].isna()]

        # Сортировка
        data = data.sort_values(
            by=['Year', 'Month', 'Направление'],
            ascending=[False, False, True],
            key=lambda col: col.astype(int) if col.name in ['Year', 'Month'] else col,
            ignore_index=True
        )

        # Финальные колонки
        data = data[[
            "Отчетный период", "Исходная страна", "Страна-партнер", "Направление",
            "Код товара (2 знака)", "Код товара (4 знака)", "Код товара (6 знаков)",
            "Код товара (8 знаков)", "Код товара (10 знаков)", "Значение (стоимость)",
            "Единицы стоимости", "Значение (масса)", "Единица объема",
            "Дополнительная единица измерения (ДЭИ)", "ДЭИ, описание"
        ]]

        data["Значение (стоимость) - ДЭИ"] = np.nan

        # Период -> datetime
        data["Отчетный период"] = pd.to_datetime(data["Отчетный период"], format="%d.%m.%Y")

        # Убираем дубли (часто появляются из-за повторной выгрузки/перезагрузки страниц)
        data = data.drop_duplicates()
        return data

    # Основной парсинг (Selenium + ручная капча)
    def parse(self):
        """
        Полуавтоматический сбор:

        ВАЖНО:
        - Требуется Chrome (или Chromium) совместимый с version_main=140 ЕЕ НАДО ПОСТОЯННО МЕНЯТЬ ИЛИ СЛЕДИТЬ, ЧТОБЫ ХРОМ НЕ ОБНОВЛЯЛСЯ
        - Требуется, чтобы undetected_chromedriver умел скачать/поднять драйвер
        - Требуется ручное прохождение капчи на сайте
        - Требуется внешний файл: ./data/Commodity.csv - коды с китайской таможни

        Результат пишется в ./data/china_trade.csv, потом decor() это приводит к стандарту.
        """
        # Вспомогательная функция: безопасно вытаскивает текст/атрибут из элемента по XPath
        def _get(el, xp, attr=None, default=""):
            try:
                node = el.find_element(By.XPATH, xp)
                if attr == "title":
                    v = node.get_attribute("title") or node.text
                else:
                    v = node.text
                return v.strip()
            except Exception:
                return default

        # Если старый CSV остался — удаляем
        if os.path.exists(China.china_params["CSV_PATH_CHINA"]):
            os.remove(China.china_params["CSV_PATH_CHINA"])

        # Читаем HS-коды из Commodity.csv
        hs = pd.DataFrame(
            pd.Series(
                pd.read_csv("./data/Commodity.csv", encoding="latin1", sep='","')['"CODES'].str[:8]
            )
            .astype(str)
            .str.zfill(8)
            .drop_duplicates()
        )

        # Приводим к числу, выкидываем NaN, обратно в строку, режем последние 2 символа, zfill(8)
        hs['"CODES'] = pd.to_numeric(hs['"CODES'], errors="coerce")
        hs = hs.dropna(subset=['"CODES'])
        hs = hs['"CODES'].astype(str).str[:-2].str.zfill(8).drop_duplicates().tolist()

        # Настройка Chrome
        prefs = {
            "safebrowsing.enabled": True
        }

        opts = uc.ChromeOptions()
        opts.add_experimental_option("prefs", prefs)

        # ВНИМАНИЕ / ХРУПКО:
        # version_main=140 означает жесткую привязку к версии Chrome. !!!!!!!
        # Если Chrome другой версии — может не подняться.
        driver = uc.Chrome(version_main=self.version_main, options=opts, use_subprocess=True)

        # Таймауты — увеличены, потому что сайт может тормозить
        driver.command_executor.set_timeout(300)
        driver.set_page_load_timeout(300)
        driver.set_script_timeout(300)

        # Открываем страницу
        driver.get(China.china_params["get_url"])

        # Ручной шаг (капча)
        print(
            "Пожалуйста, выберите в первом селекторе 'Select Commodity' и в появившееся поле введите '01'."
            " Нажмите кнопку 'Enqueyri' и решите каптчу."
            " По завершении введите в консоль любое число для начала парсинга"
        )
        start = input("Введите любое число:")

        # Основные циклы выгрузки
        first_iter = True
        for year in range(int(self.years[0]), int(self.years[-1]) + 1):
            for ttype in ["0", "1"]:  # 0 экспорт, 1 импорт

                # Идем по батчам HS — шаг 16
                for hs_code in tqdm(range(16, len(hs), 16), desc=f"Собрано HS8 (батчей по 16 шт.), {ttype}, {year}"):

                    # Берем кусок hs[hs_code-16 : hs_code] и соединяем запятыми
                    hs_ = ','.join(hs[hs_code - 16: hs_code])

                    # Payload для POST
                    payload = {
                        "pageSize": "200",
                        "pageNum": "1",
                        "iEType": ttype,
                        "currencyType": "usd",
                        "year": str(year),
                        "startMonth": "1",
                        "endMonth": "12",
                        "monthFlag": "1",
                        "unitFlag": "false",
                        "unitFlag1": "true",
                        "codeLength": "8",
                        "outerField1": "CODE_TS",
                        "outerField2": "ORIGIN_COUNTRY",
                        "outerField3": "",
                        "outerField4": "",
                        "outerValue1": hs_,
                        "outerValue2": "344",  # код страны (Россия)
                        "outerValue3": "",
                        "outerValue4": "",
                        "orderType": "CODE ASC DEFAULT",
                        "selectTableState": China.china_params["tables"][str(year)],
                        "currentStartTime": "202312" 
                    }

                    # Отправляем POST через вставку формы в DOM и submit()
                    # Это обход ограничений, чтобы запрос прошел “как с сайта”.
                    driver.execute_script("""
                    const url = arguments[0], p = arguments[1];
                    const f = document.createElement('form');
                    f.method = 'POST';
                    f.action = url;
                    f.target = '_self';
                    for (const [k,v] of Object.entries(p)) {
                        const inp = document.createElement('input');
                        inp.type = 'hidden'; inp.name = k; inp.value = v;
                        f.appendChild(inp);
                    }
                    document.body.appendChild(f);
                    f.submit();
                    """, China.china_params["post_url"], payload)

                    # Ждем появления totalPages
                    total_pages = WebDriverWait(driver, 300).until(
                        EC.presence_of_element_located((By.ID, 'totalPages'))
                    ).text.strip()

                    # Парсим число страниц (хотя дальше постраничная загрузка НЕ реализована)
                    total_pages = int(total_pages.split('of')[-1].split('pages')[0])

                    # Сбор строк из DOM
                    rows = []
                    # Period-ячейки определяются по CSS “div.th-line[style*='border-left: hidden']”
                    period_cells = driver.find_elements(By.CSS_SELECTOR, "div.th-line[style*='border-left: hidden']")

                    for per in period_cells:
                        hs_code = _get(per, "following-sibling::div[1]//div[contains(@class,'th-line')][1]")
                        description = _get(per, "following-sibling::div[1]//div[contains(@class,'th-line')][2]", attr="title")
                        country_code = _get(per, "following-sibling::div[2]//div[contains(@class,'th-line')][1]")
                        country_name = _get(per, "following-sibling::div[2]//div[contains(@class,'th-line')][2]", attr="title")
                        quantity = _get(per, "following-sibling::div[5][contains(@class,'th-line')]")
                        unit = _get(per, "following-sibling::div[6][contains(@class,'th-line')]")
                        supp_qty = _get(per, "following-sibling::div[7][contains(@class,'th-line')]")
                        supp_unit = _get(per, "following-sibling::div[8][contains(@class,'th-line')]")
                        value_usd = _get(per, "following-sibling::div[9][contains(@class,'th-line')]")

                        rows.append([
                            per.text.strip(), hs_code, description, country_code, country_name,
                            quantity, unit, supp_qty, supp_unit, value_usd
                        ])

                    # DataFrame из текущей страницы
                    df = pd.DataFrame(rows, columns=[
                        "Period", "HS Code", "Description", "Country Code", "Country Name",
                        "Quantity", "Unit", "Supp. Quantity", "Supp. Unit", "Value USD"
                    ])

                    # Description выкидывается (в итоговой таблице не используется)
                    df = df.drop(columns="Description")
                    # Добавляем тип потока
                    df["ttype"] = ttype
                    # Пишем в CSV “append”
                    df.to_csv(
                        China.china_params["CSV_PATH_CHINA"],
                        mode="a",
                        index=False,
                        header=first_iter,
                        encoding="utf-8-sig",
                    )
                    first_iter = False

        # Финальное приведение к стандарту
        data = self.decor()
        # Тут удаление CSV закомментировано — значит файл остается на диске. Если вдруг парсер сломается на четвертом часу, будет обидно
        # if os.path.exists(China.china_params["CSV_PATH_CHINA"]):
        #     os.remove(China.china_params["CSV_PATH_CHINA"])
        return data
