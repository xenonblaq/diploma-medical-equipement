# Один из первых парсеров, которые я написал. Давно его не фиксил, с июля.
# Он нестабильно работал, потом выяснили, что на евростате тоже есть турецкая статистика
# В общем, не рекомендую использовать. Гораздо удобнее сразу вместе со всей Европой спарсить по API.
# Проверяли, статистика сходится, даже секретные коды.

import pandas as pd
import os
import platform, time
from tqdm import tqdm

import warnings
warnings.filterwarnings('ignore', category=FutureWarning)

from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException


class Turkey:
    """
    Парсер внешней торговли Турции с сайта TÜIK (biruni.tuik.gov.tr) через Selenium.

    Общая логика:
      1) parse():
         - открываем страницу отчета
         - выбираем годы (multi-select через Ctrl/Cmd)
         - выбираем параметры отчета (периодичность/валюта/детализация)
         - выбираем страну-партнера (код 75 = Россия)
         - перебираем HS6 коды батчами по 25 штук
         - для каждого батча открываем результат в новой вкладке, парсим HTML-таблицу, пишем в CSV
      2) decor():
         - приводим сырой CSV к стандартному формату проекта:
           период, страна-источник/партнер, направление, HS коды, стоимость/масса/ДЭИ, сортировка
    """

    turkey_params = {
        # Страница интерактивного отчета внешней торговли (англ. интерфейс)
        "url": "https://biruni.tuik.gov.tr/disticaretapp/disticaret_ing.zul?param1=4&param2=23&sitcrev=0&isicrev=0&sayac=5902",
        # Временный CSV, куда складываем сырые результаты из таблицы
        "CSV_PATH_TURKEY": "./data/turkey_trade.csv",
        # Маппинг "год -> суффикс id" для чекбоксов/опций в UI TÜIK
        # (эти id завязаны на текущую верстку страницы)
        "years_ids": {
            "2019": "x3-cave",
            "2020": "z3-cave",
            "2021": "n3-cave",
            "2022": "p3-cave",
            "2023": "r3-cave",
            "2024": "t3-cave",
            "2025": "v3-cave",
        }
    }

    def __init__(self, years):
        """
        years: список лет строками, например ["2023", "2024", "2025"].

        Дополнительно:
          - загружаем список HS6 кодов из локального справочника
        """
        self.years = years
        self.hs6 = pd.Series(pd.read_csv("./data/HSCodeandDescription.csv")['Code']).tolist()

    def decor(self):
        """
        Приведение сырого CSV (как его выгрузили из таблицы TÜIK) к стандартному формату проекта.

        Важно:
          - таблица из TÜIK может приходить со "съехавшими" колонками: часть строк бывает сдвинута вправо.
            Поэтому сначала делаем выравнивание: если последние i колонок пустые, сдвигаем строку на i.
          - затем ffill (протяжка) по ключевым полям, потому что в исходной таблице они часто указаны
            только один раз для группы строк.
          - после этого строим стандартные поля: период, HS2/4/6, направление, стоимость/масса/ДЭИ, единицы.
        """
        data = pd.read_csv(Turkey.turkey_params["CSV_PATH_TURKEY"])

        # --- 1) Исправление "съехавших" строк: если последние колонки пустые, сдвигаем строку влево ---
        for i in range(4, 0, -1):
            mask = data.iloc[:, -i:].isna().all(axis=1)
            data.loc[mask, :] = data.loc[mask, :].shift(i, axis=1)

        # --- 2) Протяжка значений (ffill) для групповых колонок ---
        data['Year'].fillna(method='ffill', inplace=True)
        data['Month'].fillna(method='ffill', inplace=True)
        data['Country'].fillna(method='ffill', inplace=True)
        data['HS6'].fillna(method='ffill', inplace=True)
        data['HS6 name'].fillna(method='ffill', inplace=True)
        data['Unit'].fillna(method='ffill', inplace=True)
        data['Country name'].fillna(method='ffill', inplace=True)

        # --- 3) Стандартные поля проекта ---
        data['Отчетный период'] = "01." + data['Month'].astype(str).str.zfill(2) + '.' + data["Year"]
        data["Страна-партнер"] = "Россия"
        data["Исходная страна"] = "Турция"

        # HS-коды
        data["Код товара (2 знака)"] = data["HS6"].str[:2]
        data["Код товара (4 знака)"] = data["HS6"].str[:4]
        data["Код товара (6 знаков)"] = data["HS6"]

        # В этом источнике нет HS8/HS10 — ставим плейсхолдеры
        data["Код товара (8 знаков)"] = '-'
        data["Код товара (10 знаков)"] = '-'

        # Стоимость в USD по умолчанию в отчете
        data["Единицы стоимости"] = "USD"

        # --- 4) Разбор единиц: иногда Unit вида "ton/kg" или подобное ---
        data["ДЭИ, описание"] = '-'
        data["Единица объема"] = data["Unit"]

        # Если в Unit есть '/', то интерпретируем как "объем/ДЭИ"
        mask = data["Unit"].str.contains('/')
        data.loc[mask, "ДЭИ, описание"] = data.loc[mask, "Unit"].str.split('/').str[1]
        data.loc[mask, "Единица объема"] = data.loc[mask, "Unit"].str.split('/').str[0]

        # --- 5) Определяем направление (Экспорт/Импорт) и нормализуем строки ---
        # Логика: если Export Dollar == 0 => это импортная строка; если Import Dollar == 0 => экспортная
        data["Export Dollar"] = data["Export Dollar"].astype(str)
        data["Import Dollar"] = data["Import Dollar"].astype(str)
        data.loc[(data["Export Dollar"] == '0') | (data["Export Dollar"] == '0.0'), "Направление"] = "Импорт"
        data.loc[(data["Import Dollar"] == '0') | (data["Import Dollar"] == '0.0'), "Направление"] = "Экспорт"

        # Для строк, где направление не определилось (есть и импорт, и экспорт одновременно),
        # делаем "раздвоение": создаем две строки — экспортную и импортную
        mask = (data['Направление'].isna())
        data_split = data[mask]
        data_rest = data[~mask]

        df_export = data_split.copy()
        df_export['Import Dollar'] = '0'
        df_export['Направление'] = 'Экспорт'

        df_import = data_split.copy()
        df_import['Export Dollar'] = '0'
        df_import['Направление'] = 'Импорт'

        data = pd.concat([data_rest, df_export, df_import], ignore_index=True)

        # Раскладываем значения в стандартные поля проекта
        data.loc[data["Направление"] == "Импорт", "Значение (стоимость)"] = data["Import Dollar"]
        data.loc[data["Направление"] == "Экспорт", "Значение (стоимость)"] = data["Export Dollar"]
        data.loc[data["Направление"] == "Импорт", "Значение (масса)"] = data["Import quantity 1"]
        data.loc[data["Направление"] == "Экспорт", "Значение (масса)"] = data["Export quantity 1"]
        data.loc[data["Направление"] == "Импорт", "Дополнительная единица измерения (ДЭИ)"] = data["Import quantity 2"]
        data.loc[data["Направление"] == "Экспорт", "Дополнительная единица измерения (ДЭИ)"] = data["Export quantity 2"]

        # --- 6) Сортировка (свежие периоды сверху) ---
        data = data.sort_values(
            by=['Year', 'Month', 'Направление'],
            ascending=[False, False, True],
            key=lambda col: col.astype(int) if col.name in ['Year', 'Month'] else col,
            ignore_index=True
        )

        # --- 7) Финальный набор колонок (стандарт проекта) ---
        data = data[[
            "Отчетный период", "Исходная страна", "Страна-партнер", "Направление",
            "Код товара (2 знака)", "Код товара (4 знака)", "Код товара (6 знаков)",
            "Код товара (8 знаков)", "Код товара (10 знаков)", "Значение (стоимость)",
            "Единицы стоимости", "Значение (масса)", "Единица объема",
            "Дополнительная единица измерения (ДЭИ)", "ДЭИ, описание"
        ]]

        return data

    def parse(self) -> pd.DataFrame:
        """
        Парсинг TÜIK через Selenium.

        Ключевые моменты:
          - page_load_strategy="eager" ускоряет загрузку (ждем DOM, но не все ресурсы)
          - вычисляем user_id: у TÜIK динамические id, указываются с общим префиксом
          - годы выбираем мультикликом (Ctrl/Cmd + click)
          - HS6 отправляем батчами по 25 кодов в поле ввода
          - результат открывается в новой вкладке: переключаемся, парсим таблицу JS-скриптом, сохраняем, закрываем вкладку
          - если вкладка не открылась/таймаут — пытаемся повторить батч
        """
        # --- 1) Настройка драйвера ---
        opts = Options()
        opts.page_load_strategy = "eager"
        driver = webdriver.Chrome(options=opts)
        driver.command_executor.set_timeout(300)
        driver.set_page_load_timeout(60)
        driver.set_script_timeout(60)

        # --- 2) Открываем страницу (с ретраями) ---
        while True:
            try:
                driver.get(Turkey.turkey_params['url'])
                break
            except:
                continue

        # Ждем исчезновения прелоадера
        WebDriverWait(driver, 300).until(
            EC.invisibility_of_element_located((By.ID, "zk_proc"))
        )

        # --- 3) Определяем динамический префикс id (user_id) ---
        user_id = driver.find_element(By.CLASS_NAME, "z-page")
        user_id = user_id.get_attribute("id")[:-1]  # отрезаем последний символ, чтобы потом добавлять суффиксы
        wait = WebDriverWait(driver, 5)

        # Открываем блок выбора лет
        driver.find_element(By.ID, user_id + "d-real").click()

        # Преобразуем список лет в список суффиксов id
        years_ids = [Turkey.turkey_params["years_ids"][year] for year in self.years]

        # --- 4) Чистим предыдущий CSV ---
        if os.path.exists(Turkey.turkey_params["CSV_PATH_TURKEY"]):
            os.remove(Turkey.turkey_params["CSV_PATH_TURKEY"])

        # --- 5) Мультивыбор лет (Ctrl/Cmd + click) ---
        modifier = Keys.COMMAND if platform.system() == "Darwin" else Keys.CONTROL
        actions = ActionChains(driver)
        actions.key_down(modifier)

        for suffix in years_ids:
            el = driver.find_element(By.ID, user_id + suffix)

            # Для 2019 может понадобиться скролл, чтобы элемент стал кликабельным
            if "2019" in self.years:
                driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)

            actions.click(el)

        actions.key_up(modifier)
        actions.perform()

        # --- 6) Выбор остальных параметров отчета (детализация/валюта/прочее) ---
        # (id-шники завязаны на текущую структуру страницы)
        driver.find_element(By.ID, user_id + "15-cave").click()
        driver.find_element(By.ID, user_id + "a0-real").click()

        # --- 7) Выбор страны: "75" (Россия) ---
        country_input = wait.until(EC.visibility_of_element_located((By.ID, user_id + "e0")))
        country_input.clear()
        country_input.send_keys("75")

        # Применяем выбранные параметры/страны (кнопка)
        driver.find_element(By.ID, user_id + "k1-real").click()

        first_iter = True

        # --- 8) Основной цикл: HS6 батчами по 25 ---
        for idx in tqdm(
            range(0, 5613, 25),
            total=5613 // 25,
            desc="Собрано HS6 (батчей по 25 шт)",
            unit="batch"
        ):
            # Формируем строку с кодами для вставки (через запятую)
            codes = ','.join(map(str, self.hs6[idx: idx + 25]))

            # Вводим HS6 коды в поле
            hs6_input = wait.until(EC.visibility_of_element_located((By.ID, user_id + "o1")))
            hs6_input.clear()
            hs6_input.send_keys(codes)

            # На первом батче выбираем нужные галочки/опции (один раз)
            if first_iter:
                driver.find_element(By.ID, user_id + "d2-real").click()
                driver.find_element(By.ID, user_id + "e2-real").click()
                driver.find_element(By.ID, user_id + "j2-real").click()

            # Запоминаем родительскую вкладку
            parent = driver.current_window_handle

            # --- 9) Запрашиваем отчет (откроется новая вкладка) + ретраи при таймаутах ---
            while True:
                opened_before = set(driver.window_handles)

                # Кнопка генерации отчета/открытия результата
                driver.find_element(By.ID, user_id + "l3").click()

                try:
                    # Ждем новую вкладку
                    WebDriverWait(driver, 240).until(EC.new_window_is_opened(opened_before))
                    new_tab = (set(driver.window_handles) - opened_before).pop()
                    driver.switch_to.window(new_tab)

                    # Ждем появления таблицы
                    WebDriverWait(driver, 60).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, "table"))
                    )

                    # Забираем таблицу JS-скриптом (быстрее и надежнее, чем парсить html вручную)
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

                    # table_rows[2] — заголовки, [3:-1] — тело (по текущей структуре отчета)
                    data = pd.DataFrame(table_rows[3:-1], columns=table_rows[2])

                    # Сохраняем батч в CSV
                    data.to_csv(
                        Turkey.turkey_params["CSV_PATH_TURKEY"],
                        mode="a",
                        index=False,
                        header=first_iter,
                        encoding="utf-8-sig",
                    )
                    first_iter = False

                    # Закрываем вкладку с результатом и возвращаемся назад
                    driver.close()
                    driver.switch_to.window(parent)
                    break

                except TimeoutException:
                    # Если новая вкладка не открылась вообще — считаем, что UI завис/сломался, перезапускаем
                    if len(set(driver.window_handles) - {parent}) == 0:
                        print("‼️  Новая вкладка не открылась за 4 мин — перезапускаю.")
                        driver.quit()
                        # В оригинале тут было return self.parse_turkey()
                        # Оставляем как есть (но правильно было бы: return self.parse())
                        return self.parse()

                    # Если вкладка открылась, но зависла — закрываем все лишние вкладки и ретраим батч
                    for h in set(driver.window_handles) - {parent}:
                        try:
                            driver.switch_to.window(h)
                            driver.close()
                        except Exception:
                            pass

                    driver.switch_to.window(parent)
                    print(f"[retry] batch {idx}-{idx+24}: no response within 60 s")
                    continue

        # --- 10) Финал: декорирование + чистка временного CSV ---
        print("Парсинг успешно завершен. Перехожу к составлению и оформлению итоговой таблицы.")
        data = self.decor()

        if os.path.exists(Turkey.turkey_params["CSV_PATH_TURKEY"]):
            os.remove(Turkey.turkey_params["CSV_PATH_TURKEY"])

        return data
