import PyPDF2                 # Чтение PDF и извлечение текста со страниц (page.extract_text)
import pandas as pd           
import numpy as np           
import re                     # Регулярки для распознавания HS/чисел и очистки строк
import os                     # Проверка/создание/удаление файлов и папок
import subprocess             # Запуск внешних утилит (bsdtar) для распаковки rar
import shutil                 # Удаление папок целиком (rmtree)
import requests               
from bs4 import BeautifulSoup
from tqdm import tqdm        

import warnings
warnings.filterwarnings('ignore', category=UserWarning)  # Глушим warning-и (например от pandas/read_html и т.п.)


class Azerbaijan:
    """
    Парсер Азербайджана (customs.gov.az).

    Источник: "statistics bulletin" (бюллетени), которые скачиваются как RAR,
    внутри — PDF "10. Cədvəl 8.pdf" по кварталам.

    Основная идея:
    1) Берем страницу со ссылками на бюллетени по годам
    2) Скачиваем RAR для каждого года
    3) Распаковываем bsdtar'ом (важно: системная утилита)
    4) Для каждого квартала находим PDF, вытаскиваем text_lines из PDF
    5) Вырезаем блок по стране-партнеру (Россия или Беларусь)
    6) Превращаем строки блока в таблицу (hs4, unit, qty/usd exp/imp)
    7) Записываем результат в временный CSV ./data/az_trade.csv (в квартальном виде)
    8) decor(): преобразует кварталы -> месяцы, делит значения на 3 и приводит к стандартной схеме проекта
    """


    az_params = {
        # Набор разных тире, встречающихся в PDF
        "DASHES": {'-', '–', '—'},
        # Число (целое или с десятичной частью через точку)
        "NUM_RE": re.compile(r'^\d+(?:\.\d+)?$'),
        # HS4 + описание (например: "0101 Живые лошади ...")
        "HS4_RE": re.compile(r'^(\d{4})(?!\d)\s*(.*)$'),
        # Случай, когда HS4 "склеен" с цифрами дальше (например: 0101123...)
        # Тогда первые 4 цифры HS4, а следующие цифры — часть "хвоста"
        "HS4_GLUE_RE": re.compile(r'^(\d{4})(\d{2,})(?=\D|$)\s*(.*)$'),
        # Куда складывается промежуточный CSV (накопление строк по кварталам)
        "CSV_PATH_AZ": "./data/az_trade.csv",
        # Страница со ссылками на бюллетени
        "url": "https://customs.gov.az/en/faydali/gomruk-statistikasi/statistics-bulletin",
        # Линии, которые нужно выкинуть из текста PDF (мусор: шапки/подвалы/страницы)
        "to_remove": [
            '235 © Azərbaycan Respublikası Dövlət Gömrük Komitəsi',
            'Ölçü',
            'vahidi2025-ci ilin I rübüXİF',
            'MN',
            'üzrə',
            'kodu Miqdar Statistik dəyər Miqdar Statistik dəyər8-ci cədvəlin ardı',
            'İxrac İdxal',
        ]
    }

    def __init__(self, years=["2025"], belarus=False):
        # years: список лет (строки) для финальной фильтрации
        self.years = years
        # belarus:
        #   False -> вырезаем блок "CƏMİRUSİYA ... CƏMİSALVADOR"
        #   True  -> вырезаем блок "CƏMİBELARUS ... CƏMİBELÇİKA"
        self.belarus = belarus

    # Небольшие утилиты для парсинга текста PDF
    def is_num(self, tok: str) -> bool:
        """Проверка: токен — число по NUM_RE."""
        return bool(Azerbaijan.az_params["NUM_RE"].fullmatch(tok))

    def normalize_ws(self, s: str) -> str:
        """
        Нормализует “необычные пробелы” из PDF в обычный пробел.
        PDF часто содержит NBSP/тонкие пробелы, которые ломают split().
        """
        return re.sub(r'[\u00A0\u202F\u2009\u2007]', ' ', s)

    def compact_numbers(self, s: str) -> str:
        """
        В PDF числа могут быть вида "12 345 678" (с пробелами).
        Превращаем в "12345678", чтобы дальше корректно парсить как число.
        """
        s = self.normalize_ws(s)
        return re.sub(
            r'(?<!\.)\d{1,3}(?: \d{3})+(?:\.\d+)?',
            lambda m: m.group(0).replace(' ', ''),
            s
        )

    def split_head_tail(self, raw: str):
        """
        Разбирает одну строку из PDF.
        Возвращает: (hs4, unit, tail)

        Где:
        - hs4: первые 4 цифры HS (если нашли), иначе None
        - unit: единица измерения / текстовая часть до чисел/тире
        - tail: оставшаяся часть (как правило 4 числа/тире: exp qty, exp usd, imp qty, imp usd)

        Логика довольно “эвристическая”, потому что PDF-текст “кривой”.
        """
        s = self.normalize_ws(raw.strip())

        # Если строка начинается с "1000 ..." — это не HS4, а строка со шкалой/единицей
        m_unit1000 = re.match(r'^1000\s+(?=[^\d])', s)
        if m_unit1000:
            hs4, rest = None, s
        else:
            # 1) Пытаемся вытащить hs4 если цифры склеены
            m0 = Azerbaijan.az_params["HS4_GLUE_RE"].match(s)
            if m0:
                hs4, glued_digits, rest_after = m0.groups()
                # glued_digits возвращаем обратно в “хвост”, чтобы не потерять
                rest = (glued_digits + (' ' + rest_after if rest_after else '')).strip()
            else:
                # 2) Обычный HS4_RE: "dddd текст..."
                m1 = Azerbaijan.az_params["HS4_RE"].match(s)
                if m1:
                    hs4, rest = m1.group(1), m1.group(2)
                else:
                    hs4, rest = None, s

        # Вставляем пробел между буквами и цифрами (например "kg100" -> "kg 100")
        rest = re.sub(r'(?<=[^\W\d_])(?=\d)', ' ', rest)
        # Токенизация по пробелам
        toks = re.split(r'\s+', rest) if rest else []
        if not toks:
            return hs4, '', ''
        # Склеиваем "m 3" -> "m3" и "m 2" -> "m2"
        if len(toks) >= 2 and toks[0].lower() == 'm' and toks[1] == '3':
            toks = ['m3'] + toks[2:]
        if len(toks) >= 2 and toks[0].lower() == 'm' and toks[1] == '2':
            toks = ['m2'] + toks[2:]

        # Определение unit и хвоста:
        # - если первый токен число, а второй НЕ число и НЕ тире -> unit из двух токенов
        if self.is_num(toks[0]) and len(toks) >= 2 and (not self.is_num(toks[1])) and (toks[1] not in Azerbaijan.az_params["DASHES"]):
            unit_tokens = toks[:2]
            rest_tokens = toks[2:]
        else:
            # иначе идем до первого тире или числа
            j = 0
            while j < len(toks) and (toks[j] not in Azerbaijan.az_params["DASHES"]) and (not self.is_num(toks[j])):
                j += 1
            # unit хотя бы 1 токен (иначе пусто)
            unit_tokens = toks[:max(1, j)]
            rest_tokens = toks[max(1, j):] if j < len(toks) else []

        unit = ' '.join(unit_tokens)
        tail = ' '.join(rest_tokens)
        return hs4, unit, tail

    def rows_to_df(self, lines):
        """
        Превращает список строк (после вырезания нужного блока страны) в список записей:
        [hs4, unit, quantity_exp, usd_exp, quantity_imp, usd_imp]

        В tail ожидаются 4 токена (или тире), но иногда бывает только 2:
        тогда дополняем слева ['-','-'].
        """
        recs = []
        for raw in lines:
            hs4, unit, tail = self.split_head_tail(raw)

            # Компактим числа с пробелами: "12 345" -> "12345"
            tail = self.compact_numbers(tail)
            parts = re.split(r'\s+', tail.strip()) if tail else []

            # Если пришло только 2 значения, считаем что это "импорт" без экспорта (или наоборот),
            # и добавляем два тире слева, чтобы получить 4 поля.
            if len(parts) == 2:
                parts = ['-','-'] + parts

            # Гарантируем длину 4 (quantity_exp, usd_exp, quantity_imp, usd_imp)
            parts = (parts + ['-']*4)[:4]

            recs.append([hs4, unit] + parts)
        return recs

    # Приведение результата к общей структуре
    def decor(self):
        """
        Читает накопленный промежуточный CSV (квартальный),
        превращает кварталы в месяцы (делит значения на 3),
        и формирует стандартный датафрейм проекта.
        """
        def quarters_to_months_az(
            df: pd.DataFrame,
            kv_col: str = "kv",
            numeric_cols=("usd", "quantity", "unit2_quantity", "unit2_usd"),
        ):
            """
            Превращаем запись “квартал” в 3 записи “месяц”.
            ВАЖНО: значения делятся на 3 (то есть это равномерное распределение по месяцам).
            """

            df = df.copy()
            # numeric_cols: заменяем "-" -> NaN, приводим к числам
            for c in numeric_cols:
                df[c] = pd.to_numeric(df[c].replace("-", np.nan), errors="coerce")

            # Маппинг квартала -> стартовый месяц
            start_map = {1: 1, 2: 4, 3: 7, 4: 10}

            # Для каждого квартала создаем список из 3 месяцев
            months_lists = df["kv"].map(start_map).apply(lambda m: [m, m+1, m+2])
            years_lists = df["year"].apply(lambda y: [y, y, y])

            # Повторяем строки 3 раза (на каждый месяц)
            out = df.loc[df.index.repeat(3)].reset_index(drop=True)
            out["month"] = np.concatenate(months_lists.to_numpy())
            out["year"]  = np.concatenate(years_lists.to_numpy())

            # Делим значения на 3 (равномерно по месяцам)
            for c in numeric_cols:
                out[c] = out[c] / 3

            return out

        # Читаем накопленный CSV
        data = quarters_to_months_az(df=pd.read_csv(Azerbaijan.az_params["CSV_PATH_AZ"]))
        # Формируем отчетный период
        data['Отчетный период'] = "01." + data["month"].astype(str).str.zfill(2) + '.' + data["year"].astype(str)

        # Партнер Россия/Беларусь
        if self.belarus:
            data["Страна-партнер"] = "Беларусь"
        else:
            data["Страна-партнер"] = "Россия"

        data["Исходная страна"] = "Азербайджан"

        # HS-уровни: из hs4 делаем hs2 и hs4, остальные NaN
        data["Код товара (10 знаков)"] = np.nan
        data["Код товара (8 знаков)"] = np.nan
        data["Код товара (6 знаков)"] = np.nan
        data["Код товара (2 знака)"] = data["hs4"].astype(str).str.zfill(4).str[:2]
        data["Код товара (4 знака)"] = data["hs4"].astype(str).str.zfill(4)

        # Стоимость: usd * 1000 (в исходнике “тыс. USD”)
        data["Значение (стоимость)"] = data["usd"] * 1000
        # Стоимость по доп.единице
        data["Значение (стоимость) - ДЭИ"] = data["unit2_usd"] * 1000
        # Масса/количество
        data["Значение (масса)"] = data["quantity"]
        data["Единицы стоимости"] = "USD"
        data["Единица объема"] = data["unit"]
        # ДЭИ: в коде доп.единица извлекается эвристикой (см. parse())
        data["ДЭИ, описание"] = data["unit2"]
        data["Дополнительная единица измерения (ДЭИ)"] = data["unit2_quantity"]
        # Тип потока
        data["Направление"] = data["ttype"].replace({"imp": "Импорт", "exp": "Экспорт"})
        # Фильтры нулей/NaN
        data = data[(data["Значение (стоимость)"] != 0) & (data["Значение (стоимость)"] != '0')]
        data = data[~data["Значение (стоимость)"].isna()]
        # Сортировка
        data = data.sort_values(
            by=['year', 'month', 'Направление'],
            ascending=[False, False, True],
            key=lambda col: col.astype(int) if col.name in ['year', 'month'] else col,
            ignore_index=True
        )
        # Убираем строки “Всего” и мусорные коды
        data = data[data["Направление"] != "Всего"]
        data = data[data["Код товара (6 знаков)"] != "00000n"]
        # Финальный набор колонок
        data = data[[
            "Отчетный период", "Исходная страна", "Страна-партнер", "Направление",
            "Код товара (2 знака)", "Код товара (4 знака)", "Код товара (6 знаков)",
            "Код товара (8 знаков)", "Код товара (10 знаков)", "Значение (стоимость)",
            "Единицы стоимости", "Значение (масса)", "Единица объема",
            "Дополнительная единица измерения (ДЭИ)", "ДЭИ, описание", "Значение (стоимость) - ДЭИ"
        ]]

        # Отчетный период -> datetime
        data['Отчетный период'] = pd.to_datetime(data['Отчетный период'], format='%d.%m.%Y')
        # Чистим временный CSV, чтобы не оставлять “мусор” после работы
        if os.path.exists(Azerbaijan.az_params["CSV_PATH_AZ"]):
            os.remove(Azerbaijan.az_params["CSV_PATH_AZ"])
        return data

    # Основной парсинг (скачать -> распаковать -> PDF -> текст -> таблица)
    def parse(self):
        """
        Главный метод:
        - скачивает годовые бюллетени (rar),
        - распаковывает,
        - извлекает PDF по кварталам,
        - вырезает нужный блок страны,
        - преобразует строки в табличный вид,
        - дописывает в ./data/az_trade.csv,
        - затем вызывает decor() и возвращает DataFrame.
        """
        # Если временный CSV уже есть — удаляем
        if os.path.exists(Azerbaijan.az_params["CSV_PATH_AZ"]):
            os.remove(Azerbaijan.az_params["CSV_PATH_AZ"])

        # Скачиваем страницу со ссылками
        res = requests.get(Azerbaijan.az_params["url"])
        soup = BeautifulSoup(res.text, "lxml")
        # Ищем ссылки на бюллетени по конкретному классу
        links = soup.find_all(
            name='a',
            attrs={"class": "font-size_small text-color-main_color text-decoration-underline font-weight-bold d-flex align-items-center"}
        )
        hrefs = [link.get("href") for link in links]

        # Собираем словарь: год -> ссылка
        hrefs_years = {href[href.find("20"):href.find("20") + 4]: href for href in hrefs}
        # Римские кварталы -> номер квартала
        roman_map = {'I': 1, "II": 2, "III": 3, "IV": 4}
        # Первый проход: нужен, чтобы записать header=True только один раз
        first_iter = True

        # Прогресс-бар по годам от min(years) до max(years)
        pb1 = tqdm(range(int(self.years[0]), int(self.years[-1]) + 1))
        for year in pb1:
            pb1.set_description(f"Собираю {year} год")
            # Гарантируем папку downloads
            if not os.path.exists("./data/files"):
                os.makedirs("./data/files")
            # Скачиваем rar за год
            with open(f"./data/files/{year}.rar", "wb") as file:
                file.write(requests.get(hrefs_years[str(year)]).content)
            # Распаковываем rar с помощью bsdtar (ВАЖНО: должен быть установлен)
            subprocess.run(
                ["bsdtar", "-xf", f"./data/files/{year}.rar", "-C", f"./data/files/"],
                check=True
            )

            # Идем по кварталам
            for kv in ['I', "II", "III", "IV"]:
                # для 2025 нет III/IV (в коде захардкожено)
                if year == 2025 and kv in ["III", "IV"]:
                    continue

                text_lines = []

                # Блоки поиска PDF
                # ВАЖНО: здесь куча "костылей" из-за разных названий папок.
                # Это главная хрупкая часть парсера: структура архива менялась по годам.
                if year == 2025:
                    pdf_path = f"./data/files/Bulleten_{year}/Bulleten _{year}_{kv}_Rub/10. Cədvəl 8.pdf"
                    cnt = 0
                    while cnt != 4:
                        try:
                            # читаем PDF и собираем строки
                            with open(pdf_path, "rb") as f:
                                reader = PyPDF2.PdfReader(f)
                                for page in reader.pages:
                                    text = page.extract_text()
                                    if text:
                                        text_lines.extend(text.splitlines())
                            break
                        except FileNotFoundError:
                            # пытаемся альтернативные варианты путей (разные регистры/дефисы)
                            if cnt == 0:
                                pdf_path = f"./data/files/Bulleten_{year}/Bulleten_{year}_{kv}_Rub/10. Cədvəl 8.pdf"
                            elif cnt == 1:
                                pdf_path = f"./data/files/Bulleten_{year}/Bulleten_{year}_{kv}_rub/10. Cədvəl 8.pdf"
                            elif cnt == 2:
                                pdf_path = f"./data/files/Bulleten_{year}/Bulleten-{year}_{roman_map[kv]}_rub/10. Cədvəl 8.pdf"
                            elif cnt == 3:
                                pdf_path = f"./data/files/Bulleten_{year}/Bulleten-{year}_{roman_map[kv]}_Rub/10. Cədvəl 8.pdf"
                            cnt += 1
                            continue

                elif year == 2024:
                    pdf_path = f"./data/files/Bulleten _{year}_{kv}_Rub/10. Cədvəl 8.pdf"
                    cnt = 0
                    while cnt != 4:
                        try:
                            with open(pdf_path, "rb") as f:
                                reader = PyPDF2.PdfReader(f)
                                for page in reader.pages:
                                    text = page.extract_text()
                                    if text:
                                        text_lines.extend(text.splitlines())
                            break
                        except FileNotFoundError:
                            if cnt == 0:
                                pdf_path = f"./data/files/Bulleten_{year}_{kv}_Rub/10. Cədvəl 8.pdf"
                            elif cnt == 1:
                                pdf_path = f"./data/files/Bulleten_{year}_{kv}_rub/10. Cədvəl 8.pdf"
                            elif cnt == 2:
                                pdf_path = f"./data/files/Bulleten-{year}_{roman_map[kv]}_rub/10. Cədvəl 8.pdf"
                            elif cnt == 3:
                                pdf_path = f"./data/files/Bulleten-{year}_{roman_map[kv]}_Rub/10. Cədvəl 8.pdf"
                            cnt += 1
                            continue

                else:
                    # Для старых лет PDF лежит глубже и иногда внутри еще одного rar
                    rar_path = f"./data/files/Bulleten_{year}/Bulleten_{year}_{kv} rub/Bulleten_{year}_{kv} rub.rar"

                    if year == 2023:
                        rar_path = f"./data/files/Billuten {year}_illik/Bulleten_{year}_{kv} rub/Bulleten {year}_{kv} rub.rar"
                        try:
                            subprocess.run(
                                ["bsdtar", "-xf", rar_path, "-C", f"./data/files/Billuten {year}_illik/Bulleten_{year}_{kv} rub/"],
                                check=True
                            )
                            pdf_path = f"./data/files/Billuten {year}_illik/Bulleten_{year}_{kv} rub/10. Cədvəl 8.pdf"
                        except subprocess.CalledProcessError:
                            pdf_path = f"./data/files/Billuten {year}_illik/Bulleten_{year}_{kv} rub/10. Cədvəl 8.pdf"

                    elif year == 2019:
                        os.makedirs(f"./data/files/Bulleten-{year}/{year}_{roman_map[kv]}")
                        rar_path = f"./data/files/Bulleten-{year}/{year}_{roman_map[kv]} RUB.rar"
                        cnt = 0
                        while cnt != 1:
                            try:
                                subprocess.run(
                                    ["bsdtar", "-xf", rar_path, "-C", f"./data/files/Bulleten-{year}/{year}_{roman_map[kv]}"],
                                    check=True
                                )
                                pdf_path = f"./data/files/Bulleten-{year}/{year}_{roman_map[kv]}/10. Cədvəl 8.pdf"
                                break
                            except subprocess.CalledProcessError:
                                rar_path = f"./data/files/Bulleten-{year}/{year}_{roman_map[kv]} Rub.rar"
                                cnt += 1
                                continue

                    elif year == 2020:
                        os.makedirs(f"./data/files/BULLETEN_{year}_Illik/{year}_{roman_map[kv]}")
                        rar_path = f"./data/files/BULLETEN_{year}_Illik/{year}_{roman_map[kv]} RUB.rar"
                        cnt = 0
                        while cnt != 2:
                            try:
                                subprocess.run(
                                    ["bsdtar", "-xf", rar_path, "-C", f"./data/files/BULLETEN_{year}_Illik/{year}_{roman_map[kv]}"],
                                    check=True
                                )
                                pdf_path = f"./data/files/BULLETEN_{year}_Illik/{year}_{roman_map[kv]}/10. Cədvəl 8.pdf"
                                break
                            except subprocess.CalledProcessError:
                                rar_path = f"./data/files/BULLETEN_{year}_Illik/{year}-{roman_map[kv]} RUB.rar"
                                cnt += 1
                                continue

                    elif year == 2021:
                        os.makedirs(f"./data/files/Bulleten-{year}/{year}_{roman_map[kv]}")
                        rar_path = f"./data/files/Bulleten-{year}/Bulleten_{year}_{kv} RUB.rar"

                        if kv == "III":
                            # особый кейс: вложенный rar внутри распакованной папки
                            rar_path = f"./data/files/Bulleten-{year}/BULLETEN_{year}_{roman_map[kv]} RUB.rar"
                            subprocess.run(
                                ["bsdtar", "-xf", rar_path, "-C", f"./data/files/Bulleten-{year}/{year}_{roman_map[kv]}"],
                                check=True
                            )
                            rar_path = f"./data/files/Bulleten-{year}/{year}_{roman_map[kv]}/BULLETEN_{year}_{roman_map[kv]} RUB/Bulleten_{year}_{kv} RUB.rar"
                            subprocess.run(
                                ["bsdtar", "-xf", rar_path, "-C", f"./data/files/Bulleten-{year}/{year}_{roman_map[kv]}"],
                                check=True
                            )
                            pdf_path = f"./data/files/Bulleten-{year}/{year}_{roman_map[kv]}/10. Cədvəl 8.pdf"

                        else:
                            cnt = 0
                            while cnt != 2:
                                try:
                                    subprocess.run(
                                        ["bsdtar", "-xf", rar_path, "-C", f"./data/files/Bulleten-{year}/{year}_{roman_map[kv]}"],
                                        check=True
                                    )
                                    pdf_path = f"./data/files/Bulleten-{year}/{year}_{roman_map[kv]}/10. Cədvəl 8.pdf"
                                    break
                                except subprocess.CalledProcessError:
                                    if cnt == 0:
                                        rar_path = f"./data/files/Bulleten-{year}/Bulleten_{year}_{kv} RUB/Bulleten_{year}_{kv} RUB.rar"
                                    cnt += 1
                                    continue

                    else:
                        # базовый вариант: распаковываем rar_path и берем PDF
                        subprocess.run(
                            ["bsdtar", "-xf", rar_path, "-C", f"./data/files/Bulleten_{year}/Bulleten_{year}_{kv} rub/"],
                            check=True
                        )
                        pdf_path = f"./data/files/Bulleten_{year}/Bulleten_{year}_{kv} rub/10. Cədvəl 8.pdf"

                    # читаем PDF
                    try:
                        with open(pdf_path, "rb") as f:
                            reader = PyPDF2.PdfReader(f)
                            for page in reader.pages:
                                text = page.extract_text()
                                if text:
                                    text_lines.extend(text.splitlines())
                    except FileNotFoundError:
                        print(f"skip: {year} {kv}")
                        continue

                # Вырезаем блок по стране
                # Здесь жёстко завязано на маркеры в тексте PDF:
                # - Беларусь: от "CƏMİBELARUS" до "CƏMİBELÇİKA"
                # - Россия:   от "CƏMİRUSİYA"  до "CƏMİSALVADOR"
                # Если в PDF изменятся эти слова — парсер перестанет находить блок.
                if self.belarus:
                    try:
                        text_lines = text_lines[text_lines.index("CƏMİBELARUS") + 2: text_lines.index("CƏMİBELÇİKA")]
                    except ValueError:
                        print(f"skip: {year} {kv}")
                        return
                else:
                    try:
                        text_lines = text_lines[text_lines.index("CƏMİRUSİYA") + 2: text_lines.index("CƏMİSALVADOR")]
                    except ValueError:
                        print(f"skip: {year} {kv}")
                        return

                # Очистка мусора + преобразование строк в таблицу
                # Убираем мусорные строки (шапки/подвалы/служебный текст)
                cleaned_lines = [item for item in text_lines if item.strip() not in Azerbaijan.az_params["to_remove"]]

                # Преобразуем строки в записи [hs4, unit, exp_qty, exp_usd, imp_qty, imp_usd]
                cleaned_lines = self.rows_to_df(cleaned_lines)

                df = pd.DataFrame(
                    cleaned_lines,
                    columns=["hs4", "unit", "quantity_exp", "usd_exp", "quantity_imp", "usd_imp"]
                )

                df["year"] = year
                df["kv"] = roman_map[kv]  # квартал как число 1..4

                # Разделяем экспорт и импорт в разные строки
                exp = (
                    df[["hs4","unit","year","kv","quantity_exp","usd_exp"]]
                    .rename(columns={"quantity_exp":"quantity", "usd_exp":"usd"})
                )
                exp["ttype"] = "exp"

                imp = (
                    df[["hs4","unit","year","kv","quantity_imp","usd_imp"]]
                    .rename(columns={"quantity_imp":"quantity", "usd_imp":"usd"})
                )
                imp["ttype"] = "imp"

                out = pd.concat([exp, imp], ignore_index=True, copy=False)
                out = out[["hs4","unit","ttype","quantity","usd","year","kv"]]

                # Убираем строки, где unit заканчивается знаком © (мусор из PDF)
                out = out[out["unit"].str[-1] != "©"]

                # Эвристика для "второй единицы" (ДЭИ)
                # PDF иногда размещает вторую единицу в следующей строке без hs4.
                # Ниже логика пытается приклеить следующую строку как unit2 к предыдущей.
                out["unit2"] = '-'
                out["unit2_quantity"] = '-'
                out["unit2_usd"] = '-'
                out["ttype2"] = '-'

                # mask = hs4 отсутствует (скорее всего "вторая строка" для ДЭИ)
                mask = (out["hs4"].isna())

                # prev_mask: текущая строка hs4 есть, а следующая hs4 = NaN
                # (то есть следующая строка считается доп.информацией)
                prev_mask = mask.shift(-1, fill_value=False) & ~mask

                # Переносим значения из следующей строки в unit2-поля текущей
                out.loc[prev_mask, 'unit2'] = out['unit'].shift(-1)
                out.loc[prev_mask, 'unit2_quantity'] = out['quantity'].shift(-1)
                out.loc[prev_mask, 'unit2_usd'] = out['usd'].shift(-1)
                out.loc[prev_mask, 'ttype2'] = out['ttype'].shift(-1)

                # Если в основной строке usd='-' но в unit2_usd есть число — значит надо “перенести” unit2 в основную
                mask_2 = ((out["usd"] == '-') & (out["unit2_usd"] != '-'))
                out.loc[mask_2, "unit"] = out["unit2"]
                out.loc[mask_2, "quantity"] = out["unit2_quantity"]
                out.loc[mask_2, "usd"] = out["unit2_usd"]
                out.loc[mask_2, "ttype"] = out["ttype2"]
                # чистим unit2-поля
                out.loc[mask_2, "unit2"] = '-'
                out.loc[mask_2, "unit2_quantity"] = '-'
                out.loc[mask_2, "unit2_usd"] = '-'
                out.loc[mask_2, "ttype2"] = '-'

                # Если unit2 есть, но unit2_usd='-' — считаем что это мусор и чистим
                mask_3 = ((out["unit2"] != '-') & (out["unit2_usd"] == '-'))
                out.loc[mask_3, "unit2"] = '-'
                out.loc[mask_3, "unit2_quantity"] = '-'
                out.loc[mask_3, "unit2_usd"] = '-'
                out.loc[mask_3, "ttype2"] = '-'

                # Финальная чистка: оставляем только строки, где usd не '-' и hs4 не NaN
                out = out[out["usd"] != '-']
                out = out[~out["hs4"].isna()]

                # Дописываем в CSV (накопление по кварталам)
                out.to_csv(
                    Azerbaijan.az_params["CSV_PATH_AZ"],
                    mode='a',            # append
                    index=False,
                    header=first_iter,   # заголовок только в первой записи
                    encoding="utf-8-sig"
                )
                first_iter = False

            # После обработки всех кварталов года:
            # Удаляем папку ./data/files целиком (чистим, чтобы не мешало следующему году)
            shutil.rmtree(f"./data/files/")
        # После завершения: приводим к стандарту и возвращаем DataFrame
        return self.decor()
