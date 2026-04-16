import pandas as pd
import numpy as np
import os
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm


class USA:
    """
    Парсер внешней торговли США (US Census API) по HS10 для торговли с Россией (CTY_CODE=4621).

    Источник данных:
      - US Census International Trade API (timeseries)
      - два эндпоинта: exports/hs и imports/hs
      - выгрузка по маске HS10 (E/I_COMMODITY = "<digit>*"), где digit = 0..9

    Общая схема:
      1) parse():
         - удаляем временный CSV, если есть
         - собираем список прокси (free-proxy-list)
         - для exports и imports:
             * по первому разряду HS (0..9) дергаем API
             * пытаемся подобрать живой прокси
             * сохраняем «сырые» строки в CSV
      2) decor():
         - приводим «сырые» поля к стандартному формату проекта:
           период, страна-источник/партнер, направление, HS2/4/6/8/10,
           стоимость/масса/ДЭИ, сортировка, datetime
    """

    usa_params = {
        # Базовый URL для экспорта: запрашиваем поля + страну (CTY_CODE=4621) + уровень HS10 + период
        # E_COMMODITY добавляется в конце (в parse() подставляется digit + "*")
        "base_url_exports": (
            "https://api.census.gov/data/timeseries/intltrade/exports/hs?"
            "get=E_COMMODITY,E_COMMODITY_LDESC,ALL_VAL_MO,QTY_1_MO,QTY_2_MO,UNIT_QY1,UNIT_QY2,CTY_NAME"
            "&CTY_CODE=4621&COMM_LVL=HS10&time=from+2019-01+to+2025-12&E_COMMODITY="
        ),

        # Базовый URL для импорта (аналогично, только поля и I_COMMODITY)
        "base_url_imports": (
            "https://api.census.gov/data/timeseries/intltrade/imports/hs?"
            "get=I_COMMODITY,I_COMMODITY_LDESC,GEN_VAL_MO,GEN_QY1_MO,GEN_QY2_MO,UNIT_QY1,UNIT_QY2,CTY_NAME"
            "&CTY_CODE=4621&COMM_LVL=HS10&time=from+2019-01+to+2025-12&I_COMMODITY="
        ),

        # Временный CSV для промежуточного хранения «сырого» ответа API
        "CSV_PATH_USA": "./data/usa_trade.csv",

        # Сайт со списком бесплатных прокси (HTML-таблица)
        "proxy_url": "https://free-proxy-list.net/ru/"
    }

    def __init__(self, years):
        """
        years: список лет строками (например ["2023","2024","2025"]).

        В данном парсере years используется только на этапе дальнейшей фильтрации/унификации
        (сейчас в коде фильтрации по years нет, но поле хранится для совместимости с общей архитектурой).
        """
        self.years = years

    def decor(self):
        """
        Приведение сырого CSV к единому формату проекта.

        ВАЖНО: текущая реализация предполагает экспортные названия колонок
        (E_COMMODITY, ALL_VAL_MO, QTY_1_MO, QTY_2_MO). Если в CSV смешаны exports+imports,
        то для imports колонки будут называться иначе (I_COMMODITY, GEN_VAL_MO, GEN_QY1_MO, GEN_QY2_MO),
        и этот декор нужно будет расширить/нормализовать.
        """
        data = pd.read_csv(USA.usa_params["CSV_PATH_USA"])

        # Разбираем год/месяц из поля time формата YYYY-MM
        data["Year"] = data["time"].str[:4]
        data["Month"] = data["time"].str[5:]

        # Стандартный формат периода проекта: 01.MM.YYYY
        data['Отчетный период'] = "01." + data["Month"] + '.' + data["Year"]

        # Метаданные стран
        data["Страна-партнер"] = "Россия"
        data["Исходная страна"] = "CША"

        # HS-коды:
        # в исходном ответе для exports это E_COMMODITY (HS10)
        data["Код товара (10 знаков)"] = data["E_COMMODITY"].astype(str).str.zfill(10)
        data["Код товара (8 знаков)"] = data["Код товара (10 знаков)"].str[:8]
        data["Код товара (6 знаков)"] = data["Код товара (10 знаков)"].str[:6]
        data["Код товара (4 знака)"] = data["Код товара (10 знаков)"].str[:4]
        data["Код товара (2 знака)"] = data["Код товара (10 знаков)"].str[:2]

        # Привязка показателей:
        # стоимость/масса/ДЭИ берутся из полей экспорта
        data["Значение (стоимость)"] = data["ALL_VAL_MO"]
        data["Значение (масса)"] = data["QTY_1_MO"]

        # Валюта и единицы измерения
        data["Единицы стоимости"] = "USD"
        data["Единица объема"] = data["UNIT_QY1"]                # единица QTY_1
        data["ДЭИ, описание"] = data["UNIT_QY2"]                 # единица QTY_2 (доп. единица)
        data["Дополнительная единица измерения (ДЭИ)"] = data["QTY_2_MO"]

        # Направление по ttype
        data["Направление"] = data["ttype"].replace({"imports": "Импорт", "exports": "Экспорт"})

        # Удаляем нулевые/пустые стоимости
        data = data[(data["Значение (стоимость)"] != 0) & (data["Значение (стоимость)"] != '0')]
        data = data[~data["Значение (стоимость)"].isna()]

        # Сортировка: новые периоды сверху, затем Экспорт/Импорт
        data = data.sort_values(
            by=['Year', 'Month', 'Направление'],
            ascending=[False, False, True],
            key=lambda col: col.astype(int) if col.name in ['Year', 'Month'] else col,
            ignore_index=True
        )

        # В этом источнике нет отдельного показателя "стоимость - ДЭИ"
        data["Значение (стоимость) - ДЭИ"] = np.nan

        # Финальный порядок колонок (стандарт проекта)
        data = data[[
            "Отчетный период", "Исходная страна", "Страна-партнер", "Направление",
            "Код товара (2 знака)", "Код товара (4 знака)", "Код товара (6 знаков)",
            "Код товара (8 знаков)", "Код товара (10 знаков)", "Значение (стоимость)",
            "Единицы стоимости", "Значение (масса)", "Единица объема",
            "Дополнительная единица измерения (ДЭИ)", "ДЭИ, описание", "Значение (стоимость) - ДЭИ"
        ]]

        # Перевод периода в datetime для дальнейшей работы/мерджей
        data['Отчетный период'] = pd.to_datetime(data['Отчетный период'], format='%d.%m.%Y')
        return data

    def get_proxy(self) -> list:
        """
        Парсит страницу free-proxy-list и возвращает список прокси в формате "ip:port".

        Берем первые ~100 строк таблицы (индексы 1..99), потому что 0-я строка — заголовок.
        """
        proxies = BeautifulSoup(requests.get(USA.usa_params["proxy_url"]).text, "lxml")
        proxies = proxies.find_all(name="tr")
        proxies_list = [
            proxies[i].find_all(name='td')[0].text + ':' + proxies[i].find_all(name='td')[1].text
            for i in range(1, 100)
        ]
        return proxies_list

    def parse(self) -> pd.DataFrame:
        """
        Основной парсер.

        Идея: US Census API может блочить/дросселировать частые запросы -> используем бесплатные прокси.
        Алгоритм:
          - получаем список прокси
          - для exports и imports:
              * перебираем digit=0..9, делаем запрос по маске "<digit>*"
              * если ответ не ok — ищем другой прокси
              * если нашли рабочий — запоминаем best_proxy и используем дальше
              * сохраняем json-таблицу в CSV (первые строки — заголовки, затем данные)
          - после выгрузки запускаем decor() и удаляем временный CSV
        """
        # Чистим временный CSV
        if os.path.exists(USA.usa_params["CSV_PATH_USA"]):
            os.remove(USA.usa_params["CSV_PATH_USA"])

        proxies_list = self.get_proxy()
        first_iter = True
        best_proxy = ''
        r = None

        # Перебираем направления: экспорт и импорт
        for ttype in ["exports", "imports"]:
            # 0..9 — «первый разряд» HS10 (маска digit + "*")
            pb = tqdm(range(10), unit="batch", desc=f"Собираю HS10 (первый разряд), {ttype}")
            for digit in pb:
                print("Ищем прокси.")

                # Подбор прокси: перебираем список до первого успешного ответа
                for proxy_idx in range(len(proxies_list)):
                    proxy = proxies_list[proxy_idx]

                    try:
                        # Если уже был успешный запрос и CSV не первый — пробуем использовать лучший прокси
                        if not first_iter and r and r.ok:
                            proxy = best_proxy
                            proxy_idx -= 1  # формально не нужно, но оставлено как в исходнике

                        # Запрос к US Census API через прокси
                        r = requests.get(
                            url=USA.usa_params[f"base_url_{ttype}"] + f"{digit}%2A",  # %2A = '*'
                            proxies={"http": f"http://{proxy}", "https": f"http://{proxy}"},
                            headers={"User-Agent": "Mozilla/5.0"},
                            timeout=60,
                        )

                        print("Прокси найден:", proxy)

                        # Если запрос успешен — фиксируем best_proxy и выходим из цикла подбора
                        if r.ok:
                            best_proxy = proxy
                            break
                        else:
                            print("Прокси мертв. Продолжаем перебор")

                    except ConnectionResetError:
                        # Бывает, что прокси живой, но соединение рвется — пробуем еще
                        print("Прокси работает, но соединение разорвано. Попробуем еще раз")
                        continue
                    except OSError:
                        # Любые сетевые/системные ошибки прокси — идем дальше
                        continue

                # Ответ API — это "таблица": первая строка заголовки, дальше данные
                table = r.json()
                df = pd.DataFrame(table[1:], columns=table[0])

                # Добавляем направление (exports/imports), чтобы потом корректно собрать общий датасет
                df["ttype"] = ttype

                # Аппендим в CSV
                df.to_csv(
                    USA.usa_params["CSV_PATH_USA"],
                    mode="a",
                    index=False,
                    header=first_iter,
                    encoding="utf-8-sig",
                )
                first_iter = False

        print("Парсинг успешно завершен. Перехожу к составлению и оформлению итоговой таблицы.")
        data = self.decor()

        # Удаляем временный CSV, чтобы не засорять data/
        if os.path.exists(USA.usa_params["CSV_PATH_USA"]):
            os.remove(USA.usa_params["CSV_PATH_USA"])

        return data
