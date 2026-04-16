import pandas as pd                 
import requests                     
from fake_useragent import UserAgent # Генерация случайного User-Agent (иногда помогает обходить блокировки)
import numpy as np                  


class Brazil:
    """
    Парсер Бразилии через официальный API Comex Stat (MDIC).

    Источник:
    - API endpoint: https://api-comexstat.mdic.gov.br/general
    - В запросе задаются:
        - период yearStart/yearEnd
        - детализация (monthDetail=true)
        - метрики FOB, KG, Statistic
        - страна-партнер через filterArray (item=["676"] — по коду страны в API)

    Что делает:
    1) GET-запрос к API (ожидает JSON со списками exports и imports)
    2) Склеивает exports+imports в один DataFrame
    3) decor(): фильтрует по годам, сортирует, приводит к схеме проекта (HS2/4/6 + значения)
    """

    brazil_params = {
        # URL уже содержит закодированный filter JSON.
        # ВНИМАНИЕ: это очень “тяжелая” и хрупкая строка.
        # Любое изменение структуры API может ее сломать, но пока за полгода работало все прекрасно.
        "url": "https://api-comexstat.mdic.gov.br/general?filter=%7B%22yearStart%22:%222019%22,%22yearEnd%22:%222025%22,%22typeForm%22:3,%22typeOrder%22:1,%22filterList%22:%5B%7B%22id%22:%22noPaisen%22,%22text%22:%22Country%22,%22route%22:%22/en/location/countries%22,%22type%22:%221%22,%22group%22:%22gerais%22,%22groupText%22:%22General%22,%22hint%22:%22fieldsForm.general.noPais.description%22,%22placeholder%22:%22Country%22%7D%5D,%22filterArray%22:%5B%7B%22item%22:%5B%22676%22%5D,%22idInput%22:%22noPaisen%22%7D%5D,%22rangeFilter%22:%5B%5D,%22detailDatabase%22:%5B%7B%22id%22:%22noPaisen%22,%22text%22:%22Country%22,%22group%22:%22gerais%22,%22groupText%22:%22General%22%7D,%7B%22id%22:%22noSh2en%22,%22text%22:%22Chapter%20(SH2)%22,%22parentId%22:%22coSh2%22,%22parent%22:%22SH2%20Code%22,%22group%22:%22sh%22,%22groupText%22:%22Harmonized%20System%20(HS)%22%7D,%7B%22id%22:%22noSh4en%22,%22text%22:%22Heading%20(SH4)%22,%22parentId%22:%22coSh4%22,%22parent%22:%22SH4%20Code%22,%22group%22:%22sh%22,%22groupText%22:%22Harmonized%20System%20(HS)%22%7D,%7B%22id%22:%22noSh6en%22,%22text%22:%22Subheading%20(SH6)%22,%22parentId%22:%22coSh6%22,%22parent%22:%22SH6%20Code%22,%22group%22:%22sh%22,%22groupText%22:%22Harmonized%20System%20(HS)%22%7D,%7B%22id%22:%22noNcmen%22,%22text%22:%22NCM%22,%22parentId%22:%22coNcm%22,%22parent%22:%22NCM%20Code%22,%22group%22:%22sh%22,%22groupText%22:%22Harmonized%20System%20(HS)%22%7D%5D,%22monthDetail%22:true,%22metricFOB%22:true,%22metricKG%22:true,%22metricStatistic%22:true,%22metricFreight%22:false,%22metricInsurance%22:false,%22metricCIF%22:false,%22monthStart%22:%2201%22,%22monthEnd%22:%2212%22,%22formQueue%22:%22general%22,%22langDefault%22:%22en%22,%22monthStartName%22:%22January%22,%22monthEndName%22:%22December%22%7D",
        "method": "GET",
        # payload
        "payload": {
            "yearStart": "2019",
            "yearEnd": "2025",
            "typeForm": 3,
            "typeOrder": 1,
            "filterList": [{
                "id": "noPaisen",
                "text": "Country",
                "route": "/en/location/countries",
                "type": "1",
                "group": "gerais",
                "groupText": "General",
                "hint": "fieldsForm.general.noPais.description",
                "placeholder": "Country"
            }],
            "filterArray": [{
                "item": ["676"],        # код страны (Россия?) для фильтра Country
                "idInput": "noPaisen"
            }],
            "rangeFilter": [],
            "detailDatabase": [
                {"id": "noPaisen", "text": "Country", "group": "gerais", "groupText": "General"},
                {"id": "noSh2en", "text": "Chapter (SH2)", "parentId": "coSh2", "parent": "SH2 Code", "group": "sh", "groupText": "Harmonized System (HS)"},
                {"id": "noSh4en", "text": "Heading (SH4)", "parentId": "coSh4", "parent": "SH4 Code", "group": "sh", "groupText": "Harmonized System (HS)"},
                {"id": "noSh6en", "text": "Subheading (SH6)", "parentId": "coSh6", "parent": "SH6 Code", "group": "sh", "groupText": "Harmonized System (HS)"},
                {"id": "noNcmen", "text": "NCM", "parentId": "coNcm", "parent": "NCM Code", "group": "sh", "groupText": "Harmonized System (HS)"}
            ],
            "monthDetail": True,
            "metricFOB": True,
            "metricKG": True,
            "metricStatistic": True,
            "metricFreight": False,
            "metricInsurance": False,
            "metricCIF": False,
            "monthStart": "01",
            "monthEnd": "12",
            "formQueue": "general",
            "langDefault": "en",
            "monthStartName": "January",
            "monthEndName": "December"
        },

        # Заголовки запроса
        "headers": {
            "accept": "application/json, text/plain, */*",
            "accept-encoding": "gzip, deflate, br, zstd",
            "accept-language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36"
        }
    }

    def __init__(self, years):
        # years: список лет (строки), например ["2019","2020",...,"2025"]
        self.years = years

    # Приведение к общей структуре
    def decor(self, data):
        """
        На вход: “сырой” DataFrame из JSON exports+imports.
        На выход: DataFrame со стандартными колонками проекта.
        """
        # Оставляем только нужные годы
        data = data[data["coAno"].isin(self.years)]
        # Сортируем: год убывание, месяц убывание, направление (type) по алфавиту
        data = data.sort_values(
            by=['coAno', 'coMes', 'type'],
            ascending=[False, False, True],
            key=lambda col: col.astype(int) if col.name in ['coAno','coMes'] else col,
            ignore_index=True
        )
        # Формируем "Отчетный период" как строку "01.MM.YYYY"
        data["coAno"] = "01." + data["coMes"] + '.' + data["coAno"]

        # Проставляем страны и единицы
        data["first_country"] = "Бразилия"
        data["noPaisen"] = "Россия"

        # Валюта
        data["usd"] = "USD"

        # Единица массы (но вы называете колонку "Единица объема" дальше — это концептуально странно)
        data["weigth_unid"] = "килограмм"

        # Нормализация текстовых описаний "noUnid" (это ДЭИ-описание)
        data["noUnid"].replace({
            "TONELADA METRICA LIQUIDA": "тонн",
            "QUILOGRAMA LIQUIDO": "килограмм жидкий",
            "NUMERO (UNIDADE)": "штук",
            "METRO CUBICO": "кубических метров",
            "PARES": "пар",
            "DUZIA": "дюжин",
            "LITRO": "литров",
            "METRO QUADRADO": "квадратных метров",
            "QUILATE": "карат",
            "MILHEIRO": "тысяч"
        }, inplace=True)

        # Приводим метрики к float
        data["vlFob"] = data["vlFob"].astype(float)         # стоимость FOB
        data["qtEstat"] = data["qtEstat"].astype(float)     # "статистическое количество" (обычно доп.единица)
        data["kgLiquido"] = data["kgLiquido"].astype(float) # масса нетто

        # HS8/HS10 отсутствуют — ставим NaN
        data["coSh8"] = np.nan
        data["coSh10"] = np.nan

        # Выбираем и упорядочиваем колонки
        data = data[[
            "coAno", "first_country", "noPaisen", "type",
            "coSh2", "coSh4", "coSh6", "coSh8", "coSh10",
            "vlFob", "usd",
            "kgLiquido", "weigth_unid",
            "qtEstat", "noUnid"
        ]]

        # Переименовываем в стандарт проекта
        data = data.rename(columns={
            "coAno": "Отчетный период",
            "first_country": "Исходная страна",
            "noPaisen": "Страна-партнер",
            "type": "Направление",
            "coSh2": "Код товара (2 знака)",
            "coSh4": "Код товара (4 знака)",
            "coSh6": "Код товара (6 знаков)",
            "coSh8": "Код товара (8 знаков)",
            "coSh10": "Код товара (10 знаков)",
            "vlFob": "Значение (стоимость)",
            "usd": "Единица стоимости",
            "kgLiquido": "Значение (масса)",
            "weigth_unid": "Единица объема",
            "qtEstat": "Дополнительная единица измерения (ДЭИ)",
            "noUnid": "ДЭИ, описание"
        })

        # Стоимость в ДЭИ здесь не вычисляется
        data["Значение (стоимость) - ДЭИ"] = np.nan
        # Приводим период к datetime
        data['Отчетный период'] = pd.to_datetime(data['Отчетный период'], format='%d.%m.%Y')
        print("Парсинг успешно завершен. Перехожу к составлению и оформлению итоговой таблицы.")
        return data

    # Основной метод запроса к API
    def parse(self) -> pd.DataFrame:
        """
        Делает запрос к API, достает exports и imports, добавляет поле type, возвращает decor().
        """
        # Генерируем случайный User-Agent
        user = UserAgent()
        Brazil.brazil_params['headers']['user-agent'] = user.random
        payload = Brazil.brazil_params['payload'].copy()
        payload["yearStart"] = self.years[0]
        payload["yearEnd"] = self.years[-1]

        # GET-запрос
        res = requests.get(
            Brazil.brazil_params['url'],
            headers=Brazil.brazil_params['headers'],
            json=Brazil.brazil_params['payload']
        )

        # Обработка ошибок
        if res.status_code >= 400:
            print("Код состояния HTTP:", res.status_code)
            raise Exception("Ошибка клиента или сервера.")
        else:
            print("Код состояния HTTP: ", res.status_code, ". Успешно.", sep='')

        # Достаем списки
        exports = res.json()['data']['list']['exports']
        imports = res.json()['data']['list']['imports']

        # Проставляем направление
        for tn in exports:
            tn["type"] = "Экспорт"
        for tn in imports:
            tn["type"] = "Импорт"

        # Склеиваем в один DataFrame
        data = pd.DataFrame(exports + imports)
        # Приводим к стандарту
        return self.decor(data)
