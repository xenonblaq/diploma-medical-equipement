import os                                  # работа с файлами (проверка/удаление временного CSV)
import pandas as pd                        
import numpy as np                         
from tqdm import tqdm                      
import requests                            # HTTP-запросы к HK Trade API и BIS
import xml.etree.ElementTree as ET         # парсинг XML-ответа BIS (курсы валют)
from datetime import datetime              # сегодняшняя дата для курса HKD/USD


class HongKong:
    """
    Парсер торговли Гонконга с РФ по HS6 через API tradeidds.censtatd.gov.hk.

    Источники:
    1) HK Trade API (tradeidds.censtatd.gov.hk) — отдает Value и Quantity по HS6
    2) BIS API (stats.bis.org) — курсы HKD->USD (серия WS_XRU)

    Внешние файлы (обязательные):
    - ./data/HSCodeandDescription.csv  (колонка 'Code' с HS6)
      Без него парсер не знает, какие коды дергать.
    """

    hk_params = {
        # Базовый URL:
        # - sv=VCM,QCM (стоимость + количество)
        # - freq=M (месячно)
        # - period=201901,202505 (начало/конец)  <-- далее мы заменяем на динамические start_period/final_period
        # - ttype=1 (imports)                   <-- далее мы меняем на exports (ttype=4)
        # - coclass=C&co=RU (партнер RU)        <-- для exports мы заменяем на ccclass/cc
        # - codeclass=HKHS6&code=               <-- дальше дописывается список HS6
        "url": "https://tradeidds.censtatd.gov.hk/api/get?lang=EN&sv=VCM,QCM&freq=M&period=201901,202505&ttype=1&coclass=C&co=RU&codeclass=HKHS6&code=",
        "CSV_PATH_HK": "./data/hk_trade.csv"   # временный CSV с сырыми данными, потом decor() приводит к финальному виду
    }

    def __init__(self, years, final_month="06"):
        # years: список строк-годов (например ["2019",...,"2025"])
        self.years = years

        # final_month: последний доступный месяц в последнем году (например "06" для 2025-06)
        self.final_month = final_month


    # Курсы HKD->USD (BIS)
    def currency(self):
        """
        Тянет курсы BIS (WS_XRU) для валюты HKD.
        Возвращает DataFrame:
            Отчетный период (01.MM.YYYY), Курс, Единицы стоимости (CURRENCY из BIS)
        """

        curr = "HK.HKD"
        end_period = datetime.today().strftime("%Y-%m")
        url = f"https://stats.bis.org/api/v1/data/BIS,WS_XRU,1.0/M.{curr}.A/all?startPeriod=2019-01&endPeriod={end_period}"
        print(url)

        resp = requests.get(url)
        resp.raise_for_status()

        root = ET.fromstring(resp.content)
        series = root.find('.//Series')
        currency = series.get('CURRENCY')

        records = []
        for obs in series.findall('Obs'):
            period = obs.get('TIME_PERIOD')         # YYYY-MM
            value  = float(obs.get('OBS_VALUE'))    # курс
            records.append({
                'Отчетный период': period,
                'Курс': value,
                'Единицы стоимости': currency
            })

        df = pd.DataFrame(records)

        # YYYY-MM -> 01.MM.YYYY
        df['Отчетный период'] = "01." + df['Отчетный период'].str[-2:] + '.' + df['Отчетный период'].str[:4]
        return df

    # Приведение сырых данных HK API к стандартной схеме + конвертация HKD->USD
    def decor(self):
        """
        1) Читает временный CSV hk_trade.csv
        2) Собирает год/месяц/даты
        3) Приводит HS6 и стандартные поля
        4) Исправляет кейс, где API “перепутал” колонки (маска с HK$ '000)
        5) Конвертирует стоимость HKD->USD по BIS курсу
        6) Возвращает итоговый DataFrame
        """

        data = pd.read_csv(HongKong.hk_params["CSV_PATH_HK"])

        # period в формате YYYYMM
        data["Month"] = data['period'].astype(str).str[4:6]
        data["Year"] = data['period'].astype(str).str[:4]
        data['Отчетный период'] = "01." + data["Month"] + '.' + data["Year"]

        data["Страна-партнер"] = "Россия"
        data["Исходная страна"] = "Гонконг"

        data["Код товара (10 знаков)"] = np.nan
        data["Код товара (8 знаков)"] = np.nan
        data["Код товара (6 знаков)"] = data["code"].astype(str).str.zfill(6)
        data["Код товара (2 знака)"] = data["Код товара (6 знаков)"].str[:2]
        data["Код товара (4 знака)"] = data["Код товара (6 знаков)"].str[:4]

        # после merge raw_value/raw_quantity появляются *_x / *_y
        data["Значение (стоимость)"] = data["figure_x"]
        data["Значение (масса)"] = data["figure_y"]

        data["Единицы стоимости"] = data["unitEN_x"]
        data["Единица объема"] = data["unitEN_y"]

        data["ДЭИ, описание"] = np.nan
        data["Дополнительная единица измерения (ДЭИ)"] = np.nan

        data["Направление"] = data["ttypeDescEN"].replace({"Imports": "Импорт", "Total Exports": "Экспорт"})

        # чистка нулей/NaN как строк
        data = data[
            (data["Значение (стоимость)"] != 0) &
            (data["Значение (стоимость)"] != '0') &
            (data["Значение (стоимость)"] != '0.0') &
            (data["Значение (стоимость)"] != 'nan')
        ]
        data = data[~data["Значение (стоимость)"].isna()]

        # иногда API кладет "HK$ '000" в “стоимость”, а числа в “unitEN”
        # Тогда мы свапаем местами
        mask = data["Значение (стоимость)"] == "HK$ '000"

        # свап (стоимость <-> единицы стоимости)
        data.loc[mask, ["Значение (стоимость)", "Единицы стоимости"]] = \
            data.loc[mask, ["Единицы стоимости", "Значение (стоимость)"]].values

        # свап (масса <-> единица объема) — по аналогии
        data.loc[mask, ["Значение (масса)", "Единица объема"]] = \
            data.loc[mask, ["Единица объема", "Значение (масса)"]].values

        # сортировка
        data = data.sort_values(
            by=['Year', 'Month', 'Направление'],
            ascending=[False, False, True],
            key=lambda col: col.astype(int) if col.name in ['Year', 'Month'] else col,
            ignore_index=True
        )

        # финальные колонки до конвертации
        data = data[[
            "Отчетный период", "Исходная страна", "Страна-партнер", "Направление",
            "Код товара (2 знака)", "Код товара (4 знака)", "Код товара (6 знаков)",
            "Код товара (8 знаков)", "Код товара (10 знаков)", "Значение (стоимость)",
            "Единицы стоимости", "Значение (масса)", "Единица объема",
            "Дополнительная единица измерения (ДЭИ)", "ДЭИ, описание"
        ]]

        # Конвертация стоимости:
        # - figure_x в HK$ '000, поэтому * 1000
        # - дальше HKD -> USD через BIS
        df = self.currency()

        data["Значение (стоимость)"] = data["Значение (стоимость)"].astype(float) * 1000
        data["Значение (масса)"] = data["Значение (масса)"].astype(float)

        # Здесь мы принудительно ставим HKD:
        data["Единицы стоимости"] = "HKD"

        # мерджим по Отчетный период и валюте
        data = pd.merge(data, df, how='left', on=["Отчетный период", "Единицы стоимости"])

        # делим на курс -> USD
        data.loc[data["Единицы стоимости"] != "USD", "Значение (стоимость)"] = data["Значение (стоимость)"] / data["Курс"]

        data = data.drop(columns="Курс")
        data["Единицы стоимости"] = "USD"

        data["Значение (стоимость) - ДЭИ"] = np.nan
        data['Отчетный период'] = pd.to_datetime(data['Отчетный период'], format='%d.%m.%Y')
        return data

    # Основной парсинг HK Trade API
    def parse(self) -> pd.DataFrame:
        """
        Алгоритм:
        1) Удаляет временный CSV hk_trade.csv
        2) Читает HS6 из ./data/HSCodeandDescription.csv (обязательный файл)
        3) Делит HS6 на батчи по 20 кодов
        4) Вычисляет start_period = years[0] + "01", final_period = years[-1] + final_month
        5) Для imports и exports дергает API и сливает VCM (value) + QCM (quantity)
        6) Пишет в hk_trade.csv
        7) decor() приводит к стандарту и конвертирует HKD->USD
        8) Удаляет hk_trade.csv и возвращает итоговый DataFrame
        """

        # чистим временный CSV
        if os.path.exists(HongKong.hk_params["CSV_PATH_HK"]):
            os.remove(HongKong.hk_params["CSV_PATH_HK"])

        first_iter = True

        # Внешний файл со справочником HS6
        hs6 = pd.Series(pd.read_csv("./data/HSCodeandDescription.csv")['Code']).astype(str).str.zfill(6).tolist()

        # батчи по 20
        hs6 = [hs6[i: i + 20] for i in range(0, len(hs6), 20)]

        # периоды YYYYMM
        final_period = self.years[-1] + self.final_month
        start_period = self.years[0] + "01"

        hk_params = HongKong.hk_params.copy()

        # imports / exports
        for ttype in ["imports", "exports"]:
            pbar = tqdm(hs6, unit="batch")

            for hs6_batch in pbar:
                pbar.set_description(f"Собрано HS6 (батчей по 20), {ttype}")
                hk_params["url"] = hk_params["url"].replace(",202505&", f",{final_period}&")
                hk_params["url"] = hk_params["url"].replace("period=201901", f"period={start_period}")
                if ttype == "exports":
                    hk_params["url"] = hk_params["url"].replace("&ttype=1&coclass=C&co=RU&", "&ttype=4&ccclass=C&cc=RU&")

                # Собираем ссылку + HS6-коды через запятую
                api_link = hk_params["url"] + ','.join(hs6_batch)

                while True:
                    try:
                        response = requests.get(url=api_link)
                        api = response.json()

                        # Если API ответил Fail — прекращаем этот батч
                        if api["header"]["status"]["name"] == "Fail":
                            break

                        records = api["dataSet"]
                        raw = pd.DataFrame(records)

                        # иногда присутствует footnote
                        if "cellfootnoteEN" in raw.columns:
                            raw = raw.drop(columns="cellfootnoteEN")

                        raw_value = raw[raw["sv"] == "VCm"]
                        raw_quantity = raw[raw["sv"] == "QCm"]

                        # MERGE по ключам — для exports и imports ключи отличаются (cc vs co)
                        if ttype == "exports":
                            df = pd.merge(
                                raw_value, raw_quantity,
                                on=['freq', 'period', 'ttype', 'ttypeDescEN', 'cc',
                                    'ccDescEN', 'codeclass', 'code', 'codeDescEN'],
                                how='inner'
                            )
                        else:
                            df = pd.merge(
                                raw_value, raw_quantity,
                                on=['freq', 'period', 'ttype', 'ttypeDescEN', 'co',
                                    'coDescEN', 'codeclass', 'code', 'codeDescEN'],
                                how='inner'
                            )

                        df = df[~df["unitEN_x"].isna()]

                        # append в CSV
                        df.to_csv(
                            hk_params["CSV_PATH_HK"],
                            mode="a",
                            index=False,
                            header=first_iter,
                            encoding="utf-8-sig",
                        )
                        first_iter = False
                        break

                    except:
                        print("Connection error - repeat this batch")

        print("Парсинг успешно завершен. Перехожу к составлению и оформлению итоговой таблицы.")
        data = self.decor()

        # удаляем временный CSV
        if os.path.exists(hk_params["CSV_PATH_HK"]):
            os.remove(hk_params["CSV_PATH_HK"])

        return data