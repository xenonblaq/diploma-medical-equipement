import pandas as pd
import numpy as np
import requests


class Uzbekistan:
    """
    Парсер внешней торговли Узбекистана с Россией/Беларусью.
    Источник: готовые JSON-файлы (SDMX выгрузки) с сайта siat.stat.uz:
      - export_link: экспорт
      - import_link: импорт

    Логика:
      1) parse():
         - скачиваем JSON по экспорту и импорту
         - фильтруем по стране-партнеру (Россия или Беларусь)
         - приводим широкую таблицу (столбцы = YYYYMM) в длинный формат (melt)
         - добавляем год/месяц, направление, единицы, масштабируем значения
         - объединяем export+import в один DF
      2) decor():
         - собираем "Отчетный период" (01.MM.YYYY)
         - заполняем метаданные под общий формат проекта
         - превращаем накопленные значения по году в месячные (разность внутри года)
         - сортируем и фильтруем по self.years
         - приводим период к datetime
    """

    uz_params = {
        # Готовая выгрузка по экспорту (SDMX JSON)
        "export_link": "https://api.siat.stat.uz/media/uploads/sdmx/sdmx_data_1170.json",
        # Готовая выгрузка по импорту (SDMX JSON)
        "import_link": "https://api.siat.stat.uz/media/uploads/sdmx/sdmx_data_1173.json"
    }

    def __init__(self, years=["2025"], belarus=False):
        """
        years: список лет строками (например ["2023","2024","2025"])
        belarus:
          - False -> страна-партнер Россия
          - True  -> страна-партнер Беларусь
        """
        self.years = years
        self.belarus = belarus

    def decor(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Приведение "сырого" DF (year, month, ttype, value, ...) к единому формату проекта.

        На вход ожидается датафрейм с колонками минимум:
          - year (YYYY)
          - month (MM)
          - ttype ("export"/"import")
          - value (float)
        """
        # Формируем период в формате проекта: 01.MM.YYYY
        data['Отчетный период'] = (
            "01." + data["month"].astype(str).str.zfill(2) + '.' + data["year"].astype(str)
        )

        # Страна-партнер: Россия или Беларусь (в зависимости от флага)
        if self.belarus:
            data["Страна-партнер"] = "Беларусь"
        else:
            data["Страна-партнер"] = "Россия"

        # Страна-источник
        data["Исходная страна"] = "Узбекистан"

        # В этой выгрузке нет разбивки по HS — оставляем NaN
        data["Код товара (10 знаков)"] = np.nan
        data["Код товара (8 знаков)"] = np.nan
        data["Код товара (6 знаков)"] = np.nan
        data["Код товара (4 знака)"] = np.nan
        data["Код товара (2 знака)"] = np.nan

        # Стоимость (в USD), масса/объем/ДЭИ отсутствуют
        data["Значение (стоимость)"] = data["value"].astype(float)
        data["Значение (масса)"] = np.nan
        data["Единица объема"] = np.nan
        data["Единицы стоимости"] = "USD"
        data["ДЭИ, описание"] = np.nan
        data["Дополнительная единица измерения (ДЭИ)"] = np.nan

        # Направление в терминах проекта
        data["Направление"] = data["ttype"].replace({"export": "Экспорт", "import": "Импорт"})

        # Убираем нулевые/пустые значения стоимости
        data = data[(data["Значение (стоимость)"] != 0) & (data["Значение (стоимость)"] != '0')]
        data = data[~data["Значение (стоимость)"].isna()]

        # --- ВАЖНОЕ ПРЕОБРАЗОВАНИЕ ---
        # value - накопленный итог.
        year = data["Отчетный период"].astype(str).str[-4:]
        same_year_as_prev = year.eq(year.shift())
        data["Значение (стоимость)"] = data["Значение (стоимость)"].where(
            ~same_year_as_prev,
            data["Значение (стоимость)"] - data["Значение (стоимость)"].shift()
        )

        # Сортировка: новые периоды сверху, затем Экспорт/Импорт
        data = data.sort_values(
            by=['year', 'month', "Направление"],
            ascending=[False, False, True],
            key=lambda col: col.astype(int) if col.name in ['year', 'month'] else col,
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

        # В этом источнике нет показателя "стоимость - ДЭИ"
        data["Значение (стоимость) - ДЭИ"] = np.nan
        # Фильтрация только по нужным годам
        data = data[data['Отчетный период'].str[-4:].isin(self.years)]
        # Перевод периода в datetime
        data['Отчетный период'] = pd.to_datetime(data['Отчетный период'], format='%d.%m.%Y')
        return data

    def parse(self) -> pd.DataFrame:
        """
        Скачивает экспорт и импорт, фильтрует по стране-партнеру, приводит к единому формату.
        """
        dfs = []
        # Два направления: экспорт и импорт
        for ttype in ["export", "import"]:
            # Скачиваем JSON
            response = requests.get(url=Uzbekistan.uz_params[f"{ttype}_link"]).json()

            # Нормализуем вложенную структуру: берем response[0]['data'] и превращаем в табличку
            df = pd.json_normalize(response[0]['data'])

            # Фильтруем строку по стране-партнеру (колонка Klassifikator_ru)
            # .iloc[:, 5:] — отбрасываем первые метаданные колонки, оставляем только YYYYMM-колонки
            if self.belarus:
                df = df[df['Klassifikator_ru'] == 'Беларусь'].iloc[:, 5:]
            else:
                df = df[df['Klassifikator_ru'] == 'Россия'].iloc[:, 5:]

            # Превращаем широкую структуру (YYYYMM столбцы) в длинную:
            # variable = YYYYMM, value = значение
            df = df.melt()

            # Добавляем служебные поля
            df["ttype"] = ttype
            df["country"] = "Узбекистан"
            df["month"] = df["variable"].str[-2:]  # последние 2 символа = месяц
            df["year"] = df["variable"].str[:4]    # первые 4 символа = год

            # Страна-партнер в явном виде (хотя в decor это еще раз переопределится)
            if self.belarus:
                df["country_1"] = 'Беларусь'
            else:
                df["country_1"] = 'Россия'

            # Масштабирование: *1000
            df["value"] = df["value"].astype(float) * 1000
            df["unit"] = "USD"

            # Складываем в список только нужные колонки
            dfs.append(df[["year", "month", "country", "country_1", "ttype", "value", "unit"]])

        # Объединяем экспорт+импорт
        df = pd.concat(dfs, ignore_index=True)
        # Приводим к стандартному формату проекта
        return self.decor(df)
