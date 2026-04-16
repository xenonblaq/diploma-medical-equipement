from EU import EU                    # Локальный парсер “EU” (Молдова и Украина)
from Kazakhstan import Kazakhstan     # Локальный парсер Казахстана
from Uzbekistan import Uzbekistan     # Локальный парсер Узбекистана
from Armenia import Armenia           # Локальный парсер Армении
from Tadjikistan import Tadjikistan   # Локальный парсер Таджикистана
from Kyrgyzstan import Kyrgyzstan     # Локальный парсер Киргизии
from Azerbaijan import Azerbaijan     # Локальный парсер Азербайджана
import pandas as pd                  


class CIS:
    """
    Агрегатор парсеров для блока "СНГ/окрестности" (по факту: EU(Молдова+Украина) + ряд стран).

    Основная идея:
    - запустить несколько отдельных парсеров с одинаковыми параметрами (years, belarus)
    - объединить результаты в один DataFrame

    Важно:
    - Каждый импортируемый модуль (EU.py, Kazakhstan.py, Uzbekistan.py, Armenia.py, Tadjikistan.py,
      Kyrgyzstan.py, Azerbaijan.py) должен лежать рядом и быть импортируемым.
    - У каждого из них должен быть класс с методом parse(), который возвращает pd.DataFrame
      со стандартными колонками (судя по остальным парсерам: Отчетный период / страны / направление / HS / значения).
    """

    def __init__(self, years=["2025"], belarus=False):
        # years: список лет строками (например ["2019","2020",...,"2025"])
        self.years = years

        # belarus: флаг, который прокидывается в дочерние парсеры
        # (например, у Armenia/Azerbaijan этот флаг меняет страну-партнера или срез данных)
        self.belarus = belarus

    def parse(self):
        """
        Последовательно запускает парсеры:
        1) EU (“Молдова и Украина”)
        2) Казахстан
        3) Узбекистан
        4) Армения
        5) Таджикистан
        6) Киргизия
        7) Азербайджан

        Затем объединяет результаты в один DataFrame через pd.concat.
        """

        counter = 0
        # Общие параметры для всех дочерних парсеров
        params = {"years": self.years, "belarus": self.belarus}

        # 1) EU (Молдова и Украина)
        print(f"{counter + 1}. Парсим Молдову и Украину")
        ukr_md_data = EU(**params).parse()
        counter += 1

        # 2) Казахстан
        print(f"{counter + 1}. Парсим Казахстан")
        kaz_data = Kazakhstan(**params).parse()
        counter += 1

        # 3) Узбекистан
        print(f"{counter + 1}. Парсим Узбекистан")
        uz_data = Uzbekistan(**params).parse()
        counter += 1

        # 4) Армения
        print(f"{counter + 1}. Парсим Армению")
        armenia_data = Armenia(**params).parse()
        counter += 1

        # 5) Таджикистан
        print(f"{counter + 1}. Парсим Таджикистан")
        taj_data = Tadjikistan(**params).parse()
        counter += 1

        # 6) Киргизия
        print(f"{counter + 1}. Парсим Киргизию")
        kg_data = Kyrgyzstan(**params).parse()
        counter += 1

        # 7) Азербайджан
        print(f"{counter + 1}. Парсим Азербайджан")
        az_data = Azerbaijan(**params).parse()

        # Склейка результатов
        CIS_data = pd.concat([ukr_md_data, kaz_data, uz_data, armenia_data, taj_data, kg_data])
        CIS_data = pd.concat([CIS_data, az_data])
        return CIS_data