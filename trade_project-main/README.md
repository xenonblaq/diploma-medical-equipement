# Russian Foreign Trade Parsers

## Общее описание

Этот репозиторий содержит набор специализированных парсеров для сбора и приведения к единому формату
статистики внешней торговли России с различными странами и регионами мира.

Каждый парсер:
- работает с **одной страной / регионом**;
- самостоятельно получает данные (API, сайты статистики, PDF, Excel, Selenium);
- приводит данные к **единому табличному формату**;
- возвращает `pandas.DataFrame`, готовый к агрегации или экспорту.

Поверх всех парсеров реализована **обертка** `RussianForeignTradeParser_1`,
которая позволяет запускать парсеры по одной стране или все сразу
и формировать итоговый Excel-файл.

Очень сложно описать каждый парсер по отдельности. Они все уникальны. Поэтому совместно с ГПТ я подробно закомментировал каждый .py файлик. 

В `example.ipynb` находится пример запуска каждого файла, записи в базу, чтения базы. Рекомендую из него все парсеры и запускать.

---

## Структура проекта

```
scrapers/
│
├─ data/
│   ├─ HSCodeandDescription.csv
│   ├─ HSCodeandDescription.xlsx
│   ├─ *.csv                  # временные файлы парсеров
│   └─ excel_files/           # итоговые Excel-файлы
│
├─ Brazil.py
├─ Korea.py
├─ Turkey.py
├─ USA.py
├─ Mexico.py
├─ EU.py
├─ Vietnam.py
├─ Uzbekistan.py
├─ Kyrgyzstan.py
├─ Tadjikistan.py
├─ Taiwan.py
├─ Thailand.py
├─ China.py
├─ CIS.py
├─ ...
│
├─ RussianForeignTradeParser_1.py
└─ README.md
```

В отдельных папках `missing_forecasting` и `expimp_forecasting` находятся модели прогнозирования остатка на уровне страна+HS4 и общих сумм экспорт/импорта соответственно.

---

## Общий формат выходных данных

Все парсеры приводят данные к единому набору колонок:

- Отчетный период (`datetime`)
- Исходная страна
- Страна-партнер
- Направление (Импорт / Экспорт)
- Код товара (2, 4, 6, 8, 10 знаков)
- Значение (стоимость)
- Единицы стоимости
- Значение (масса)
- Единица объема
- Дополнительная единица измерения (ДЭИ)
- Описание ДЭИ

Это позволяет **без дополнительной обработки объединять данные** разных стран.

---

## Обертка `RussianForeignTradeParser_1`

Файл: `RussianForeignTradeParser_1.py`

Назначение:
- единая точка входа;
- запуск одного или всех парсеров;
- объединение данных;
- сохранение форматированного Excel-файла.

Пример использования:

```python
from RussianForeignTradeParser_1 import RussianForeignTradeParser_1

parser = RussianForeignTradeParser_1(
    country="Turkey",
    params={"years": ["2023", "2024"]}
)

df = parser.parse()
parser.create_excel(df, "turkey_trade.xlsx")
```

Запуск всех стран:

```python
parser = RussianForeignTradeParser_1(all=True, params={"years": ["2024"]})
df = parser.parse_all()
```

Не рекомендую. Скорее всего где-то что-то сломается и весь прогресс будет потерян. Это так, на будущее функция.

---

## Зависимости (requirements)

Основные библиотеки:

- `pandas`
- `numpy`
- `requests`
- `beautifulsoup4`
- `lxml`
- `tqdm`
- `openpyxl`
- `selenium`
- `undetected-chromedriver`
- `fake-useragent`
- `PyPDF2`
- `transformers` (используется в парсере Taiwan)
- `torch` (неявная зависимость для OCR)

Для Selenium:
- установленный **Google Chrome**
- совместимая версия ChromeDriver (используется `undetected_chromedriver`)

---

## Внешние файлы и ресурсы

### Обязательные локальные файлы

| Файл | Где используется | Назначение |
|----|----|----|
| `data/HSCodeandDescription.csv` | Turkey, Taiwan, Korea | список HS-кодов |
| `data/HSCodeandDescription.xlsx` | Korea | список HS-кодов |
| `data/Commodity.csv` | China | список HS-кодов |
| `data/` | все | временные CSV-файлы |

### Временные папки

Некоторые парсеры создают:
- `./files/` — временные PDF / Excel / ZIP
- файлы автоматически удаляются после завершения парсинга

---

## Особенности парсеров (кратко)

- **USA, EU, Uzbekistan** — API (быстро, стабильно)
- **Turkey, Korea, Taiwan, Mexico** — Selenium (медленно, чувствительно к сайтам)
- **Vietnam** — PDF + Selenium
- **Kyrgyzstan, Tadjikistan** — Excel/ZIP с ручной структурой
- **CIS** — агрегатор нескольких стран
- **Belarus / Kazakhstan / Armenia / Azerbaijan** — национальные статистические источники

---

## Рекомендации

- Запускать Selenium-парсеры **по одной стране**.
- НЕ обновлять Chrome.
- Регулярно обновлять и исправлять ошибки парсеров при изменении структуры сайтов и тд 
- Запускать парсеры по одному. Если какой-то парсер выдаст ошибку, будет жалко потраченного времени, когда вы думали, что они все отработали.
---


