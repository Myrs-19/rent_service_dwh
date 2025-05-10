from sqlalchemy import create_engine
import pandas as pd

# Создаем подключение
engine = create_engine('postgresql://mseleznev:garo6767@localhost:5432/test')

# Пример DataFrame
df = pd.DataFrame({
    'column1': [1, 2, 3],
    'column2': ['A', 'B', 'C']
})

df.columns = ['c1', 'c2']

# Запись в PostgreSQL
df.to_sql(
    schema='for_test',
    name='test',      # Название таблицы
    con=engine,                # Подключение
    if_exists='append',        # 'append', 'replace' или 'fail'
    index=False,               # Не записывать индекс
    index_label=False
)

print('done')