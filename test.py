import pandas as pd
import os

# Создаём DataFrame
data = {
    'id': [1, 2, 3],
    'name': ['Alice, Привет, Квратира ""идеальный вариант""в ценрте Москвы, 25 тыс', 'Bob', 'Charlie'],
    'age': [25, 30, 35]
}
df = pd.DataFrame(data)

# Определяем путь для сохранения
output_dir = "./test/output_files"
os.makedirs(output_dir, exist_ok=True)

# Сохранение в CSV
csv_path = os.path.join(output_dir, "data.csv")
df.to_csv(csv_path, index=False)
print(f"DataFrame сохранён в {csv_path}")

df_new = pd.read_csv(csv_path)
print(df_new)