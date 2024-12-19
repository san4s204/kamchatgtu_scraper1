import json

input_file = 'parsed_data.json'
output_file = 'parsed_data_documents.json'

with open(input_file, 'r', encoding='utf-8') as f:
    data = json.load(f)

documents = [entry for entry in data if entry.get("class") == "Document"]

with open(output_file, 'w', encoding='utf-8') as f:
    json.dump(documents, f, ensure_ascii=False, indent=4)

print(f"Найдено {len(documents)} записей класса Document. Результат сохранён в {output_file}")
