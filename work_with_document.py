import json
import os
from keybert import KeyBERT
import docx2txt
import PyPDF2

input_file = 'final_parsed_data_general_cleaned.json'
output_file = 'final_parsed_data_general_cleaned_fulltext.json'

# Инициализируем модель KeyBERT
kw_model = KeyBERT('distiluse-base-multilingual-cased-v1')

def extract_text_from_pdf(file_path):
    text = ""
    try:
        with open(file_path, 'rb') as f:
            reader = PyPDF2.PdfReader(f)
            for page in reader.pages:
                page_text = page.extract_text()
                if page_text:
                    text += page_text + "\n"
    except Exception as e:
        print(f"Ошибка чтения PDF {file_path}: {e}")
    return text.strip()

def extract_text_from_docx(file_path):
    try:
        text = docx2txt.process(file_path)
        return text.strip() if text else ""
    except Exception as e:
        print(f"Ошибка чтения DOCX {file_path}: {e}")
        return ""

def extract_text_from_file(file_path):
    # Определяем тип файла по расширению
    ext = os.path.splitext(file_path)[1].lower()
    if ext == ".pdf":
        return extract_text_from_pdf(file_path)
    elif ext == ".docx":
        return extract_text_from_docx(file_path)
    else:
        # Если формат неизвестен или не поддерживается - возвращаем пустую строку
        return ""

with open(input_file, 'r', encoding='utf-8') as f:
    data = json.load(f)

# Подсчёт общего числа документов
documents_indices = [i for i, e in enumerate(data) if e.get("class") == "Document"]
total_documents = len(documents_indices)
processed_count = 0

for idx, doc_index in enumerate(documents_indices, start=1):
    entry = data[doc_index]
    props = entry.get("properties", {})
    file_path = props.get("file_path")

    if file_path and os.path.exists(file_path):
        # Извлекаем текст из файла
        full_text = extract_text_from_file(file_path)
        if full_text:
            # Применяем KeyBERT для извлечения ключевых слов из полного текста
            keywords = kw_model.extract_keywords(full_text, keyphrase_ngram_range=(1,2), top_n=10)
            props["keywords"] = [kw[0] for kw in keywords if kw]
            props["content"] = full_text  # Заменяем контент на полный текст из файла
        else:
            # Если не удалось извлечь текст, оставляем как есть
            props["keywords"] = props.get("keywords", [])

    # Обновляем счётчик обработанных документов и выводим прогресс
    processed_count += 1
    progress = (processed_count / total_documents) * 100
    print(f"Обработка документов: {processed_count}/{total_documents} ({progress:.2f}%)")

with open(output_file, 'w', encoding='utf-8') as f:
    json.dump(data, f, ensure_ascii=False, indent=4)

print(f"Обработка завершена. Из {total_documents} документов было обработано {processed_count}. Результат сохранён в {output_file}")
