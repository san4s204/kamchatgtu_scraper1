import json
import os
from keybert import KeyBERT
import docx2txt
import PyPDF2

input_file = 'final_parsed_data_general_cleaned.json'
output_file = 'final_parsed_data_general_cleaned_fulltext.json'
progress_file = 'progress.json'

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
    ext = os.path.splitext(file_path)[1].lower()
    if ext == ".pdf":
        return extract_text_from_pdf(file_path)
    elif ext == ".docx":
        return extract_text_from_docx(file_path)
    else:
        return ""

# Загружаем данные
with open(input_file, 'r', encoding='utf-8') as f:
    data = json.load(f)

# Определяем все документы
documents_indices = [i for i, e in enumerate(data) if e.get("class") == "Document"]
total_documents = len(documents_indices)

# Загружаем прогресс, если есть
if os.path.exists(progress_file):
    with open(progress_file, 'r', encoding='utf-8') as pf:
        progress_data = json.load(pf)
    start_index = progress_data.get("last_processed_index", -1) + 1
else:
    start_index = 0

# Если start_index больше total_documents, значит мы все обработали
if start_index >= total_documents:
    print("Все документы уже обработаны.")
    exit(0)

print(f"Начинаем обработку документов с индекса {start_index} из {total_documents}")

for idx in range(start_index, total_documents):
    doc_index = documents_indices[idx]
    entry = data[doc_index]
    props = entry.get("properties", {})
    file_path = props.get("file_path")

    if file_path and os.path.exists(file_path):
        full_text = extract_text_from_file(file_path)
        if full_text:
            keywords = kw_model.extract_keywords(full_text, keyphrase_ngram_range=(1,2), top_n=10)
            props["keywords"] = [kw[0] for kw in keywords if kw]
            props["content"] = full_text
        else:
            # Если не удалось извлечь текст, оставляем как есть
            props["keywords"] = props.get("keywords", [])

    # Сохраняем данные после каждого обработанного документа
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
    
    # Обновляем прогресс
    with open(progress_file, 'w', encoding='utf-8') as pf:
        json.dump({"last_processed_index": idx}, pf, ensure_ascii=False, indent=4)

    progress = ((idx + 1) / total_documents) * 100
    print(f"Обработка документов: {idx + 1}/{total_documents} ({progress:.2f}%)")

print("Обработка завершена.")
