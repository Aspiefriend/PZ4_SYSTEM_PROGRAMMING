import asyncio
import aiohttp
import json
import random
from datetime import datetime

COINGECKO_URL = "https://api.coingecko.com/api/v3/simple/price"
OPENLIBRARY_URL = "https://openlibrary.org/search.json"


async def fetch_data(url, params, session, semaphore):
    async with semaphore:
        try:
            async with session.get(url, params=params, timeout=5) as response:
                if response.status == 200:
                    return await response.json()
                else:
                    error_text = await response.text()
                    
                    try:
                        error_dict = json.loads(error_text)
                        error = error_dict.get("error")
                        message = error_dict.get("message")
                        
                        write_errors_to_json({
                            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            "message_from_me": None,
                            "error": error,
                            "message": message,
                            "url": url,
                            "status": response.status
                        })
                    except json.JSONDecodeError:
                        write_errors_to_json({
                            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            "message_from_me": "Ответ от сервера пришёл не в JSON формате",
                            "error": None,
                            "message": None,
                            "url": url,
                            "status": response.status
                        })
                        
        except asyncio.TimeoutError:
             write_errors_to_json({
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "message_from_me": "Время ожидания истекло",
                "error": None,
                "message": None,
                "url": url,
                "status": "timeout"
             })
            
        except Exception as e:
            write_errors_to_json({
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "message_from_me": "Непредвиденная ошибка",
                "error": str(e),                
                "message": None,
                "url": url,
                "status": "unknown error"
            })

    return None


def write_errors_to_json(error):
    errors_list = []
    try:
        with open("errors.json", "r", encoding="utf-8") as f:
            data = json.load(f)
            errors_list = data.get("errors", [])
    except FileNotFoundError:
        errors_list = []
    except json.JSONDecodeError:
        print("Файл с ошибками битый , перезапишу")
        errors_list = []
    except Exception as e:
            print(f"Ошибка при чтении json файла с ошибками. Прекращаю выполнение функции {e}")
            return
        
            

    errors_list.append(error)
    
    new_data = {"errors": errors_list}
    try:
        with open("errors.json", "w", encoding="utf-8") as f:
            json.dump(new_data, f, ensure_ascii=False, indent=4)
    except Exception as e:
        print(f"Ошибка записи в файл ошибок {e}")

def write_data_to_json(item):
    data_list = []
    try:
        with open("data.json", "r", encoding="utf-8") as f:
            data = json.load(f)
            data_list = data.get("data", [])

    except FileNotFoundError:
        data_list = []
    except json.JSONDecodeError:
        print("Файл с данными битый , перезапишу")
        data_list = []
    except Exception as e:
            print(f"Ошибка при чтении файла с данными {e}")
            return
    data_list.append(item)
    new_data = {"data": data_list}
    try:
        with open("data.json", "w", encoding="utf-8") as f:
            json.dump(new_data, f, ensure_ascii=False, indent=4)
    except Exception as e:
        print(f"Ошибка записи в файл данных {e}")
        
        



async def fetch_crypto(coins, session, semaphore):
    params = {
        "ids": ",".join(coins), 
        "vs_currencies": "usd",    
        "include_24hr_change": "true" 
    }
    
    data = await fetch_data(COINGECKO_URL, params, session, semaphore)
    
    if not data:
        return []

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    results = []
    
    for coin_id, info in data.items():
        results.append({
            "timestamp": timestamp,
            "coin_id": coin_id, 
            "price_usd": info.get("usd"),
            "change_24h": info.get("usd_24h_change")
        })
        
    return results


async def fetch_books(query, session, semaphore):
    offset = random.randint(0,100)
    params = {
        "q": query,
        "limit": 3,
        "offset": offset
    }
    
    data = await fetch_data(OPENLIBRARY_URL, params, session, semaphore)
    
    if not data or "docs" not in data:
        return []

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    results = []
    
    for book in data["docs"][:3]:
        results.append({
            "timestamp": timestamp,
            "book_key": book.get("key"),
            "title": book.get("title", "Без названия"),
            "author": ", ".join(book.get("author_name", [])) or "Не указан",
            "first_publish_year": book.get("first_publish_year"),
        })
        
    return results


async  def collect_and_save():
    all_queries = [
    "Golang", "Лев Толстой", "Александр Пушкин",
    "Batman", "Wolverine", "X-men", "New Avengers", "Mighty Thor",
    "Brian Michael Benids", "Scott Snyder", "Лев Троцкий"
]
    queries = random.sample(all_queries, 3) 
    
    coins = ["bitcoin", "ethereum", "solana"] 
    
    async with aiohttp.ClientSession() as session:
        semaphore = asyncio.Semaphore(3)
        
        tasks = [
            fetch_crypto(coins, session, semaphore),
            fetch_books(queries[0], session, semaphore),
            fetch_books(queries[1], session, semaphore),
            fetch_books(queries[2], session,semaphore)
        ]
        
        results = await asyncio.gather(*tasks, return_exceptions=True)

    if isinstance(results[0], Exception):
        print(f"Крипта упала с ошибкой: {results[0]}")
        crypto_data = []
    else:
        crypto_data = results[0]
    
    books_data = []
    for res in results[1:]: 
        if isinstance(res, Exception):
            print(f"Книги упали с ошибкой: {res}")
            continue 
        
        books_data.extend(res) 

    print(f"Собрано: {len(crypto_data)} монет и {len(books_data)} книг.")
    

    all_items = []


    for c in crypto_data:
        all_items.append({"type": "crypto", **c})
    

    for b in books_data:
        all_items.append({"type": "book", **b})


    for item in all_items:
        write_data_to_json(item)




async def main():
    print("Запуск системы сбора данных...")
    
    
    while True:
        print(f"\n--- Начинаю цикл сбора: {datetime.now().strftime('%H:%M:%S')} ---")
        
        try:
            await collect_and_save()
        except Exception as e:
            print(f"Критическая ошибка в цикле: {e}")
            
        print("Ждем 30 секунд до следующего сбора (Ctrl+C для выхода)...")
        await asyncio.sleep(30)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nПрограмма остановлена пользователем.")

