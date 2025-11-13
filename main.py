import requests
import os
from dotenv import load_dotenv
from datetime import datetime
from zoneinfo import ZoneInfo
from time import sleep
from datetime import datetime, timezone
load_dotenv(dotenv_path=".env", override=True)

api_url = os.getenv("API_URL")
bus_target = os.getenv("BUS_REGISTER")
device_target = os.getenv("DEVICE")
def upload_counters(payload):
    url = f"{api_url}/api/vehicle/counter"
    try:
        r = requests.post(url, json=payload, timeout=10)
        print("[upload] status:", r.status_code)
        return r.status_code == 201
    except requests.RequestException as e:
        print("HTTP error (upload):", e)
        return False

def upload_transactions(payload):
    url = f"{api_url}/api/transactions"
    try:
        r = requests.post(url, json=payload, timeout=10)
        print("[upload] status:", r.status_code)
        return r.status_code == 201
    except requests.RequestException as e:
        print("HTTP error (upload):", e)
        return False

def get_transactions():
    url = "http://localhost:8000/api/transactions/pending"
    try:
        r = requests.get(
            url,
            headers={"Accept": "application/json"},
            timeout=10
        )
        data = r.json()
        return data["result"] if data.get("status") == 200 else []
    except requests.RequestException as e:
        print("HTTP error (get):", e)
        return []
    except ValueError:
        print("Respuesta no JSON (get).")
        return []


def get_passengers():
    today = datetime.now(ZoneInfo("America/Guayaquil")).strftime("%Y-%m-%d")
    start_time = f"{today}T05:40:00"
    end_time   = f"{today}T22:40:00"

    url = "http://localhost:8000/api/passengers/by_date/uploaded"
    try:
        r = requests.get(
            url,
            params={
                "start_datetime": start_time,
                "end_datetime": end_time,
                "uploaded": "false"
            },
            headers={"Accept": "application/json"},
            timeout=10
        )
        data = r.json()
        return data["result"] if data.get("status") == 200 else []
    except requests.RequestException as e:
        print("HTTP error (get):", e)
        return []
    except ValueError:
        print("Respuesta no JSON (get).")
        return []

def update_passenger(user_id):
    url = f"http://localhost:8000/api/passengers/{user_id}"
    try:
        r = requests.patch(url, timeout=10)
        data = r.json()
        ok = data.get("status") == 200
        print(f"[update] id={user_id} -> {ok}")
        return ok
    except requests.RequestException as e:
        print("HTTP error (update):", e)
        return False
    except ValueError:
        print("Respuesta no JSON (update).")
        return False
    

def update_transaction(user_id):
    url = f"http://localhost:8000/api/transaction/{user_id}"
    try:
        r = requests.patch(url, timeout=10)
        data = r.json()
        ok = data.get("status") == 200
        print(f"[update] id={user_id} -> {ok}")
        return ok
    except requests.RequestException as e:
        print("HTTP error (update):", e)
        return False
    except ValueError:
        print("Respuesta no JSON (update).")
        return False

def process_once():
    print(f"\n[{datetime.now().isoformat()}] Iniciando batch…")
    counters = get_passengers()
    transactions = get_transactions()
    print(f"Pasajeros pendientes: {len(counters)}")
    print(f"transacciones pendientes: {len(transactions)}")
    passengeers = len(counters)
    payload = {"passengers": passengeers,"register_vehicle": int(bus_target)}
    if upload_counters(payload):
        for p in counters:
            try:
                update_passenger(p["id"])
            except Exception as e:
                print("Error procesando pasajero:", p.get("id"), e)
    for t in transactions:
        try:
            dt = datetime.fromisoformat(t["timestamp"])
            formatted = dt.replace(tzinfo=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")
            payload = {
                "card_code":t["card_code"],
                "card_type":str(t["card_type"]),
                "card_date":t["date"],
                "card_time":t["time"],
                "date":formatted,
                "amount":t["amount"],
                "balance":t["balance"],
                "latitude":float(t["latitude"]),
                "longitude":float(t["longitude"]),
                "device":device_target
            }
            print(payload)
            if upload_transactions(payload):
                update_transaction(t['id'])
        except Exception as e:
            print("Error procesando transaction:", t.get("id"), e)


if __name__ == "__main__":
    INTERVAL = 15 * 60
    while True:
        t0 = datetime.now()
        try:
            process_once()
        except Exception as e:
            print("Error en batch:", e)
        elapsed = (datetime.now() - t0).total_seconds()
        wait = max(0, INTERVAL - elapsed)
        print(f"Duración: {int(elapsed)}s. Dormir {int(wait)}s…")
        sleep(wait)
