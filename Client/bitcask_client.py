import argparse
import requests
import csv
import time
import threading

CENTRAL_STATION_URL = "http://localhost:8080/weatherMonitoring/BaseCentralStation" 


# view all keys
def view_all():
    timestamp = int(time.time())
    response = requests.get(f"{CENTRAL_STATION_URL}/view-all")

    # writing keys in file
    if response.status_code == 200:
        data = response.json()
        filename = f"{timestamp}.csv"
        with open(filename, "w", newline='') as f:
            writer = csv.writer(f)
            writer.writerow(["key", "value"])
            for item in data:
                writer.writerow([item['key'], item['value']])
        print(f"✅ Written to {filename}")

    # error status
    else:
        print(f"💥Error Status {response.status_code}: {response}")




# starting number of threads then viewing all keys
def clients_test(clients):
    timestamp = int(time.time())

    # function performed by each thread
    def worker(thread_num):
        try:
            response = requests.get(f"{CENTRAL_STATION_URL}/view-all")

            # writing keys in file
            if response.status_code == 200:
                data = response.json()
                filename = f"{timestamp}_thread_{thread_num}.csv"
                with open(filename, "w", newline='') as f:
                    writer = csv.writer(f)
                    writer.writerow(["key", "value"])
                    for item in data:
                        writer.writerow([item['key'], item['value']])
                print(f"✅Thread {thread_num} wrote to {filename}")

            # error status
            else:
                print(f"💥Error for Thread {thread_num} , error status: {response.status_code}")
            
        # error with thread
        except Exception as e:
            print(f"💥Thread {thread_num} error: {e}")

    threads = []
    for i in range(1, clients + 1):
        t = threading.Thread(target=worker, args=(i,))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()




# view key value
def view_key(key):
    response = requests.get(f"{CENTRAL_STATION_URL}/view-key?key={key}")
    # Handle the response
    if response.status_code == 200:
        print("✅ Value:", response.json().get("value", ""))
    else:
        print(f"💥Error Status {response.status_code}: {response}")



if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--view-all", action="store_true")
    parser.add_argument("--view", action="store_true")
    parser.add_argument("--key",  type=int)
    parser.add_argument("--perf", action="store_true")
    parser.add_argument("--clients", type=int)

    args = parser.parse_args()

    if args.view_all:
        view_all()
    elif args.view and args.key:
        view_key(args.key)
    elif args.perf and args.clients:
        clients_test(args.clients)
    else:
        print("Invalid usage. Try:")
        print("  python bitcask_client.py --view-all")
        print("  python bitcask_client.py --view --key=SOME_KEY")
        print("  python bitcask_client.py --perf --clients=100")