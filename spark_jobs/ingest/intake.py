import os
import json
from time import sleep
from utils.api_utils import discover_movie_ids, fetch_movie_details, RAW_DIR


def main():
    ids = discover_movie_ids(pages=50) 
    print(f"Descubiertos {len(ids)} IDs de películas")
    for mid in ids:
        raw_path = os.path.join(RAW_DIR, f"movie_{mid}.json")
        if os.path.exists(raw_path):
            print(f"Ya existe {raw_path}, salteando")
            continue
        # print(f"Descargando película {mid}")
        data = fetch_movie_details(mid)
        with open(raw_path, "w", encoding="utf8") as f:
            import json; json.dump(data, f, ensure_ascii=False, indent=2)
        sleep(0.3)

if __name__ == "__main__":
    main()
