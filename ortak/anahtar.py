import os as __os
import json as __json

HEADERS = {
    'x-ibm-client-id': "",
    'accept': "application/json"
}

kimlik_dosyasi = __os.path.join(__os.path.expanduser("~"), ".seffaflik\\.kimlik")
if __os.path.exists(kimlik_dosyasi):
    with open(kimlik_dosyasi, "r") as f:
        kimlik = f.read()
        api_anahtari = __json.loads(kimlik)["istemci_taniticisi"]
        HEADERS["x-ibm-client-id"] = api_anahtari

## 4a08ada0-3f0b-43e9-8928-c7f8d5130a7a
