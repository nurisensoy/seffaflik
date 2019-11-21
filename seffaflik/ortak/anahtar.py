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
        istemci_taniticisi = __json.loads(kimlik)["istemci_taniticisi"]
        HEADERS["x-ibm-client-id"] = istemci_taniticisi
