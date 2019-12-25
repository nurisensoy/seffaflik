import os as __os
import json as __json

# from requests.compat import json as __json

HEADERS = {
    'x-ibm-client-id': "",
    'accept': "application/json"
}

if __os.path.exists(__os.path.join(__os.path.expanduser("~"), ".seffaflik/.kimlik")):
    with open(__os.path.join(__os.path.expanduser("~"), ".seffaflik/.kimlik"), "r") as f:
        kimlik = f.read()
        istemci_taniticisi = __json.loads(kimlik)["istemci_taniticisi"]
        HEADERS["x-ibm-client-id"] = istemci_taniticisi
