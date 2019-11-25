import os as __os
import json as __json
# from requests.compat import json as __json

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


def kimlik_dosyasi_olustur(istemci_taniticisi):
    """Api anahtarını `~/.seffaflik/.kimlik` içerisinde oluşturur.

    :param istemci_taniticisi: Şeffaflık Platformuna kayıt olurken verilen api anahtarı
    """
    HEADERS["x-ibm-client-id"] = istemci_taniticisi
    seffaflik_dir = __os.environ.get("SEFFAFLIK_DIR",
                                     __os.path.join(__os.path.expanduser("~"), ".seffaflik"))
    if not __os.path.exists(seffaflik_dir):
        __os.makedirs(seffaflik_dir)
    kimlik_dosyasi = __os.path.join(seffaflik_dir, ".kimlik")
    kimlik_bilgisi = {"istemci_taniticisi": istemci_taniticisi}
    json_string = __json.dumps(kimlik_bilgisi, indent=4)
    with open(kimlik_dosyasi, "w") as f:
        f.write(json_string)
