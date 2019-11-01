import os as __os
from requests.compat import json as __json
import pandas as __pd

from seffaflik.ortak.anahtar import HEADERS


def __merge_ia_dfs_evenif_empty(df_arz, df_talep):
    """
    This method merges the given dataframes. If one of them is empty, then set them zero. If both of them is empty,
    return empty dataframe.

    Parameters
    ----------
    df_arz  : ikili anlaşma arz miktarıları
    df_talep: ikili anlaşma talep miktarıları

    Return
    ------
    Arz/Talep Miktarları
    """

    df = __pd.DataFrame()
    if len(df_arz) > 0 and len(df_talep) > 0:
        df_arz.rename(index=str, columns={"quantity": "Arz Miktarı"}, inplace=True)
        df_talep.rename(index=str, columns={"quantity": "Talep Miktarı"}, inplace=True)
        df = __pd.merge(df_arz, df_talep, on="date")
    elif len(df_arz) == 0 and len(df_talep) > 0:
        df_talep.rename(index=str, columns={"quantity": "Talep Miktarı"}, inplace=True)
        df_talep["Arz Miktarı"] = 0
        df = df_talep
    elif len(df_arz) > 0 and len(df_talep) == 0:
        df_arz.rename(index=str, columns={"quantity": "Arz Miktarı"}, inplace=True)
        df_arz["Talep Miktarı"] = 0
        df = df_arz
    return df


def __change_df_eic_column_names_with_short_names(df, org):
    """
    This method changes the EIC column names with short names.

    Parameters
    ----------
    df  : DataFrame
    org: organizasyon isim ve eic değerleri

    Return
    ------
    Data Frame
    """
    new_column_names = []
    for eic in df.columns[2:]:
        short_name = org[org["EIC Kodu"] == eic]["Kısa Adı"].values[0]
        new_column_names.append(short_name)
    new_column_names.insert(0, "Saat")
    new_column_names.insert(0, "Tarih")
    df.columns = new_column_names
    return df


def kimlik_dosyasi_olustur(istemci_taniticisi):
    """Kullanıcı adı ve api anahtar ikilisini `~/.seffaflik/.kimlik` içerisinde oluşturur.

    :param api_anahtari: Şeffaflık Platformuna kayıt olurken verilen api anahtarı
    """
    HEADERS["x-ibm-client-id"] = api_anahtari
    seffaflik_dir = __os.environ.get("SEFFAFLIK_DIR",
                                     __os.path.join(__os.path.expanduser("~"), ".seffaflik"))
    if not __os.path.exists(seffaflik_dir):
        __os.makedirs(seffaflik_dir)
    kimlik_dosyasi = __os.path.join(seffaflik_dir, ".kimlik")
    kimlik_bilgisi = {"istemci_taniticisi": istemci_taniticisi}
    json_string = __json.dumps(kimlik_bilgisi, indent=4)
    with open(kimlik_dosyasi, "w") as f:
        f.write(json_string)
