import requests as __requests
import pandas as __pd
import datetime as __dt
from seffaflik.ortak.dogrulama import Dogrulama as __Dogrulama

__hata = "Bağlantı Hatası oluştu. İstek tekrar deneniyor!"
__transparency_url = "https://seffaflik.epias.com.tr/transparency/service/transmission/"


def organizasyonlar(tarih=__dt.datetime.now().strftime("%Y-%m-%d")):
    """
    İlgili tarih için organizasyonlara verilmiş olunan EIC kodlarının bilgisini vermektedir.

    Parametre
    ----------
    tarih : %YYYY-%AA-%GG formatında tarih (Varsayılan: bugün)

    Geri Dönüş Değeri
    -----------------
    Santral Bilgileri(Id, Adı, EIC Kodu, Kısa Adı)
    """
    while __Dogrulama.tarih_dogrulama(tarih):
        try:
            resp = __requests.get(
                __transparency_url + "ents-organization" + "?period=" + tarih)
            list_kisit = resp.json()["body"]["congestionRentList"]
            df_kisit = __pd.DataFrame(list_kisit)
            # df_kgup["Saat"] = df_kgup["tarih"].apply(lambda h: int(h[11:13]))
            # df_kgup["Tarih"] = __pd.to_datetime(df_kgup["tarih"].apply(lambda d: d[:10]))
            # df_kgup.rename(index=str,
            #                columns={"akarsu": "Akarsu", "barajli": "Barajlı", "biokutle": "Biokütle", "diger": "Diğer",
            #                         "dogalgaz": "Doğalgaz", "fuelOil": "Fuel Oil", "ithalKomur": "İthal Kömür",
            #                         "jeotermal": "Jeo Termal", "linyit": "Linyit", "nafta": "Nafta",
            #                         "ruzgar": "Rüzgar", "tasKomur": "Taş Kömür", "toplam": "Toplam"}, inplace=True)
            # df_kgup = df_kgup[
            #     ["Tarih", "Saat", "Akarsu", "Barajlı", "Biokütle", "Doğalgaz", "Fuel Oil", "İthal Kömür", "Jeo Termal",
            #      "Linyit", "Nafta", "Rüzgar", "Taş Kömür", "Diğer", "Toplam"]]
        except __requests.exceptions.RequestException as e:
            print(e)
        except KeyError:
            return print("İlgili tarih için veri bulunmamaktadır!")
        else:
            return df_kgup

