import requests as __requests
import pandas as __pd
import datetime as __dt
from multiprocessing import Pool as __Pool
import multiprocessing as __mp
from functools import reduce as __red

from seffaflik.ortak import araclar as __araclar
from seffaflik.ortak import dogrulama as __dogrulama
from seffaflik.ortak import parametreler as __param
from seffaflik.ortak import anahtar as __api

__hata = __param.BILINMEYEN_HATA_MESAJI
__transparency_url = __param.SEFFAFLIK_URL + "production/"
__headers = __api.HEADERS


def organizasyonlar():
    """
    Kesinleşmiş Gün Öncesi Üretim Planı (KGÜP) girebilecek olan organizasyon bilgilerini vermektedir.

    Parametreler
    ------------

    Geri Dönüş Değeri
    -----------------
    KGÜP Girebilen Organizasyon Bilgileri(Id, Adı, EIC Kodu, Kısa Adı, Durum)
    """
    while True:
        try:
            resp = __requests.get(__transparency_url + "dpp-organization", headers=__headers)
            list_org = resp.json()["body"]["organizations"]
            df_org = __pd.DataFrame(list_org)
            df_org.rename(index=str,
                          columns={"organizationId": "Id", "organizationName": "Adı",
                                   "organizationETSOCode": "EIC Kodu", "organizationShortName": "Kısa Adı",
                                   "organizationStatus": "Durum"},
                          inplace=True)
            df_org = df_org[["Id", "Adı", "EIC Kodu", "Kısa Adı", "Durum"]]
        except __requests.exceptions.RequestException as e:
            print(e)
        except KeyError:
            print("Veri bulunmamaktadır!")
            return __pd.DataFrame()
        else:
            return df_org


def organizasyon_cekis_birimleri(eic):
    """
    İlgili eic değeri için Kesinleşmiş Gün Öncesi Üretim Planı (KGÜP) girebilecek olan organizasyonun uzlaştırmaya
    esas veriş-çekiş birim (UEVÇB) bilgilerini vermektedir.

    Parametreler
    ------------
    eic : metin formatında organizasyon eic kodu

    Geri Dönüş Değeri
    -----------------
    KGÜP Girebilen Organizasyonun UEVÇB Bilgileri(Id, Adı, EIC Kodu)
    """

    while __dogrulama.kgup_girebilen_organizasyon_dogrulama(eic):
        try:
            resp = __requests.get(__transparency_url + "dpp-injection-unit-name?organizationEIC=" + eic,
                                  headers=__headers)
            list_unit = resp.json()["body"]["injectionUnitNames"]
            df_unit = __pd.DataFrame(list_unit)
            df_unit.rename(index=str, columns={"id": "Id", "name": "Adı", "eic": "EIC Kodu"}, inplace=True)
            df_unit = df_unit[["Id", "Adı", "EIC Kodu"]]
        except __requests.exceptions.RequestException as e:
            return print(e)
        except KeyError:
            print("İlgili organizasyon için kayıt bulunmamaktadır!")
            return __pd.DataFrame()
        else:
            return df_unit

def santral_cekis_birimleri(santral_id, tarih = __dt.datetime.today().strftime("%Y-%m-%d")):
    """
    İlgili tarih ve santral ID için santralin altında tanımlanmış uzlaştırmaya
    esas veriş-çekiş birim (UEVÇB) bilgilerini vermektedir.

    Parametreler
    ------------
    eic : metin formatında organizasyon eic kodu

    Geri Dönüş Değeri
    -----------------
    İlgili  UEVÇB Bilgileri(Id, Adı, EIC Kodu)
    """

    while __dogrulama.tarih_dogrulama(tarih):
        try:
            resp = __requests.get(__transparency_url + "uevcb?period="+tarih+
                    "&powerPlantId=" + santral_id,headers=__headers)
            list_unit = resp.json()["body"]["uevcbList"]
            df_unit = __pd.DataFrame(list_unit)
            df_unit.rename(index=str, columns={"id": "Id", "name": "Adı", "eic": "EIC Kodu"}, inplace=True)
            df_unit = df_unit[["Id", "Adı", "EIC Kodu"]]
        except __requests.exceptions.RequestException as e:
            return print(e)
        except KeyError:
            print("İlgili organizasyon için kayıt bulunmamaktadır!")
            return __pd.DataFrame()
        else:
            return df_unit


def tum_organizasyonlar_cekis_birimleri():
    """
    Kesinleşmiş Gün Öncesi Üretim Planı (KGÜP) girebilecek olan tüm organizasyon ve bu organizasyonların
    uzlaştırmaya esas veriş-çekiş birim (UEVÇB) bilgilerini vermektedir.

    Parametreler
    ------------

    Geri Dönüş Değeri
    -----------------
    KGÜP Girebilen Organizasyonlar ve UEVÇB Bilgileri(Org Id, Org Adı, Org EIC Kodu, Org Kısa Adı, Org Durum, UEVÇB Id,
    UEVÇB Adı, UEVÇB EIC Kodu)
    """
    list_org_eic = list(organizasyonlar()["EIC Kodu"])
    with __Pool(__mp.cpu_count()) as p:
        list_df_unit = p.map(__organizasyon_cekis_birimleri, list_org_eic, chunksize=1)
    return __pd.concat(list_df_unit).reset_index(drop=True)


def kgup(baslangic_tarihi=__dt.datetime.today().strftime("%Y-%m-%d"),
         bitis_tarihi=__dt.datetime.today().strftime("%Y-%m-%d"), organizasyon_eic="", uevcb_eic=""):
    """
    İlgili tarih aralığı için kaynak bazlı kesinleşmiş günlük üretim planı (KGÜP) bilgisini vermektedir.
    Not: "organizasyon_eic" değeri girildiği, "uevcb_eic" değeri girilmediği taktirde organizasyona ait tüm uevcb'lerin
    toplamı için kgüp bilgisini vermektedir. Her iki değer de girildiği taktirde ilgili organizasyonun ilgili uevcb'si
    için kgüp bilgisini vermektedir.

    Parametreler
    ------------
    baslangic_tarihi : %YYYY-%AA-%GG formatında başlangıç tarihi (Varsayılan: bugün)
    bitis_tarihi     : %YYYY-%AA-%GG formatında bitiş tarihi (Varsayılan: bugün)
    organizasyon_eic : metin formatında organizasyon eic kodu (Varsayılan: "")
    uevcb_eic        : metin formatında metin formatında uevcb eic kodu (Varsayılan: "")

    Geri Dönüş Değeri
    -----------------
    KGUP (Tarih, Saat, Doğalgaz, Barajlı, Linyit, Akarsu, İthal Kömür, Rüzgar, Fuel Oil, Jeo Termal, Taş Kömür, Biokütle
    ,Nafta, Diğer, Toplam)
    """
    while __dogrulama.baslangic_bitis_tarih_dogrulama(baslangic_tarihi, bitis_tarihi):
        try:
            resp = __requests.get(
                __transparency_url + "dpp" + "?startDate=" + baslangic_tarihi + "&endDate=" + bitis_tarihi +
                "&organizationEIC=" + organizasyon_eic + "&uevcbEIC=" + uevcb_eic, headers=__headers)
            list_kgup = resp.json()["body"]["dppList"]
            df_kgup = __pd.DataFrame(list_kgup)
            df_kgup["Saat"] = df_kgup["tarih"].apply(lambda h: int(h[11:13]))
            df_kgup["Tarih"] = __pd.to_datetime(df_kgup["tarih"].apply(lambda d: d[:10]))
            df_kgup.rename(index=str,
                           columns={"akarsu": "Akarsu", "barajli": "Barajlı", "biokutle": "Biokütle", "diger": "Diğer",
                                    "dogalgaz": "Doğalgaz", "fuelOil": "Fuel Oil", "ithalKomur": "İthal Kömür",
                                    "jeotermal": "Jeo Termal", "linyit": "Linyit", "nafta": "Nafta",
                                    "ruzgar": "Rüzgar", "tasKomur": "Taş Kömür", "toplam": "Toplam"}, inplace=True)
            df_kgup = df_kgup[["Tarih", "Saat", "Doğalgaz", "Barajlı", "Linyit", "Akarsu", "İthal Kömür", "Rüzgar",
                               "Fuel Oil", "Jeo Termal", "Taş Kömür", "Biokütle", "Nafta", "Diğer", "Toplam"]]
        except __requests.exceptions.RequestException as e:
            print(e)
        except KeyError:
            return print("İlgili tarih için veri bulunmamaktadır!")
        else:
            return df_kgup


def tum_organizasyonlar_kgup(baslangic_tarihi=__dt.datetime.today().strftime("%Y-%m-%d"),
                             bitis_tarihi=__dt.datetime.today().strftime("%Y-%m-%d")):
    """
    İlgili tarih aralığı için Kesinleşmiş Gün Öncesi Üretim Planı (KGÜP) girebilecek olan tüm organizasyonların saatlik
    KGUP bilgilerini vermektedir.

    Parametreler
    ------------
    baslangic_tarihi : %YYYY-%AA-%GG formatında başlangıç tarihi (Varsayılan: bugün)
    bitis_tarihi     : %YYYY-%AA-%GG formatında bitiş tarihi (Varsayılan: bugün)

    Geri Dönüş Değeri
    -----------------
    KGÜP Girebilen Organizasyonların KGUP Değerleri (Tarih, Saat, KGUP)
    """
    if __dogrulama.baslangic_bitis_tarih_dogrulama(baslangic_tarihi, bitis_tarihi):
        org = organizasyonlar()
        list_org_eic = list(org["EIC Kodu"])
        org_len = len(list_org_eic)
        list_date_org_eic = list(zip([baslangic_tarihi] * org_len, [bitis_tarihi] * org_len, list_org_eic))
        list_date_org_eic = list(map(list, list_date_org_eic))
        with __Pool(__mp.cpu_count()) as p:
            list_df_unit = p.starmap(__kgup, list_date_org_eic, chunksize=1)
        list_df_unit = list(filter(lambda x: len(x) > 0, list_df_unit))
        df_unit = __red(lambda left, right: __pd.merge(left, right, how="outer", on=["Tarih", "Saat"], sort=True),
                        list_df_unit)
        df_unit = __araclar.__change_df_eic_column_names_with_short_names(df_unit, org)
        return df_unit


def eak(baslangic_tarihi=__dt.datetime.today().strftime("%Y-%m-%d"),
        bitis_tarihi=__dt.datetime.today().strftime("%Y-%m-%d"), organizasyon_eic="", uevcb_eic=""):
    """
    İlgili tarih aralığı için kaynak bazlı emre amade kapasite (EAK) bilgisini vermektedir.
    Not: "organizasyon_eic" değeri girildiği, "uevcb_eic" değeri girilmediği taktirde organizasyona ait tüm uevcb'lerin
    toplamı için eak bilgisini vermektedir. Her iki değer de girildiği taktirde ilgili organizasyonun ilgili uevcb'si
    için kgüp bilgisini vermektedir.

    Parametreler
    ------------
    baslangic_tarihi : %YYYY-%AA-%GG formatında başlangıç tarihi (Varsayılan: bugün)
    bitis_tarihi     : %YYYY-%AA-%GG formatında bitiş tarihi (Varsayılan: bugün)
    organizasyon_eic : metin formatında organizasyon eic kodu (Varsayılan: "")
    uevcb_eic        : metin formatında metin formatında uevcb eic kodu (Varsayılan: "")

    Geri Dönüş Değeri
    -----------------
    EAK (Tarih, Saat, Doğalgaz, Barajlı, Linyit, Akarsu, İthal Kömür, Rüzgar, Fuel Oil, Jeo Termal, Taş Kömür, Biokütle,
    Nafta, Diğer, Toplam)
    """
    while __dogrulama.baslangic_bitis_tarih_dogrulama(baslangic_tarihi, bitis_tarihi):
        try:
            resp = __requests.get(
                __transparency_url + "aic" + "?startDate=" + baslangic_tarihi + "&endDate=" + bitis_tarihi +
                "&organizationEIC=" + organizasyon_eic + "&uevcbEIC=" + uevcb_eic, headers=__headers)
            list_aic = resp.json()["body"]["aicList"]
            df_aic = __pd.DataFrame(list_aic)
            df_aic["Saat"] = df_aic["tarih"].apply(lambda h: int(h[11:13]))
            df_aic["Tarih"] = __pd.to_datetime(df_aic["tarih"].apply(lambda d: d[:10]))
            df_aic.rename(index=str,
                          columns={"akarsu": "Akarsu", "barajli": "Barajlı", "biokutle": "Biokütle", "diger": "Diğer",
                                   "dogalgaz": "Doğalgaz", "fuelOil": "Fuel Oil", "ithalKomur": "İthal Kömür",
                                   "jeotermal": "Jeo Termal", "linyit": "Linyit", "nafta": "Nafta",
                                   "ruzgar": "Rüzgar", "tasKomur": "Taş Kömür", "toplam": "Toplam"}, inplace=True)
            df_aic = df_aic[["Tarih", "Saat", "Doğalgaz", "Barajlı", "Linyit", "Akarsu", "İthal Kömür", "Rüzgar",
                             "Fuel Oil", "Jeo Termal", "Taş Kömür", "Biokütle", "Nafta", "Diğer", "Toplam"]]
        except __requests.exceptions.RequestException as e:
            print(e)
        except KeyError:
            return print("İlgili tarih için veri bulunmamaktadır!")
        else:
            return df_aic


def tum_organizasyonlar_eak(baslangic_tarihi=__dt.datetime.today().strftime("%Y-%m-%d"),
                            bitis_tarihi=__dt.datetime.today().strftime("%Y-%m-%d")):
    """
    İlgili tarih aralığı için Emre Amade Kapasite (EAK) girebilecek olan tüm organizasyonların saatlik EAK bilgilerini
    vermektedir.

    Parametreler
    ------------
    baslangic_tarihi : %YYYY-%AA-%GG formatında başlangıç tarihi (Varsayılan: bugün)
    bitis_tarihi     : %YYYY-%AA-%GG formatında bitiş tarihi (Varsayılan: bugün)

    Geri Dönüş Değeri
    -----------------
    EAK Girebilen Organizasyonların EAK Değerleri (Tarih, Saat, EAK)
    """
    if __dogrulama.baslangic_bitis_tarih_dogrulama(baslangic_tarihi, bitis_tarihi):
        org = organizasyonlar()
        list_org_eic = list(org["EIC Kodu"])
        org_len = len(list_org_eic)
        list_date_org_eic = list(zip([baslangic_tarihi] * org_len, [bitis_tarihi] * org_len, list_org_eic))
        list_date_org_eic = list(map(list, list_date_org_eic))
        with __Pool(__mp.cpu_count()) as p:
            list_df_unit = p.starmap(__eak, list_date_org_eic, chunksize=1)
        list_df_unit = list(filter(lambda x: len(x) > 0, list_df_unit))
        df_unit = __red(lambda left, right: __pd.merge(left, right, how="outer", on=["Tarih", "Saat"], sort=True),
                        list_df_unit)
        df_unit = __araclar.__change_df_eic_column_names_with_short_names(df_unit, org)
        return df_unit


def kudup(baslangic_tarihi=__dt.datetime.today().strftime("%Y-%m-%d"),
          bitis_tarihi=__dt.datetime.today().strftime("%Y-%m-%d"), organizasyon_eic="", uevcb_eic=""):
    """
    İlgili tarih aralığı için gün içi piyasasının kapanışından sonra yapılan güncellemeyle kaynak bazlı Kesinleşmiş
    Uzlaştırma Dönemi Üretim Planı (KUDÜP) bilgisini vermektedir.
    Not: "organizasyon_eic" değeri girildiği, "uevcb_eic" değeri girilmediği taktirde organizasyona ait tüm uevcb'lerin
    toplamı için kgüp bilgisini vermektedir. Her iki değer de girildiği taktirde ilgili organizasyonun ilgili uevcb'si
    için kgüp bilgisini vermektedir.

    Parametreler
    ------------
    baslangic_tarihi : %YYYY-%AA-%GG formatında başlangıç tarihi (Varsayılan: bugün)
    bitis_tarihi     : %YYYY-%AA-%GG formatında bitiş tarihi (Varsayılan: bugün)
    organizasyon_eic : metin formatında organizasyon eic kodu (Varsayılan: "")
    uevcb_eic        : metin formatında metin formatında uevcb eic kodu (Varsayılan: "")

    Geri Dönüş Değeri
    -----------------
    KUDÜP (Tarih, Saat, Doğalgaz, Barajlı, Linyit, Akarsu, İthal Kömür, Rüzgar, Fuel Oil, Jeo Termal, Taş Kömür,
    Biokütle, Nafta, Diğer, Toplam)
    """
    while __dogrulama.baslangic_bitis_tarih_dogrulama(baslangic_tarihi, bitis_tarihi):
        try:
            resp = __requests.get(
                __transparency_url + "sbfgp" + "?startDate=" + baslangic_tarihi + "&endDate=" + bitis_tarihi +
                "&organizationEIC=" + organizasyon_eic + "&uevcbEIC=" + uevcb_eic, headers=__headers)
            list_kgup = resp.json()["body"]["dppList"]
            df_kgup = __pd.DataFrame(list_kgup)
            df_kgup["Saat"] = df_kgup["tarih"].apply(lambda h: int(h[11:13]))
            df_kgup["Tarih"] = __pd.to_datetime(df_kgup["tarih"].apply(lambda d: d[:10]))
            df_kgup.rename(index=str,
                           columns={"akarsu": "Akarsu", "barajli": "Barajlı", "biokutle": "Biokütle", "diger": "Diğer",
                                    "dogalgaz": "Doğalgaz", "fuelOil": "Fuel Oil", "ithalKomur": "İthal Kömür",
                                    "jeotermal": "Jeo Termal", "linyit": "Linyit", "nafta": "Nafta",
                                    "ruzgar": "Rüzgar", "tasKomur": "Taş Kömür", "toplam": "Toplam"}, inplace=True)
            df_kgup = df_kgup[["Tarih", "Saat", "Doğalgaz", "Barajlı", "Linyit", "Akarsu", "İthal Kömür", "Rüzgar",
                               "Fuel Oil", "Jeo Termal", "Taş Kömür", "Biokütle", "Nafta", "Diğer", "Toplam"]]
        except __requests.exceptions.RequestException as e:
            print(e)
        except KeyError:
            return print("İlgili tarih için veri bulunmamaktadır!")
        else:
            return df_kgup


def gerceklesen(baslangic_tarihi=__dt.datetime.today().strftime("%Y-%m-%d"),
                bitis_tarihi=__dt.datetime.today().strftime("%Y-%m-%d")):
    """
    İlgili tarih aralığı için lisanslı santrallerin toplam gerçek zamanlı üretim bilgisini vermektedir.

    Parametreler
    ------------
    baslangic_tarihi : %YYYY-%AA-%GG formatında başlangıç tarihi (Varsayılan: bugün)
    bitis_tarihi     : %YYYY-%AA-%GG formatında bitiş tarihi (Varsayılan: bugün)

    Geri Dönüş Değeri
    -----------------
    Gerçek Zamanlı Üretim(Id, Adı, EIC Kodu, Kısa Adı)
    """
    while __dogrulama.baslangic_bitis_tarih_dogrulama(baslangic_tarihi, bitis_tarihi):
        try:
            resp = __requests.get(
                __transparency_url + "real-time-generation" + "?startDate=" + baslangic_tarihi + "&endDate="
                + bitis_tarihi, headers=__headers)
            list_uretim = resp.json()["body"]["hourlyGenerations"]
            df_uretim = __pd.DataFrame(list_uretim)
            df_uretim["Saat"] = df_uretim["date"].apply(lambda h: int(h[11:13]))
            df_uretim["Tarih"] = __pd.to_datetime(df_uretim["date"].apply(lambda d: d[:10]))
            df_uretim.rename(index=str,
                             columns={"asphaltiteCoal": "Asfaltit Kömür", "river": "Akarsu", "dammedHydro": "Barajlı",
                                      "biomass": "Biokütle", "sun": "Güneş", "naturalGas": "Doğalgaz",
                                      "fueloil": "Fuel Oil", "importCoal": "İthal Kömür", "geothermal": "Jeo Termal",
                                      "lignite": "Linyit", "naphta": "Nafta", "lng": "LNG", "wind": "Rüzgar",
                                      "blackCoal": "Taş Kömür", "importExport": "Uluslararası", "total": "Toplam"},
                             inplace=True)
            df_uretim = df_uretim[
                ["Tarih", "Saat", "Doğalgaz", "Barajlı", "Linyit", "Akarsu", "İthal Kömür", "Rüzgar", "Güneş",
                 "Fuel Oil", "Jeo Termal", "Asfaltit Kömür", "Taş Kömür", "Biokütle", "Nafta", "LNG", "Uluslararası",
                 "Toplam"]]
        except __requests.exceptions.RequestException as e:
            print(e)
        except KeyError:
            return print("İlgili tarih için veri bulunmamaktadır!")
        else:
            return df_uretim


def __organizasyon_cekis_birimleri(eic):
    """
    İlgili eic değeri için Kesinleşmiş Gün Öncesi Üretim Planı (KGÜP) girebilecek olan organizasyonun uzlaştırmaya
    esas veriş-çekiş birim (UEVÇB) bilgilerini ve organizasyona dair bilgileri vermektedir.

    Parametreler
    ------------
    eic : metin formatında organizasyon eic kodu

    Geri Dönüş Değeri
    -----------------
    KGÜP Girebilen Organizasyon Bilgileri(Org Id, Org Adı, Org EIC Kodu, Org Kısa Adı,Org Durum, UEVÇB Id, UEVÇB Adı,
    UEVÇB EIC Kodu)
    """

    while __dogrulama.kgup_girebilen_organizasyon_dogrulama(eic):
        try:
            resp = __requests.get(__transparency_url + "dpp-injection-unit-name?organizationEIC=" + eic,
                                  headers=__headers)
            list_unit = resp.json()["body"]["injectionUnitNames"]
            df_unit = __pd.DataFrame(list_unit)
            df_org = organizasyonlar()
            org_info = df_org[df_org["EIC Kodu"] == eic]
            df_unit["Org Id"] = org_info["Id"].values[0]
            df_unit["Org Adı"] = org_info["Adı"].values[0]
            df_unit["Org EIC Kodu"] = org_info["EIC Kodu"].values[0]
            df_unit["Org Kısa Adı"] = org_info["Kısa Adı"].values[0]
            df_unit["Org Durum"] = org_info["Durum"].values[0]
            df_unit.rename(index=str,
                           columns={"id": "UEVÇB Id", "name": "UEVÇB Adı", "eic": "UEVÇB EIC Kodu"},
                           inplace=True)
            df_unit = df_unit[["Org Id", "Org Adı", "Org EIC Kodu", "Org Kısa Adı",
                               "Org Durum", "UEVÇB Id", "UEVÇB Adı", "UEVÇB EIC Kodu"]]
        except __requests.exceptions.RequestException as e:
            return print(e)
        except KeyError:
            return __pd.DataFrame()
        else:
            return df_unit


def __kgup(baslangic_tarihi, bitis_tarihi, organizasyon_eic):
    """
    İlgili tarih aralığı ve organizasyon için  kesinleşmiş günlük üretim prgoramı (KGÜP) bilgisini vermektedir.

    Parametreler
    ------------
    baslangic_tarihi : %YYYY-%AA-%GG formatında başlangıç tarihi (Varsayılan: bugün)
    bitis_tarihi     : %YYYY-%AA-%GG formatında bitiş tarihi (Varsayılan: bugün)
    organizasyon_eic : metin formatında organizasyon eic kodu (Varsayılan: "")

    Geri Dönüş Değeri
    -----------------
    Tum organizasyon KGUP değerleri
    """
    while __dogrulama.baslangic_bitis_tarih_dogrulama(baslangic_tarihi, bitis_tarihi):
        try:
            resp = __requests.get(
                __transparency_url + "dpp" + "?startDate=" + baslangic_tarihi + "&endDate=" + bitis_tarihi +
                "&organizationEIC=" + organizasyon_eic, headers=__headers)
            list_kgup = resp.json()["body"]["dppList"]
            df_kgup = __pd.DataFrame(list_kgup)
            df_kgup["Saat"] = df_kgup["tarih"].apply(lambda h: int(h[11:13]))
            df_kgup["Tarih"] = __pd.to_datetime(df_kgup["tarih"].apply(lambda d: d[:10]))
            df_kgup.rename(index=str, columns={"toplam": organizasyon_eic}, inplace=True)
            df_kgup = df_kgup[["Tarih", "Saat", organizasyon_eic]]
        except __requests.exceptions.RequestException as e:
            print(e)
        except KeyError:
            return __pd.DataFrame()
        else:
            return df_kgup


def __eak(baslangic_tarihi=__dt.datetime.today().strftime("%Y-%m-%d"),
          bitis_tarihi=__dt.datetime.today().strftime("%Y-%m-%d"), organizasyon_eic=""):
    """
    İlgili tarih aralığı ve organizasyon için emre amade kapasite (EAK) bilgisini vermektedir.

    Parametreler
    ------------
    baslangic_tarihi : %YYYY-%AA-%GG formatında başlangıç tarihi (Varsayılan: bugün)
    bitis_tarihi     : %YYYY-%AA-%GG formatında bitiş tarihi (Varsayılan: bugün)
    organizasyon_eic : metin formatında organizasyon eic kodu (Varsayılan: "")

    Geri Dönüş Değeri
    -----------------
    Tum organizasyon EAK değerleri
    """
    while __dogrulama.baslangic_bitis_tarih_dogrulama(baslangic_tarihi, bitis_tarihi):
        try:
            resp = __requests.get(
                __transparency_url + "aic" + "?startDate=" + baslangic_tarihi + "&endDate=" + bitis_tarihi +
                "&organizationEIC=" + organizasyon_eic, headers=__headers)
            list_aic = resp.json()["body"]["aicList"]
            df_aic = __pd.DataFrame(list_aic)
            df_aic["Saat"] = df_aic["tarih"].apply(lambda h: int(h[11:13]))
            df_aic["Tarih"] = __pd.to_datetime(df_aic["tarih"].apply(lambda d: d[:10]))
            df_aic.rename(index=str, columns={"toplam": organizasyon_eic}, inplace=True)
            df_aic = df_aic[["Tarih", "Saat", organizasyon_eic]]
        except __requests.exceptions.RequestException as e:
            print(e)
        except KeyError:
            return __pd.DataFrame()
        else:
            return df_aic
