import requests as __requests
from requests import ConnectionError as __ConnectionError
from requests.exceptions import HTTPError as __HTTPError, RequestException as __RequestException, Timeout as __Timeout
import pandas as __pd
import datetime as __dt
from multiprocessing import Pool as __Pool
import multiprocessing as __mp
from functools import reduce as __red
import logging as __logging

from seffaflik.__ortak import __dogrulama as __dogrulama, __parametreler as __param, __anahtar as __api

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
    try:
        resp = __requests.get(__transparency_url + "dpp-organization", headers=__headers, timeout=__param.__timeout)
        resp.raise_for_status()
        list_org = resp.json()["body"]["organizations"]
        df_org = __pd.DataFrame(list_org)
        df_org.rename(index=str,
                      columns={"organizationId": "Id", "organizationName": "Adı",
                               "organizationETSOCode": "EIC Kodu", "organizationShortName": "Kısa Adı",
                               "organizationStatus": "Durum"},
                      inplace=True)
        df_org = df_org[["Id", "Adı", "EIC Kodu", "Kısa Adı", "Durum"]]
    except __ConnectionError:
        __logging.error(__param.__requestsConnectionErrorLogging, exc_info=False)
    except __Timeout:
        __logging.error(__param.__requestsTimeoutErrorLogging, exc_info=False)
    except __HTTPError as e:
        __dogrulama.__check_http_error(e.response.status_code)
    except __RequestException:
        __logging.error(__param.__request_error, exc_info=False)
    except KeyError:
        return __pd.DataFrame()
    else:
        return df_org


def organizasyon_veris_cekis_birimleri(eic):
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

    if __dogrulama.__kgup_girebilen_organizasyon_dogrulama(eic):
        try:
            resp = __requests.get(__transparency_url + "dpp-injection-unit-name?organizationEIC=" + eic,
                                  headers=__headers, timeout=__param.__timeout)
            resp.raise_for_status()
            list_unit = resp.json()["body"]["injectionUnitNames"]
            df_unit = __pd.DataFrame(list_unit)
            df_unit.rename(index=str, columns={"id": "Id", "name": "Adı", "eic": "EIC Kodu"}, inplace=True)
            df_unit = df_unit[["Id", "Adı", "EIC Kodu"]]
        except __ConnectionError:
            __logging.error(__param.__requestsConnectionErrorLogging, exc_info=False)
        except __Timeout:
            __logging.error(__param.__requestsTimeoutErrorLogging, exc_info=False)
        except __HTTPError as e:
            __dogrulama.__check_http_error(e.response.status_code)
        except __RequestException:
            __logging.error(__param.__request_error, exc_info=False)
        except KeyError:
            return __pd.DataFrame()
        else:
            return df_unit


def tum_organizasyonlar_veris_cekis_birimleri():
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
    list_org = organizasyonlar()[["Id", "Adı", "EIC Kodu", "Kısa Adı", "Durum"]].to_dict("records")
    with __Pool(__mp.cpu_count()) as p:
        list_df_unit = p.map(__organizasyon_cekis_birimleri, list_org, chunksize=1)
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
    if __dogrulama.__baslangic_bitis_tarih_dogrulama(baslangic_tarihi, bitis_tarihi):
        try:
            resp = __requests.get(
                __transparency_url + "dpp" + "?startDate=" + baslangic_tarihi + "&endDate=" + bitis_tarihi +
                "&organizationEIC=" + organizasyon_eic + "&uevcbEIC=" + uevcb_eic, headers=__headers,
                timeout=__param.__timeout)
            resp.raise_for_status()
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
        except __ConnectionError:
            __logging.error(__param.__requestsConnectionErrorLogging, exc_info=False)
        except __Timeout:
            __logging.error(__param.__requestsTimeoutErrorLogging, exc_info=False)
        except __HTTPError as e:
            __dogrulama.__check_http_error(e.response.status_code)
        except __RequestException:
            __logging.error(__param.__request_error, exc_info=False)
        except KeyError:
            return __pd.DataFrame()
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
    if __dogrulama.__baslangic_bitis_tarih_dogrulama(baslangic_tarihi, bitis_tarihi):
        org = organizasyonlar()
        list_org = org[["EIC Kodu", "Kısa Adı"]].to_dict("records")
        org_len = len(list_org)
        list_date_org_eic = list(zip([baslangic_tarihi] * org_len, [bitis_tarihi] * org_len, list_org))
        list_date_org_eic = list(map(list, list_date_org_eic))
        with __Pool(__mp.cpu_count()) as p:
            list_df_unit = p.starmap(__kgup, list_date_org_eic, chunksize=1)
        list_df_unit = list(filter(lambda x: len(x) > 0, list_df_unit))
        df_unit = __red(lambda left, right: __pd.merge(left, right, how="outer", on=["Tarih", "Saat"], sort=True),
                        list_df_unit)
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
    if __dogrulama.__baslangic_bitis_tarih_dogrulama(baslangic_tarihi, bitis_tarihi):
        try:
            resp = __requests.get(
                __transparency_url + "aic" + "?startDate=" + baslangic_tarihi + "&endDate=" + bitis_tarihi +
                "&organizationEIC=" + organizasyon_eic + "&uevcbEIC=" + uevcb_eic, headers=__headers,
                timeout=__param.__timeout)
            resp.raise_for_status()
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
        except __ConnectionError:
            __logging.error(__param.__requestsConnectionErrorLogging, exc_info=False)
        except __Timeout:
            __logging.error(__param.__requestsTimeoutErrorLogging, exc_info=False)
        except __HTTPError as e:
            __dogrulama.__check_http_error(e.response.status_code)
        except __RequestException:
            __logging.error(__param.__request_error, exc_info=False)
        except KeyError:
            return __pd.DataFrame()
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
    if __dogrulama.__baslangic_bitis_tarih_dogrulama(baslangic_tarihi, bitis_tarihi):
        org = organizasyonlar()
        list_org = org[["EIC Kodu", "Kısa Adı"]].to_dict("records")
        org_len = len(list_org)
        list_date_org_eic = list(zip([baslangic_tarihi] * org_len, [bitis_tarihi] * org_len, list_org))
        list_date_org_eic = list(map(list, list_date_org_eic))
        with __Pool(__mp.cpu_count()) as p:
            list_df_unit = p.starmap(__eak, list_date_org_eic, chunksize=1)
        list_df_unit = list(filter(lambda x: len(x) > 0, list_df_unit))
        df_unit = __red(lambda left, right: __pd.merge(left, right, how="outer", on=["Tarih", "Saat"], sort=True),
                        list_df_unit)
        return df_unit


def kudup(baslangic_tarihi=__dt.datetime.today().strftime("%Y-%m-%d"),
          bitis_tarihi=__dt.datetime.today().strftime("%Y-%m-%d"), organizasyon_id="", uevcb_id=""):
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
    organizasyon_id  : metin formatında organizasyon id (Varsayılan: "")
    uevcb_id         : metin formatında uevcb id (Varsayılan: "")

    Geri Dönüş Değeri
    -----------------
    KUDÜP (Tarih, Saat, Doğalgaz, Barajlı, Linyit, Akarsu, İthal Kömür, Rüzgar, Fuel Oil, Jeo Termal, Taş Kömür,
    Biokütle, Nafta, Diğer, Toplam)
    """
    if __dogrulama.__baslangic_bitis_tarih_dogrulama(baslangic_tarihi, bitis_tarihi):
        try:
            resp = __requests.get(
                __transparency_url + "sbfgp" + "?startDate=" + baslangic_tarihi + "&endDate=" + bitis_tarihi +
                "&organizationEIC=" + organizasyon_id + "&uevcbEIC=" + uevcb_id, headers=__headers,
                timeout=__param.__timeout)
            resp.raise_for_status()
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
        except __ConnectionError:
            __logging.error(__param.__requestsConnectionErrorLogging, exc_info=False)
        except __Timeout:
            __logging.error(__param.__requestsTimeoutErrorLogging, exc_info=False)
        except __HTTPError as e:
            __dogrulama.__check_http_error(e.response.status_code)
        except __RequestException:
            __logging.error(__param.__request_error, exc_info=False)
        except KeyError:
            return __pd.DataFrame()
        else:
            return df_kgup


def uevm(baslangic_tarihi=__dt.datetime.today().strftime("%Y-%m-%d"),
         bitis_tarihi=__dt.datetime.today().strftime("%Y-%m-%d")):
    """
    İlgili tarih aralığı için saatlik Uzlaştırmaya Esas Variş Miktarı (UEVM) bilgisini vermektedir.

    Parametreler
    ------------
    baslangic_tarihi : %YYYY-%AA-%GG formatında başlangıç tarihi (Varsayılan: bugün)
    bitis_tarihi     : %YYYY-%AA-%GG formatında bitiş tarihi (Varsayılan: bugün)

    Geri Dönüş Değeri
    -----------------
    Uzlaştırmaya Esas Veriş Miktarı (Tarih, Saat, UEVM)

    """
    if __dogrulama.__baslangic_bitis_tarih_dogrulama(baslangic_tarihi, bitis_tarihi):
        try:
            resp = __requests.get(
                __transparency_url + "ssv-categorized" + "?startDate=" + baslangic_tarihi +
                "&endDate=" + bitis_tarihi, headers=__headers, timeout=__param.__timeout)
            resp.raise_for_status()
            list_uevm = resp.json()["body"]["ssvList"]
            df_uevm = __pd.DataFrame(list_uevm)
            df_uevm["Saat"] = df_uevm["date"].apply(lambda h: int(h[11:13]))
            df_uevm["Tarih"] = __pd.to_datetime(df_uevm["date"].apply(lambda d: d[:10]))
            df_uevm.rename(index=str,
                           columns={"asphaltite": "Asfaltit Kömür", "river": "Akarsu", "dam": "Barajlı",
                                    "biomass": "Biokütle", "naturalGas": "Doğalgaz", "fueloil": "Fuel Oil",
                                    "importedCoal": "İthal Kömür", "geothermal": "Jeo Termal", "lignite": "Linyit",
                                    "naphtha": "Nafta", "lng": "LNG", "wind": "Rüzgar", "stonecoal": "Taş Kömür",
                                    "international": "Uluslararası", "total": "Toplam", "other": "Diğer"},
                           inplace=True)
            df_uevm = df_uevm[
                ["Tarih", "Saat", "Doğalgaz", "Barajlı", "Linyit", "Akarsu", "İthal Kömür", "Rüzgar",
                 "Fuel Oil", "Jeo Termal", "Asfaltit Kömür", "Taş Kömür", "Biokütle", "Nafta", "LNG", "Uluslararası",
                 "Diğer", "Toplam"]]
        except __ConnectionError:
            __logging.error(__param.__requestsConnectionErrorLogging, exc_info=False)
        except __Timeout:
            __logging.error(__param.__requestsTimeoutErrorLogging, exc_info=False)
        except __HTTPError as e:
            __dogrulama.__check_http_error(e.response.status_code)
        except __RequestException:
            __logging.error(__param.__request_error, exc_info=False)
        except KeyError:
            return __pd.DataFrame()
        else:
            return df_uevm


def gerceklesen(baslangic_tarihi=__dt.datetime.today().strftime("%Y-%m-%d"),
                bitis_tarihi=__dt.datetime.today().strftime("%Y-%m-%d"), santral_id=""):
    """
    İlgili tarih aralığı için lisanslı santrallerin toplam gerçek zamanlı üretim bilgisini vermektedir.
    Not: "santral_id" değeri girildiği taktirde santrale ait gerçek zamanlı üretim bilgisini vermektedir.
    Girilmediği taktirde toplam gerçek zamanlı üretim bilgisini vermektedir.

    Parametreler
    ------------
    baslangic_tarihi : %YYYY-%AA-%GG formatında başlangıç tarihi (Varsayılan: bugün)
    bitis_tarihi     : %YYYY-%AA-%GG formatında bitiş tarihi (Varsayılan: bugün)
    santral_id       : metin yada tam sayı formatında santral id (Varsayılan: "")

    Geri Dönüş Değeri
    -----------------
    Gerçek Zamanlı Üretim("Tarih", "Saat", "Doğalgaz", "Barajlı", "Linyit", "Akarsu", "İthal Kömür", "Rüzgar", "Güneş",
                 "Fuel Oil", "Jeo Termal", "Asfaltit Kömür", "Taş Kömür", "Biokütle", "Nafta", "LNG", "Uluslararası",
                 "Toplam")
    """
    if __dogrulama.__baslangic_bitis_tarih_id_dogrulama(baslangic_tarihi, bitis_tarihi, santral_id):
        if santral_id == "":
            return __gerceklesen(baslangic_tarihi, bitis_tarihi)
        else:
            return __santral_bazli_gerceklesen(baslangic_tarihi, bitis_tarihi, santral_id)


def __gerceklesen(baslangic_tarihi=__dt.datetime.today().strftime("%Y-%m-%d"),
                  bitis_tarihi=__dt.datetime.today().strftime("%Y-%m-%d")):
    """
    İlgili tarih aralığı için lisanslı santrallerin toplam gerçek zamanlı üretim bilgisini vermektedir.

    Parametreler
    ------------
    baslangic_tarihi : %YYYY-%AA-%GG formatında başlangıç tarihi (Varsayılan: bugün)
    bitis_tarihi     : %YYYY-%AA-%GG formatında bitiş tarihi (Varsayılan: bugün)

    Geri Dönüş Değeri
    -----------------
    Gerçek Zamanlı Üretim("Tarih", "Saat", "Doğalgaz", "Barajlı", "Linyit", "Akarsu", "İthal Kömür", "Rüzgar", "Güneş",
                 "Fuel Oil", "Jeo Termal", "Asfaltit Kömür", "Taş Kömür", "Biokütle", "Nafta", "LNG", "Uluslararası",
                 "Toplam")
    """
    try:
        resp = __requests.get(
            __transparency_url + "real-time-generation" + "?startDate=" + baslangic_tarihi + "&endDate="
            + bitis_tarihi, headers=__headers, timeout=__param.__timeout)
        resp.raise_for_status()
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
    except __ConnectionError:
        __logging.error(__param.__requestsConnectionErrorLogging, exc_info=False)
    except __Timeout:
        __logging.error(__param.__requestsTimeoutErrorLogging, exc_info=False)
    except __HTTPError as e:
        __dogrulama.__check_http_error(e.response.status_code)
    except __RequestException:
        __logging.error(__param.__request_error, exc_info=False)
    except KeyError:
        return __pd.DataFrame()
    else:
        return df_uretim


def __santral_bazli_gerceklesen(baslangic_tarihi, bitis_tarihi, santral_id):
    """
    İlgili tarih aralığı ve santral için gerçek zamanlı üretim bilgisini vermektedir.

    Parametreler
    ------------
    baslangic_tarihi
    bitis_tarihi
    santral_id

    Geri Dönüş Değeri
    -----------------
    Santral Bazlı Gerçek Zamanlı Üretim("Tarih", "Saat", "Doğalgaz", "Barajlı", "Linyit", "Akarsu", "İthal Kömür",
                "Rüzgar", "Güneş", "Fuel Oil", "Jeo Termal", "Asfaltit Kömür", "Taş Kömür", "Biokütle", "Nafta", "LNG",
                "Uluslararası", "Toplam")
    """
    try:
        resp = __requests.get(
            __transparency_url + "real-time-generation_with_powerplant" + "?startDate=" + baslangic_tarihi +
            "&endDate=" + bitis_tarihi + "&powerPlantId=" + str(santral_id), headers=__headers,
            timeout=__param.__timeout)
        resp.raise_for_status()
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
    except __ConnectionError:
        __logging.error(__param.__requestsConnectionErrorLogging, exc_info=False)
    except __Timeout:
        __logging.error(__param.__requestsTimeoutErrorLogging, exc_info=False)
    except __HTTPError as e:
        __dogrulama.__check_http_error(e.response.status_code)
    except __RequestException:
        __logging.error(__param.__request_error, exc_info=False)
    except KeyError:
        return __pd.DataFrame()
    else:
        return df_uretim


def __organizasyon_cekis_birimleri(org):
    """
    İlgili eic değeri için Kesinleşmiş Gün Öncesi Üretim Planı (KGÜP) girebilecek olan organizasyonun uzlaştırmaya
    esas veriş-çekiş birim (UEVÇB) bilgilerini ve organizasyona dair bilgileri vermektedir.

    Parametreler
    ------------
    eic : dict formatında organizasyon Id, Adı, EIC Kodu, Kısa Adı, Durum

    Geri Dönüş Değeri
    -----------------
    KGÜP Girebilen Organizasyon Bilgileri(Org Id, Org Adı, Org EIC Kodu, Org Kısa Adı,Org Durum, UEVÇB Id, UEVÇB Adı,
    UEVÇB EIC Kodu)
    """
    try:
        resp = __requests.get(__transparency_url + "dpp-injection-unit-name?organizationEIC=" + org["EIC Kodu"],
                              headers=__headers, timeout=__param.__timeout)
        resp.raise_for_status()
        list_unit = resp.json()["body"]["injectionUnitNames"]
        df_unit = __pd.DataFrame(list_unit)
        df_unit["Org Id"] = org["Id"]
        df_unit["Org Adı"] = org["Adı"]
        df_unit["Org EIC Kodu"] = org["EIC Kodu"]
        df_unit["Org Kısa Adı"] = org["Kısa Adı"]
        df_unit["Org Durum"] = org["Durum"]
        df_unit.rename(index=str,
                       columns={"id": "UEVÇB Id", "name": "UEVÇB Adı", "eic": "UEVÇB EIC Kodu"},
                       inplace=True)
        df_unit = df_unit[["Org Id", "Org Adı", "Org EIC Kodu", "Org Kısa Adı",
                           "Org Durum", "UEVÇB Id", "UEVÇB Adı", "UEVÇB EIC Kodu"]]
    except __ConnectionError:
        __logging.error(__param.__requestsConnectionErrorLogging, exc_info=False)
    except __Timeout:
        __logging.error(__param.__requestsTimeoutErrorLogging, exc_info=False)
    except __HTTPError as e:
        __dogrulama.__check_http_error(e.response.status_code)
    except __RequestException:
        __logging.error(__param.__request_error, exc_info=False)
    except KeyError:
        return __pd.DataFrame()
    else:
        return df_unit


def __kgup(baslangic_tarihi, bitis_tarihi, org):
    """
    İlgili tarih aralığı ve organizasyon için  kesinleşmiş günlük üretim prgoramı (KGÜP) bilgisini vermektedir.

    Parametreler
    ------------
    baslangic_tarihi : %YYYY-%AA-%GG formatında başlangıç tarihi
    bitis_tarihi     : %YYYY-%AA-%GG formatında bitiş tarihi
    org              : dict formatında organizasyon EIC Kodu, Kısa Adı

    Geri Dönüş Değeri
    -----------------
    Tum organizasyon KGUP değerleri
    """
    if __dogrulama.__baslangic_bitis_tarih_dogrulama(baslangic_tarihi, bitis_tarihi):
        try:
            resp = __requests.get(
                __transparency_url + "dpp" + "?startDate=" + baslangic_tarihi + "&endDate=" + bitis_tarihi +
                "&organizationEIC=" + org["EIC Kodu"], headers=__headers, timeout=__param.__timeout)
            resp.raise_for_status()
            list_kgup = resp.json()["body"]["dppList"]
            df_kgup = __pd.DataFrame(list_kgup)
            df_kgup["Saat"] = df_kgup["tarih"].apply(lambda h: int(h[11:13]))
            df_kgup["Tarih"] = __pd.to_datetime(df_kgup["tarih"].apply(lambda d: d[:10]))
            df_kgup.rename(index=str, columns={"toplam": org["Kısa Adı"]}, inplace=True)
            df_kgup = df_kgup[["Tarih", "Saat", org["Kısa Adı"]]]
        except __ConnectionError:
            __logging.error(__param.__requestsConnectionErrorLogging, exc_info=False)
        except __Timeout:
            __logging.error(__param.__requestsTimeoutErrorLogging, exc_info=False)
        except __HTTPError as e:
            __dogrulama.__check_http_error(e.response.status_code)
        except __RequestException:
            __logging.error(__param.__request_error, exc_info=False)
        except KeyError:
            return __pd.DataFrame()
        else:
            return df_kgup


def __eak(baslangic_tarihi, bitis_tarihi, org):
    """
    İlgili tarih aralığı ve organizasyon için emre amade kapasite (EAK) bilgisini vermektedir.

    Parametreler
    ------------
    baslangic_tarihi : %YYYY-%AA-%GG formatında başlangıç tarihi
    bitis_tarihi     : %YYYY-%AA-%GG formatında bitiş tarihi
    org              : dict formatında organizasyon EIC Kodu, Kısa Adı

    Geri Dönüş Değeri
    -----------------
    Tum organizasyon EAK değerleri
    """
    if __dogrulama.__baslangic_bitis_tarih_dogrulama(baslangic_tarihi, bitis_tarihi):
        try:
            resp = __requests.get(
                __transparency_url + "aic" + "?startDate=" + baslangic_tarihi + "&endDate=" + bitis_tarihi +
                "&organizationEIC=" + org["EIC Kodu"], headers=__headers, timeout=__param.__timeout)
            resp.raise_for_status()
            list_aic = resp.json()["body"]["aicList"]
            df_aic = __pd.DataFrame(list_aic)
            df_aic["Saat"] = df_aic["tarih"].apply(lambda h: int(h[11:13]))
            df_aic["Tarih"] = __pd.to_datetime(df_aic["tarih"].apply(lambda d: d[:10]))
            df_aic.rename(index=str, columns={"toplam": org["Kısa Adı"]}, inplace=True)
            df_aic = df_aic[["Tarih", "Saat", org["Kısa Adı"]]]
        except __ConnectionError:
            __logging.error(__param.__requestsConnectionErrorLogging, exc_info=False)
        except __Timeout:
            __logging.error(__param.__requestsTimeoutErrorLogging, exc_info=False)
        except __HTTPError as e:
            __dogrulama.__check_http_error(e.response.status_code)
        except __RequestException:
            __logging.error(__param.__request_error, exc_info=False)
        except KeyError:
            return __pd.DataFrame()
        else:
            return df_aic
