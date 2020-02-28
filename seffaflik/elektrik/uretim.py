import pandas as __pd
import datetime as __dt
from multiprocessing import Pool as __Pool
import multiprocessing as __mp
from functools import reduce as __red

from seffaflik.__ortak.__araclar import make_requests as __make_requests
from seffaflik.__ortak import __dogrulama as __dogrulama
from seffaflik.elektrik import santraller as __santraller

__first_part_url = "production/"


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
        particular_url = __first_part_url + "dpp-organization"
        json = __make_requests(particular_url)
        df = __pd.DataFrame(json["body"]["organizations"])
        df.rename(index=str,
                  columns={"organizationId": "Id", "organizationName": "Adı",
                           "organizationETSOCode": "EIC Kodu", "organizationShortName": "Kısa Adı",
                           "organizationStatus": "Durum"},
                  inplace=True)
        df = df[["Id", "Adı", "EIC Kodu", "Kısa Adı", "Durum"]]
    except (KeyError, TypeError):
        return __pd.DataFrame()
    else:
        return df


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
            particular_url = __first_part_url + "dpp-injection-unit-name?organizationEIC=" + eic
            json = __make_requests(particular_url)
            df_unit = __pd.DataFrame(json["body"]["injectionUnitNames"])
            df_unit.rename(index=str, columns={"id": "Id", "name": "Adı", "eic": "EIC Kodu"}, inplace=True)
            df_unit = df_unit[["Id", "Adı", "EIC Kodu"]]
        except (KeyError, TypeError):
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
            particular_url = __first_part_url + "dpp" + "?startDate=" + baslangic_tarihi + "&endDate=" + bitis_tarihi \
                             + "&organizationEIC=" + organizasyon_eic + "&uevcbEIC=" + uevcb_eic
            json = __make_requests(particular_url)
            df = __pd.DataFrame(json["body"]["dppList"])
            df["Saat"] = df["tarih"].apply(lambda h: int(h[11:13]))
            df["Tarih"] = __pd.to_datetime(df["tarih"].apply(lambda d: d[:10]))
            df.rename(index=str,
                      columns={"akarsu": "Akarsu", "barajli": "Barajlı", "biokutle": "Biyokütle", "diger": "Diğer",
                               "dogalgaz": "Doğalgaz", "fuelOil": "Fuel Oil", "ithalKomur": "İthal Kömür",
                               "jeotermal": "Jeo Termal", "linyit": "Linyit", "nafta": "Nafta",
                               "ruzgar": "Rüzgar", "tasKomur": "Taş Kömür", "toplam": "Toplam"}, inplace=True)
            df = df[["Tarih", "Saat", "Doğalgaz", "Barajlı", "Linyit", "Akarsu", "İthal Kömür", "Rüzgar",
                     "Fuel Oil", "Jeo Termal", "Taş Kömür", "Biyokütle", "Nafta", "Diğer", "Toplam"]]
        except (KeyError, TypeError):
            return __pd.DataFrame()
        else:
            return df


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


def tum_uevcb_kgup(baslangic_tarihi=__dt.datetime.today().strftime("%Y-%m-%d"),
                   bitis_tarihi=__dt.datetime.today().strftime("%Y-%m-%d")):
    """
    İlgili tarih aralığı için Kesinleşmiş Gün Öncesi Üretim Planı (KGÜP) girebilecek olan tüm organizasyonların
    uzlaştırmaya esas veriş-çekiş birimlerinin saatlik KGUP bilgilerini vermektedir.

    Parametreler
    ------------
    baslangic_tarihi : %YYYY-%AA-%GG formatında başlangıç tarihi (Varsayılan: bugün)
    bitis_tarihi     : %YYYY-%AA-%GG formatında bitiş tarihi (Varsayılan: bugün)

    Geri Dönüş Değeri
    -----------------
    KGÜP Girebilen Organizasyonların UEVCB KGUP Değerleri (Tarih, Saat, KGUP)
    """
    if __dogrulama.__baslangic_bitis_tarih_dogrulama(baslangic_tarihi, bitis_tarihi):
        org_uevcb = tum_organizasyonlar_veris_cekis_birimleri()
        list_org_uevcb = org_uevcb[["Org EIC Kodu", "UEVÇB EIC Kodu", "UEVÇB Adı"]].to_dict("records")
        list_org_uevcb_len = len(list_org_uevcb)
        list_date_org_uevcb_eic = list(
            zip([baslangic_tarihi] * list_org_uevcb_len, [bitis_tarihi] * list_org_uevcb_len, list_org_uevcb))
        list_date_org_uevcb_eic = list(map(list, list_date_org_uevcb_eic))
        with __Pool(__mp.cpu_count()) as p:
            list_df_unit = p.starmap(__kgup_uevcb, list_date_org_uevcb_eic, chunksize=1)
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
            particular_url = __first_part_url + "aic" + "?startDate=" + baslangic_tarihi + "&endDate=" + bitis_tarihi \
                             + "&organizationEIC=" + organizasyon_eic + "&uevcbEIC=" + uevcb_eic
            json = __make_requests(particular_url)
            df = __pd.DataFrame(json["body"]["aicList"])
            df["Saat"] = df["tarih"].apply(lambda h: int(h[11:13]))
            df["Tarih"] = __pd.to_datetime(df["tarih"].apply(lambda d: d[:10]))
            df.rename(index=str,
                      columns={"akarsu": "Akarsu", "barajli": "Barajlı", "biyokutle": "Biokütle", "diger": "Diğer",
                               "dogalgaz": "Doğalgaz", "fuelOil": "Fuel Oil", "ithalKomur": "İthal Kömür",
                               "jeotermal": "Jeo Termal", "linyit": "Linyit", "nafta": "Nafta",
                               "ruzgar": "Rüzgar", "tasKomur": "Taş Kömür", "toplam": "Toplam"}, inplace=True)
            df = df[["Tarih", "Saat", "Doğalgaz", "Barajlı", "Linyit", "Akarsu", "İthal Kömür", "Rüzgar",
                     "Fuel Oil", "Jeo Termal", "Taş Kömür", "Biyokütle", "Nafta", "Diğer", "Toplam"]]
        except (KeyError, TypeError):
            return __pd.DataFrame()
        else:
            return df


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


def tum_uevcb_eak(baslangic_tarihi=__dt.datetime.today().strftime("%Y-%m-%d"),
                  bitis_tarihi=__dt.datetime.today().strftime("%Y-%m-%d")):
    """
    İlgili tarih aralığı için Emre Amade Kapasite (EAK) girebilecek olan tüm organizasyonların uzlaştırmaya esas
    veriş-çekiş birimlerinin saatlik KGUP bilgilerini vermektedir.

    Parametreler
    ------------
    baslangic_tarihi : %YYYY-%AA-%GG formatında başlangıç tarihi (Varsayılan: bugün)
    bitis_tarihi     : %YYYY-%AA-%GG formatında bitiş tarihi (Varsayılan: bugün)

    Geri Dönüş Değeri
    -----------------
    KGÜP Girebilen Organizasyonların UEVCB KGUP Değerleri (Tarih, Saat, KGUP)
    """
    if __dogrulama.__baslangic_bitis_tarih_dogrulama(baslangic_tarihi, bitis_tarihi):
        org_uevcb = tum_organizasyonlar_veris_cekis_birimleri()
        list_org_uevcb = org_uevcb[["Org EIC Kodu", "UEVÇB EIC Kodu", "UEVÇB Adı"]].to_dict("records")
        list_org_uevcb_len = len(list_org_uevcb)
        list_date_org_uevcb_eic = list(
            zip([baslangic_tarihi] * list_org_uevcb_len, [bitis_tarihi] * list_org_uevcb_len, list_org_uevcb))
        list_date_org_uevcb_eic = list(map(list, list_date_org_uevcb_eic))
        with __Pool(__mp.cpu_count()) as p:
            list_df_unit = p.starmap(__eak_uevcb, list_date_org_uevcb_eic, chunksize=1)
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
            particular_url = __first_part_url + "sbfgp" + "?startDate=" + baslangic_tarihi + "&endDate=" + \
                             bitis_tarihi + "&organizationId=" + organizasyon_id + "&uevcbId=" + uevcb_id
            json = __make_requests(particular_url)
            df = __pd.DataFrame(json["body"]["dppList"])
            df["Saat"] = df["tarih"].apply(lambda h: int(h[11:13]))
            df["Tarih"] = __pd.to_datetime(df["tarih"].apply(lambda d: d[:10]))
            df.rename(index=str,
                      columns={"akarsu": "Akarsu", "barajli": "Barajlı", "biokutle": "Biyokütle", "diger": "Diğer",
                               "dogalgaz": "Doğalgaz", "fuelOil": "Fuel Oil", "ithalKomur": "İthal Kömür",
                               "jeotermal": "Jeo Termal", "linyit": "Linyit", "nafta": "Nafta",
                               "ruzgar": "Rüzgar", "tasKomur": "Taş Kömür", "toplam": "Toplam"}, inplace=True)
            df = df[["Tarih", "Saat", "Doğalgaz", "Barajlı", "Linyit", "Akarsu", "İthal Kömür", "Rüzgar",
                     "Fuel Oil", "Jeo Termal", "Taş Kömür", "Biyokütle", "Nafta", "Diğer", "Toplam"]]
        except (KeyError, TypeError):
            return __pd.DataFrame()
        else:
            return df


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
            particular_url = __first_part_url + "ssv-categorized" + "?startDate=" + baslangic_tarihi + "&endDate=" + \
                             bitis_tarihi
            json = __make_requests(particular_url)
            df = __pd.DataFrame(json["body"]["ssvList"])
            df["Saat"] = df["date"].apply(lambda h: int(h[11:13]))
            df["Tarih"] = __pd.to_datetime(df["date"].apply(lambda d: d[:10]))
            df.rename(index=str,
                      columns={"asphaltite": "Asfaltit Kömür", "river": "Akarsu", "dam": "Barajlı",
                               "biomass": "Biyokütle", "naturalGas": "Doğalgaz", "fueloil": "Fuel Oil",
                               "importedCoal": "İthal Kömür", "geothermal": "Jeo Termal", "lignite": "Linyit",
                               "naphtha": "Nafta", "lng": "LNG", "wind": "Rüzgar", "stonecoal": "Taş Kömür",
                               "international": "Uluslararası", "total": "Toplam", "other": "Diğer"},
                      inplace=True)
            df = df[
                ["Tarih", "Saat", "Doğalgaz", "Barajlı", "Linyit", "Akarsu", "İthal Kömür", "Rüzgar",
                 "Fuel Oil", "Jeo Termal", "Asfaltit Kömür", "Taş Kömür", "Biyokütle", "Nafta", "LNG", "Uluslararası",
                 "Diğer", "Toplam"]]
        except (KeyError, TypeError):
            return __pd.DataFrame()
        else:
            return df


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


def tum_santraller_gerceklesen(baslangic_tarihi=__dt.datetime.today().strftime("%Y-%m-%d"),
                               bitis_tarihi=__dt.datetime.today().strftime("%Y-%m-%d")):
    """
    İlgili tarih aralığı için tüm lisanslı santrallerin gerçek zamanlı üretim bilgisini vermektedir.

    Parametreler
    ------------
    baslangic_tarihi : %YYYY-%AA-%GG formatında başlangıç tarihi (Varsayılan: bugün)
    bitis_tarihi     : %YYYY-%AA-%GG formatında bitiş tarihi (Varsayılan: bugün)

    Geri Dönüş Değeri
    -----------------
    Tüm lisanslı santrallerin gerçek zamanlı üretim Değerleri (Tarih, Saat, Santral Üretimleri)
    """
    if __dogrulama.__baslangic_bitis_tarih_dogrulama(baslangic_tarihi, bitis_tarihi):
        sant = __santraller.gercek_zamanli_uretim_yapan_santraller()
        list_sant = sant[["Id", "Kısa Adı"]].to_dict("records")
        list_sant_len = len(list_sant)
        list_sant = list(
            zip([baslangic_tarihi] * list_sant_len, [bitis_tarihi] * list_sant_len, list_sant))
        list_sant = list(map(list, list_sant))
        with __Pool(__mp.cpu_count()) as p:
            list_df_unit = p.starmap(__gerceklesen_santral, list_sant, chunksize=1)
        list_df_unit = list(filter(lambda x: len(x) > 0, list_df_unit))
        df_unit = __red(lambda left, right: __pd.merge(left, right, how="outer", on=["Tarih", "Saat"], sort=True),
                        list_df_unit)
        return df_unit


def gddk(baslangic_tarihi=__dt.datetime.today().strftime("%Y-%m-%d"),
         bitis_tarihi=__dt.datetime.today().strftime("%Y-%m-%d")):
    """
    İlgili tarih aralığı için geriye dönük düzeltme kalemine (GDDK) ait değerleri vermektedir.

    Parametreler
    ------------
    baslangic_tarihi : %YYYY-%AA-%GG formatında başlangıç tarihi (Varsayılan: bugün)
    bitis_tarihi     : %YYYY-%AA-%GG formatında bitiş tarihi (Varsayılan: bugün)

    Geri Dönüş Değeri
    -----------------
    Dönemlik GDDK (₺)
    """
    if __dogrulama.__baslangic_bitis_tarih_dogrulama(baslangic_tarihi, bitis_tarihi):
        try:
            particular_url = \
                __first_part_url + "gddk-amount" + "?startDate=" + baslangic_tarihi + "&endDate=" + bitis_tarihi
            json = __make_requests(particular_url)
            df = __pd.DataFrame(json["body"]["gddkAmountList"])
            df["Dönem"] = df["id"].apply(
                lambda x: str(__pd.to_datetime(x["date"][:10]).month_name(locale='tr_TR.UTF-8')) + "-" + str(
                    __pd.to_datetime(x["date"][:10]).year))
            df["Versiyon"] = df["id"].apply(
                lambda x: str(__pd.to_datetime(x["version"][:10]).month_name(locale='tr_TR.UTF-8')) + "-" + str(
                    __pd.to_datetime(x["version"][:10]).year))
            df.rename(index=str,
                      columns={"gddkCreditAmount": "Alacak GDDK Tutarı (TL)", "gddkDebtAmount": "Borç GDDK Tutarı (TL)",
                               "gddkNetAmount": "Net GDDK Tutarı (TL)"}, inplace=True)
            df = df[["Dönem", "Versiyon", "Alacak GDDK Tutarı (TL)", "Borç GDDK Tutarı (TL)", "Net GDDK Tutarı (TL)"]]
        except (KeyError, TypeError):
            return __pd.DataFrame()
        else:
            return df


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
        particular_url = __first_part_url + "real-time-generation" + "?startDate=" + baslangic_tarihi + "&endDate=" \
                         + bitis_tarihi
        json = __make_requests(particular_url)
        df = __pd.DataFrame(json["body"]["hourlyGenerations"])
        df["Saat"] = df["date"].apply(lambda h: int(h[11:13]))
        df["Tarih"] = __pd.to_datetime(df["date"].apply(lambda d: d[:10]))
        df.rename(index=str,
                  columns={"asphaltiteCoal": "Asfaltit Kömür", "river": "Akarsu", "dammedHydro": "Barajlı",
                           "biomass": "Biyokütle", "sun": "Güneş", "naturalGas": "Doğalgaz",
                           "fueloil": "Fuel Oil", "importCoal": "İthal Kömür", "geothermal": "Jeo Termal",
                           "lignite": "Linyit", "naphta": "Nafta", "lng": "LNG", "wind": "Rüzgar",
                           "blackCoal": "Taş Kömür", "importExport": "Uluslararası", "total": "Toplam"},
                  inplace=True)
        df = df[
            ["Tarih", "Saat", "Doğalgaz", "Barajlı", "Linyit", "Akarsu", "İthal Kömür", "Rüzgar", "Güneş",
             "Fuel Oil", "Jeo Termal", "Asfaltit Kömür", "Taş Kömür", "Biyokütle", "Nafta", "LNG", "Uluslararası",
             "Toplam"]]
    except (KeyError, TypeError):
        return __pd.DataFrame()
    else:
        return df


def __santral_bazli_gerceklesen(baslangic_tarihi, bitis_tarihi, santral_id):
    """
    İlgili tarih aralığı ve santral için gerçek zamanlı üretim bilgisini vermektedir.

    Parametreler
    ------------
    baslangic_tarihi : %YYYY-%AA-%GG formatında başlangıç tarihi
    bitis_tarihi     : %YYYY-%AA-%GG formatında bitiş tarihi
    santral_id       : metin yada tam sayı formatında santral id

    Geri Dönüş Değeri
    -----------------
    Santral Bazlı Gerçek Zamanlı Üretim("Tarih", "Saat", "Doğalgaz", "Barajlı", "Linyit", "Akarsu", "İthal Kömür",
                "Rüzgar", "Güneş", "Fuel Oil", "Jeo Termal", "Asfaltit Kömür", "Taş Kömür", "Biokütle", "Nafta", "LNG",
                "Uluslararası", "Toplam")
    """
    try:
        particular_url = __first_part_url + "real-time-generation_with_powerplant" + "?startDate=" + \
                         baslangic_tarihi + "&endDate=" + bitis_tarihi + "&powerPlantId=" + str(santral_id)
        json = __make_requests(particular_url)
        df = __pd.DataFrame(json["body"]["hourlyGenerations"])
        df["Saat"] = df["date"].apply(lambda h: int(h[11:13]))
        df["Tarih"] = __pd.to_datetime(df["date"].apply(lambda d: d[:10]))
        df.rename(index=str,
                  columns={"asphaltiteCoal": "Asfaltit Kömür", "river": "Akarsu", "dammedHydro": "Barajlı",
                           "biomass": "Biyokütle", "sun": "Güneş", "naturalGas": "Doğalgaz",
                           "fueloil": "Fuel Oil", "importCoal": "İthal Kömür", "geothermal": "Jeo Termal",
                           "lignite": "Linyit", "naphta": "Nafta", "lng": "LNG", "wind": "Rüzgar",
                           "blackCoal": "Taş Kömür", "importExport": "Uluslararası", "total": "Toplam"},
                  inplace=True)
        df = df[
            ["Tarih", "Saat", "Doğalgaz", "Barajlı", "Linyit", "Akarsu", "İthal Kömür", "Rüzgar", "Güneş",
             "Fuel Oil", "Jeo Termal", "Asfaltit Kömür", "Taş Kömür", "Biyokütle", "Nafta", "LNG", "Uluslararası",
             "Toplam"]]
    except (KeyError, TypeError):
        return __pd.DataFrame()
    else:
        return df


def __gerceklesen_santral(baslangic_tarihi, bitis_tarihi, santral):
    """
    İlgili tarih aralığı ve santral için gerçek zamanlı üretim bilgisini vermektedir.

    Parametreler
    ------------
    baslangic_tarihi : %YYYY-%AA-%GG formatında başlangıç tarihi
    bitis_tarihi     : %YYYY-%AA-%GG formatında bitiş tarihi
    santral_id       : metin yada tam sayı formatında santral id

    Geri Dönüş Değeri
    -----------------
    Santral Bazlı Gerçek Zamanlı Üretim("Tarih", "Saat", "Toplam")
    """
    try:
        particular_url = __first_part_url + "real-time-generation_with_powerplant" + "?startDate=" + \
                         baslangic_tarihi + "&endDate=" + bitis_tarihi + "&powerPlantId=" + str(santral["Id"])
        json = __make_requests(particular_url)
        df = __pd.DataFrame(json["body"]["hourlyGenerations"])
        df["Saat"] = df["date"].apply(lambda h: int(h[11:13]))
        df["Tarih"] = __pd.to_datetime(df["date"].apply(lambda d: d[:10]))
        df.rename(index=str, columns={"total": santral["Kısa Adı"]}, inplace=True)
        df = df[["Tarih", "Saat", santral["Kısa Adı"]]]
    except (KeyError, TypeError):
        return __pd.DataFrame()
    else:
        return df


def __organizasyon_cekis_birimleri(org):
    """
    İlgili eic değeri için Kesinleşmiş Gün Öncesi Üretim Planı (KGÜP) girebilecek olan organizasyonun uzlaştırmaya
    esas veriş-çekiş birim (UEVÇB) bilgilerini ve organizasyona dair bilgileri vermektedir.

    Parametreler
    ------------
    org : dict formatında organizasyon Id, Adı, EIC Kodu, Kısa Adı, Durum

    Geri Dönüş Değeri
    -----------------
    KGÜP Girebilen Organizasyon Bilgileri(Org Id, Org Adı, Org EIC Kodu, Org Kısa Adı,Org Durum, UEVÇB Id, UEVÇB Adı,
    UEVÇB EIC Kodu)
    """
    try:
        particular_url = __first_part_url + "dpp-injection-unit-name?organizationEIC=" + org["EIC Kodu"]
        json = __make_requests(particular_url)
        df = __pd.DataFrame(json["body"]["injectionUnitNames"])
        df["Org Id"] = org["Id"]
        df["Org Adı"] = org["Adı"]
        df["Org EIC Kodu"] = org["EIC Kodu"]
        df["Org Kısa Adı"] = org["Kısa Adı"]
        df["Org Durum"] = org["Durum"]
        df.rename(index=str,
                  columns={"id": "UEVÇB Id", "name": "UEVÇB Adı", "eic": "UEVÇB EIC Kodu"},
                  inplace=True)
        df = df[["Org Id", "Org Adı", "Org EIC Kodu", "Org Kısa Adı",
                 "Org Durum", "UEVÇB Id", "UEVÇB Adı", "UEVÇB EIC Kodu"]]
    except (KeyError, TypeError):
        return __pd.DataFrame()
    else:
        return df


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
    Organizasyonel KGUP değerleri
    """
    if __dogrulama.__baslangic_bitis_tarih_dogrulama(baslangic_tarihi, bitis_tarihi):
        try:
            particular_url = __first_part_url + "dpp" + "?startDate=" + baslangic_tarihi + "&endDate=" + bitis_tarihi \
                             + "&organizationEIC=" + org["EIC Kodu"]
            json = __make_requests(particular_url)
            df = __pd.DataFrame(json["body"]["dppList"])
            df["Saat"] = df["tarih"].apply(lambda h: int(h[11:13]))
            df["Tarih"] = __pd.to_datetime(df["tarih"].apply(lambda d: d[:10]))
            df.rename(index=str, columns={"toplam": org["Kısa Adı"]}, inplace=True)
            df = df[["Tarih", "Saat", org["Kısa Adı"]]]
        except (KeyError, TypeError):
            return __pd.DataFrame()
        else:
            return df


def __kgup_uevcb(baslangic_tarihi, bitis_tarihi, org_uevcb):
    """
    İlgili tarih aralığı ve uzlaştırmaya esas veriş çekiş birimi için kesinleşmiş günlük üretim prgoramı (KGÜP)
    bilgisini vermektedir.

    Parametreler
    ------------
    baslangic_tarihi : %YYYY-%AA-%GG formatında başlangıç tarihi
    bitis_tarihi     : %YYYY-%AA-%GG formatında bitiş tarihi
    org              : dict formatında organizasyon EIC Kodu, Kısa Adı

    Geri Dönüş Değeri
    -----------------
    UEVCB KGUP değerleri
    """
    if __dogrulama.__baslangic_bitis_tarih_dogrulama(baslangic_tarihi, bitis_tarihi):
        try:
            particular_url = __first_part_url + "dpp" + "?startDate=" + baslangic_tarihi + "&endDate=" + bitis_tarihi \
                             + "&organizationEIC=" + org_uevcb["Org EIC Kodu"] + "&uevcbEIC=" + org_uevcb[
                                 "UEVÇB EIC Kodu"]
            json = __make_requests(particular_url)
            df = __pd.DataFrame(json["body"]["dppList"])
            df["Saat"] = df["tarih"].apply(lambda h: int(h[11:13]))
            df["Tarih"] = __pd.to_datetime(df["tarih"].apply(lambda d: d[:10]))
            df.rename(index=str, columns={"toplam": org_uevcb["UEVÇB Adı"]}, inplace=True)
            df = df[["Tarih", "Saat", org_uevcb["UEVÇB Adı"]]]
        except (KeyError, TypeError):
            return __pd.DataFrame()
        else:
            return df


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
    ORganizasyonel EAK değerleri
    """
    if __dogrulama.__baslangic_bitis_tarih_dogrulama(baslangic_tarihi, bitis_tarihi):
        try:
            particular_url = __first_part_url + "aic" + "?startDate=" + baslangic_tarihi + "&endDate=" + bitis_tarihi \
                             + "&organizationEIC=" + org["EIC Kodu"]
            json = __make_requests(particular_url)
            df = __pd.DataFrame(json["body"]["aicList"])
            df["Saat"] = df["tarih"].apply(lambda h: int(h[11:13]))
            df["Tarih"] = __pd.to_datetime(df["tarih"].apply(lambda d: d[:10]))
            df.rename(index=str, columns={"toplam": org["Kısa Adı"]}, inplace=True)
            df = df[["Tarih", "Saat", org["Kısa Adı"]]]
        except (KeyError, TypeError):
            return __pd.DataFrame()
        else:
            return df


def __eak_uevcb(baslangic_tarihi, bitis_tarihi, org_uevcb):
    """
    İlgili tarih aralığı ve uzlaştırmaya esas veriş çekiş birimi için emre amade kapasite (EAK) bilgisini vermektedir.

    Parametreler
    ------------
    baslangic_tarihi : %YYYY-%AA-%GG formatında başlangıç tarihi
    bitis_tarihi     : %YYYY-%AA-%GG formatında bitiş tarihi
    org              : dict formatında organizasyon EIC Kodu, Kısa Adı

    Geri Dönüş Değeri
    -----------------
    UEVCB EAK değerleri
    """
    if __dogrulama.__baslangic_bitis_tarih_dogrulama(baslangic_tarihi, bitis_tarihi):
        try:
            particular_url = __first_part_url + "aic" + "?startDate=" + baslangic_tarihi + "&endDate=" + bitis_tarihi \
                             + "&organizationEIC=" + org_uevcb["Org EIC Kodu"] + "&uevcbEIC=" + org_uevcb[
                                 "UEVÇB EIC Kodu"]
            json = __make_requests(particular_url)
            df = __pd.DataFrame(json["body"]["aicList"])
            df["Saat"] = df["tarih"].apply(lambda h: int(h[11:13]))
            df["Tarih"] = __pd.to_datetime(df["tarih"].apply(lambda d: d[:10]))
            df.rename(index=str, columns={"toplam": org_uevcb["UEVÇB Adı"]}, inplace=True)
            df = df[["Tarih", "Saat", org_uevcb["UEVÇB Adı"]]]
        except (KeyError, TypeError):
            return __pd.DataFrame()
        else:
            return df
