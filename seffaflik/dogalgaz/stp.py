import pandas as __pd
import datetime as __dt

from seffaflik.__ortak.__araclar import make_requests as __make_requests
from seffaflik.__ortak import __dogrulama as __dogrulama

__first_part_url = "stp/"


def gunluk_fiyat(baslangic_tarihi=__dt.datetime.today().strftime("%Y-%m-%d"),
                 bitis_tarihi=__dt.datetime.today().strftime("%Y-%m-%d")):
    """
    İlgili tarih aralığı için günlük STP fiyatlarını (GRF, GÖF, GİF, GEF, AOF) vermektedir.

    Parametreler
    ------------
    baslangic_tarihi : %YYYY-%AA-%GG formatında başlangıç tarihi (Varsayılan: bugün)
    bitis_tarihi     : %YYYY-%AA-%GG formatında bitiş tarihi (Varsayılan: bugün)

    Geri Dönüş Değeri
    -----------------
    Günlük Fiyat (₺/1000 sm3)
    """
    if __dogrulama.__baslangic_bitis_tarih_dogrulama(baslangic_tarihi, bitis_tarihi):
        try:
            particular_url = \
                __first_part_url + "daily-price" + "?startDate=" + baslangic_tarihi + "&endDate=" + bitis_tarihi
            json = __make_requests(particular_url)
            df = __pd.DataFrame(json["body"]["stpDailyPriceDtos"])
            df = df.rename(columns={"gasDay": "Gaz Günü", "contractName": "Kontrat İsmi", "intraDayPrice": "GİF",
                                    "dayAfterPrice": "GEF", "dayAheadPrice": "GÖF", "weightedAverage": "AOF",
                                    "gasReferencePrice": "GRF"})
            df["Gaz Günü"] = df["Gaz Günü"].apply(lambda s: s[:10])
        except (KeyError, TypeError):
            return __pd.DataFrame()
        else:
            return df


def gunluk_hacim(baslangic_tarihi=__dt.datetime.today().strftime("%Y-%m-%d"),
                 bitis_tarihi=__dt.datetime.today().strftime("%Y-%m-%d")):
    """
    İlgili tarih aralığı için günlük STP GRF Eşleşme Miktarı (GRFEM), Gün Öncesi Eşleşme Miktarı (GÖEM),
    Gün İçi Eşleşme Miktarı (GİEM), Gün Ertesi Eşleşme Miktarı (GEEM) bilgisini vermektedir.

    Parametreler
    ------------
    baslangic_tarihi : %YYYY-%AA-%GG formatında başlangıç tarihi (Varsayılan: bugün)
    bitis_tarihi     : %YYYY-%AA-%GG formatında bitiş tarihi (Varsayılan: bugün)

    Geri Dönüş Değeri
    -----------------
    Eşleşme Miktarı (x1000 sm3)
    """
    if __dogrulama.__baslangic_bitis_tarih_dogrulama(baslangic_tarihi, bitis_tarihi):
        try:
            particular_url = \
                __first_part_url + "matching-quantity" + "?startDate=" + baslangic_tarihi + "&endDate=" + bitis_tarihi
            json = __make_requests(particular_url)
            df = __pd.DataFrame(json["body"]["matchingDtos"])
            df = df.rename(
                columns={"gasDay": "Gaz Günü", "contractName": "Kontrat İsmi", "intraDayMatchingQuantity": "GİEM",
                         "dayAfterMatchingQuantity": "GEEM", "dayAheadMatchingQuantity": "GÖEM",
                         "gasReferenceMatchingQuantity": "GRFEM", "weeklyMatchingQuantity": "HEM"})
            df["Gaz Günü"] = df["Gaz Günü"].apply(lambda s: s[:10])
            df = df[["Gaz Günü", "Kontrat İsmi", "GİEM", "GEEM", "GÖEM", "GRFEM", "HEM"]]
        except (KeyError, TypeError):
            return __pd.DataFrame()
        else:
            return df


def gunluk_islem_hacmi(baslangic_tarihi=__dt.datetime.today().strftime("%Y-%m-%d"),
                        bitis_tarihi=__dt.datetime.today().strftime("%Y-%m-%d")):
    """
    İlgili tarih aralığı için günlük STP GRF İşlem Hacmi (GRFİH), Gün Öncesi İşlem Hacmi (GÖİH),
    Gün İçi İşlem Hacmi (GİİH), Gün Ertesi İşlem Hacmi (GEİH) bilgisini vermektedir.

    Parametreler
    ------------
    baslangic_tarihi : %YYYY-%AA-%GG formatında başlangıç tarihi (Varsayılan: bugün)
    bitis_tarihi     : %YYYY-%AA-%GG formatında bitiş tarihi (Varsayılan: bugün)

    Geri Dönüş Değeri
    -----------------
    İşlem Hacmi (TL)
    """
    if __dogrulama.__baslangic_bitis_tarih_dogrulama(baslangic_tarihi, bitis_tarihi):
        try:
            particular_url = \
                __first_part_url + "trade-value" + "?startDate=" + baslangic_tarihi + "&endDate=" + bitis_tarihi
            json = __make_requests(particular_url)
            df = __pd.DataFrame(json["body"]["tradeValues"])
            df = df.rename(
                columns={"gasDay": "Gaz Günü", "contractName": "Kontrat İsmi", "intraDayTradeValue": "GİİH",
                         "dayAfterTradeValue": "GEİH", "dayAheadTradeValue": "GÖİH",
                         "gasReferenceTradeValue": "GRFİH"})
            df["Gaz Günü"] = df["Gaz Günü"].apply(lambda s: s[:10])
            df = df[["Gaz Günü", "Kontrat İsmi", "GİİH", "GEİH", "GÖİH", "GRFİH"]]
        except (KeyError, TypeError):
            return __pd.DataFrame()
        else:
            return df


def haftalik_fiyat(baslangic_tarihi=__dt.datetime.today().strftime("%Y-%m-%d"),
                   bitis_tarihi=__dt.datetime.today().strftime("%Y-%m-%d")):
    """
    İlgili tarih aralığı için günlük STP fiyatlarını (GRF, GÖF, GİF, GEF, AOF) vermektedir.

    Parametreler
    ------------
    baslangic_tarihi : %YYYY-%AA-%GG formatında başlangıç tarihi (Varsayılan: bugün)
    bitis_tarihi     : %YYYY-%AA-%GG formatında bitiş tarihi (Varsayılan: bugün)

    Geri Dönüş Değeri
    -----------------
    Günlük Fiyat (₺/1000 sm3)
    """
    if __dogrulama.__baslangic_bitis_tarih_dogrulama(baslangic_tarihi, bitis_tarihi):
        try:
            particular_url = __first_part_url + "stp-weekly-reference-price" + "?startDate=" + baslangic_tarihi + \
                             "&endDate=" + bitis_tarihi
            json = __make_requests(particular_url)
            df = __pd.DataFrame(json["body"]["weeklyRefPriceList"])
            df = df.rename(columns={"week": "Hafta", "weekdayPrice": "HI", "weekendPrice": "HS", "weekTotalPrice": "HT",
                                    "weeklyRefPrice": "HRF"})
        except (KeyError, TypeError):
            return __pd.DataFrame()
        else:
            return df


def haftalik_hacim(baslangic_tarihi=__dt.datetime.today().strftime("%Y-%m-%d"),
                   bitis_tarihi=__dt.datetime.today().strftime("%Y-%m-%d")):
    """
    İlgili tarih aralığı için haftalık STP Haftalık Eşleşme Miktarı bilgisini vermektedir.

    Parametreler
    ------------
    baslangic_tarihi : %YYYY-%AA-%GG formatında başlangıç tarihi (Varsayılan: bugün)
    bitis_tarihi     : %YYYY-%AA-%GG formatında bitiş tarihi (Varsayılan: bugün)

    Geri Dönüş Değeri
    -----------------
    Eşleşme Miktarı (x1000 sm3)
    """
    if __dogrulama.__baslangic_bitis_tarih_dogrulama(baslangic_tarihi, bitis_tarihi):
        try:
            particular_url = __first_part_url + "matching-quantity/stp-weekly-matching-quantity" + "?startDate=" + \
                             baslangic_tarihi + "&endDate=" + bitis_tarihi
            json = __make_requests(particular_url)
            df = __pd.DataFrame(json["body"]["stpWeeklyMatchList"])
            df = df.rename(
                columns={"week": "Hafta", "hiem": "HİEM", "hsem": "HSEM", "htem": "HTEM", "emTotal": "TOPLAM"})
        except (KeyError, TypeError):
            return __pd.DataFrame()
        else:
            return df


def haftalik_islem_hacmi(baslangic_tarihi=__dt.datetime.today().strftime("%Y-%m-%d"),
                          bitis_tarihi=__dt.datetime.today().strftime("%Y-%m-%d")):
    """
    İlgili tarih aralığı için haftalık STP Haftalık İşlem Hacmi bilgisini vermektedir.

    Parametreler
    ------------
    baslangic_tarihi : %YYYY-%AA-%GG formatında başlangıç tarihi (Varsayılan: bugün)
    bitis_tarihi     : %YYYY-%AA-%GG formatında bitiş tarihi (Varsayılan: bugün)

    Geri Dönüş Değeri
    -----------------
    İşlem Hacmi (TL)
    """
    if __dogrulama.__baslangic_bitis_tarih_dogrulama(baslangic_tarihi, bitis_tarihi):
        try:
            particular_url = __first_part_url + "volume/stp-weekly-volume" + "?startDate=" + \
                             baslangic_tarihi + "&endDate=" + bitis_tarihi
            json = __make_requests(particular_url)
            df = __pd.DataFrame(json["body"]["stpWeeklyMatchList"])
            df = df.rename(
                columns={"week": "Hafta", "hiih": "HİİH", "hsih": "HSİH", "htih": "HTİH", "ihTotal": "TOPLAM"})
        except (KeyError, TypeError):
            return __pd.DataFrame()
        else:
            return df


def dengeleme_gazi_fiyati(baslangic_tarihi=__dt.datetime.today().strftime("%Y-%m-%d"),
                          bitis_tarihi=__dt.datetime.today().strftime("%Y-%m-%d")):
    """
    İlgili tarih aralığı için günlük Dengeleme Gazı Fiyatlarını (DGF) vermektedir.

    Parametreler
    ------------
    baslangic_tarihi : %YYYY-%AA-%GG formatında başlangıç tarihi (Varsayılan: bugün)
    bitis_tarihi     : %YYYY-%AA-%GG formatında bitiş tarihi (Varsayılan: bugün)

    Geri Dönüş Değeri
    -----------------
    Dengeleme Gazı Fiyatları (DGF) (₺/1000 sm3)
    """
    if __dogrulama.__baslangic_bitis_tarih_dogrulama(baslangic_tarihi, bitis_tarihi):
        try:
            particular_url = \
                __first_part_url + "balancing-gas-price" + "?startDate=" + baslangic_tarihi + "&endDate=" + bitis_tarihi
            json = __make_requests(particular_url)
            df = __pd.DataFrame(json["body"]["prices"])
            df = df.rename(
                columns={"gasDay": "Gaz Günü", "additionalBalancingPurchase": "İDAF", "additionalBalancingSale": "İDSF",
                         "balancingGasPurchase": "DGAF", "balancingGasSale": "DGSF", "finalAbp": "İDAF (Kesinleşmiş)",
                         "finalAbs": "İDSF (Kesinleşmiş)", "finalBgp": "DGAF (Kesinleşmiş)",
                         "finalBgs": "DGSF (Kesinleşmiş)"})
            df["Gaz Günü"] = df["Gaz Günü"].apply(lambda s: s[:10])
        except (KeyError, TypeError):
            return __pd.DataFrame()
        else:
            return df
