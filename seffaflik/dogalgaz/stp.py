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
            df["Gaz Günü"] = df["Gaz Günü"].apply(lambda s: __pd.to_datetime(s[:10]))
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
            df["Gaz Günü"] = df["Gaz Günü"].apply(lambda s: __pd.to_datetime(s[:10]))
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
            df["Gaz Günü"] = df["Gaz Günü"].apply(lambda s: __pd.to_datetime(s[:10]))
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
            df["Gaz Günü"] = df["Gaz Günü"].apply(lambda s: __pd.to_datetime(s[:10]))
        except (KeyError, TypeError):
            return __pd.DataFrame()
        else:
            return df


def ilave_dengeleyici_1_kodlu_islemler(baslangic_tarihi=__dt.datetime.today().strftime("%Y-%m-%d"),
                                       bitis_tarihi=__dt.datetime.today().strftime("%Y-%m-%d")):
    """
    İlgili tarih aralığı için ilave dengeleyici 1 kodlu işlemler listesini vermektedir.

    Parametreler
    ------------
    baslangic_tarihi : %YYYY-%AA-%GG formatında başlangıç tarihi (Varsayılan: bugün)
    bitis_tarihi     : %YYYY-%AA-%GG formatında bitiş tarihi (Varsayılan: bugün)

    Geri Dönüş Değeri
    -----------------
    İşlemler (Fiyat: ₺/1000 sm3, Miktar: x1000 sm3)
    """
    if __dogrulama.__baslangic_bitis_tarih_dogrulama(baslangic_tarihi, bitis_tarihi):
        try:
            particular_url = \
                __first_part_url + "greencode-operation" + "?startDate=" + baslangic_tarihi + "&endDate=" + bitis_tarihi
            json = __make_requests(particular_url)
            df = __pd.DataFrame(json["body"]["operations"])
            df = df.rename(
                columns={"gasDay": "Etki Ettiği Gaz Günü", "contractGasDay": "Kontrat Gaz Günü",
                         "transactionDate": "İşlem Tarihi", "contractName": "Kontrat İsmi", "amount": "Miktar",
                         "weightedAverage": "AOF"})
            df["Etki Ettiği Gaz Günü"] = df["Etki Ettiği Gaz Günü"].apply(lambda s: __pd.to_datetime(s[:10]))
            df["Kontrat Gaz Günü"] = df["Kontrat Gaz Günü"].apply(lambda s: __pd.to_datetime(s[:10]))
            df["İşlem Tarihi"] = df["İşlem Tarihi"].apply(lambda s: __pd.to_datetime(s[:10]))
        except (KeyError, TypeError):
            return __pd.DataFrame()
        else:
            return df


def ilave_dengeleyici_2_kodlu_islemler(baslangic_tarihi=__dt.datetime.today().strftime("%Y-%m-%d"),
                                       bitis_tarihi=__dt.datetime.today().strftime("%Y-%m-%d")):
    """
    İlgili tarih aralığı için ilave dengeleyici 2 kodlu işlemler listesini vermektedir.

    Parametreler
    ------------
    baslangic_tarihi : %YYYY-%AA-%GG formatında başlangıç tarihi (Varsayılan: bugün)
    bitis_tarihi     : %YYYY-%AA-%GG formatında bitiş tarihi (Varsayılan: bugün)

    Geri Dönüş Değeri
    -----------------
    İşlemler (Fiyat: ₺/1000 sm3, Miktar: x1000 sm3)
    """
    if __dogrulama.__baslangic_bitis_tarih_dogrulama(baslangic_tarihi, bitis_tarihi):
        try:
            particular_url = \
                __first_part_url + "bluecode-operation" + "?startDate=" + baslangic_tarihi + "&endDate=" + bitis_tarihi
            json = __make_requests(particular_url)
            df = __pd.DataFrame(json["body"]["operations"])
            df = df.rename(
                columns={"gasDay": "Gaz Günü", "contractName": "Kontrat İsmi", "amount": "Miktar",
                         "weightedAverage": "AOF"})
            df["Gaz Günü"] = df["Gaz Günü"].apply(lambda s: __pd.to_datetime(s[:10]))
        except (KeyError, TypeError):
            return __pd.DataFrame()
        else:
            return df


def ilave_dengeleyici_3_kodlu_islemler(baslangic_tarihi=__dt.datetime.today().strftime("%Y-%m-%d"),
                                       bitis_tarihi=__dt.datetime.today().strftime("%Y-%m-%d")):
    """
    İlgili tarih aralığı için ilave dengeleyici 3 kodlu işlemler listesini vermektedir.

    Parametreler
    ------------
    baslangic_tarihi : %YYYY-%AA-%GG formatında başlangıç tarihi (Varsayılan: bugün)
    bitis_tarihi     : %YYYY-%AA-%GG formatında bitiş tarihi (Varsayılan: bugün)

    Geri Dönüş Değeri
    -----------------
    İşlemler (Fiyat: ₺/1000 sm3, Miktar: x1000 sm3)
    """
    if __dogrulama.__baslangic_bitis_tarih_dogrulama(baslangic_tarihi, bitis_tarihi):
        try:
            particular_url = \
                __first_part_url + "orangecode-operation" + "?startDate=" + baslangic_tarihi + "&endDate=" + bitis_tarihi
            json = __make_requests(particular_url)
            df = __pd.DataFrame(json["body"]["operations"])
        except (KeyError, TypeError):
            return __pd.DataFrame()
        else:
            return df


def ilave_dengeleyici_4_kodlu_islemler(baslangic_tarihi=__dt.datetime.today().strftime("%Y-%m-%d"),
                                       bitis_tarihi=__dt.datetime.today().strftime("%Y-%m-%d")):
    """
    İlgili tarih aralığı için ilave dengeleyici 4 kodlu işlemler listesini vermektedir.

    Parametreler
    ------------
    baslangic_tarihi : %YYYY-%AA-%GG formatında başlangıç tarihi (Varsayılan: bugün)
    bitis_tarihi     : %YYYY-%AA-%GG formatında bitiş tarihi (Varsayılan: bugün)

    Geri Dönüş Değeri
    -----------------
    İşlemler (Fiyat: ₺/1000 sm3, Miktar: x1000 sm3)
    """
    if __dogrulama.__baslangic_bitis_tarih_dogrulama(baslangic_tarihi, bitis_tarihi):
        try:
            particular_url = \
                __first_part_url + "fourcode-operation" + "?startDate=" + baslangic_tarihi + "&endDate=" + bitis_tarihi
            json = __make_requests(particular_url)
            df = __pd.DataFrame(json["body"]["operations"])
        except (KeyError, TypeError):
            return __pd.DataFrame()
        else:
            return df


def ilave_dengeleyici_bildirimleri(baslangic_tarihi=__dt.datetime.today().strftime("%Y-%m-%d"),
                                   bitis_tarihi=__dt.datetime.today().strftime("%Y-%m-%d")):
    """
    İlgili tarih aralığı için ilave dengeleyici bildirimlerini vermektedir.

    Parametreler
    ------------
    baslangic_tarihi : %YYYY-%AA-%GG formatında başlangıç tarihi (Varsayılan: bugün)
    bitis_tarihi     : %YYYY-%AA-%GG formatında bitiş tarihi (Varsayılan: bugün)

    Geri Dönüş Değeri
    -----------------
    Bildirimler
    """
    if __dogrulama.__baslangic_bitis_tarih_dogrulama(baslangic_tarihi, bitis_tarihi):
        try:
            particular_url = \
                __first_part_url + "additional-notification" + "?startDate=" + baslangic_tarihi + "&endDate=" + \
                bitis_tarihi
            json = __make_requests(particular_url)
            df = __pd.DataFrame(json["body"]["additionalNotifications"])
            df = df.rename(
                columns={"date": "Tarih", "subjectTr": "Konu", "messageTr": "Açıklama"})
            df["Tarih"] = __pd.to_datetime((df["Tarih"]))
            df = df[["Tarih", "Konu", "Açıklama"]]
        except (KeyError, TypeError):
            return __pd.DataFrame()
        else:
            return df


def dengesizlik_ve_tahsilat(baslangic_tarihi=__dt.datetime.today().strftime("%Y-%m-%d"),
                            bitis_tarihi=__dt.datetime.today().strftime("%Y-%m-%d")):
    """
    İlgili tarih aralığı için dengesizlik ve tahsilat verilerini vermektedir.

    Parametreler
    ------------
    baslangic_tarihi : %YYYY-%AA-%GG formatında başlangıç tarihi (Varsayılan: bugün)
    bitis_tarihi     : %YYYY-%AA-%GG formatında bitiş tarihi (Varsayılan: bugün)

    Geri Dönüş Değeri
    -----------------
    Bildirimler
    """
    if __dogrulama.__baslangic_bitis_tarih_dogrulama(baslangic_tarihi, bitis_tarihi):
        try:
            particular_url = \
                __first_part_url + "allowance" + "?startDate=" + baslangic_tarihi + "&endDate=" + \
                bitis_tarihi
            json = __make_requests(particular_url)
            df = __pd.DataFrame(json["body"]["allowances"])
            df = df.rename(
                columns={"gasDay": "Gaz Günü", "type": "Veri Türü", "inputDataPyhsical": "Fiziki Giriş (Sm3)",
                         "outputDataPyhsical": "Fiziki Çıkış (Sm3)",
                         "inputDataVirtual": "Sanal Giriş (Sm3)", "outputDataVirtual": "Sanal Çıkış (Sm3)",
                         "systemDirection": "Sistem Yönü",
                         "negativeImbalance": "Negatif Dengesizlik Miktarı (Sm3)",
                         "positiveImbalance": "Pozitif Dengesizlik Miktarı (Sm3)",
                         "negativeImbalanceTradeValue": "Negatif Dengesizlik Tutarı (TL)",
                         "positiveImbalanceTradeValue": "Pozitif Dengesizlik Tutarı (TL)"})
            df["Gaz Günü"] = df["Gaz Günü"].apply(lambda x: __pd.to_datetime(x[:10]))
        except (KeyError, TypeError):
            return __pd.DataFrame()
        else:
            return df


def bast(baslangic_tarihi=__dt.datetime.today().strftime("%Y-%m-%d"),
         bitis_tarihi=__dt.datetime.today().strftime("%Y-%m-%d")):
    """
    İlgili tarih aralığı için bakiye sıfırlama tutarı bilgisini vermektedir.

    Parametreler
    ------------
    baslangic_tarihi : %YYYY-%AA-%GG formatında başlangıç tarihi (Varsayılan: bugün)
    bitis_tarihi     : %YYYY-%AA-%GG formatında bitiş tarihi (Varsayılan: bugün)

    Geri Dönüş Değeri
    -----------------
    BAST (TL)
    """
    if __dogrulama.__baslangic_bitis_tarih_dogrulama(baslangic_tarihi, bitis_tarihi):
        try:
            particular_url = \
                __first_part_url + "zero-balance" + "?startDate=" + baslangic_tarihi + "&endDate=" + \
                bitis_tarihi
            json = __make_requests(particular_url)
            df = __pd.DataFrame(json["body"]["zeroBalances"])
            df = df.rename(columns={"gasDay": "Gaz Günü", "zeroBalance": "BAST(TL)"})
            df["Gaz Günü"] = df["Gaz Günü"].apply(lambda s: __pd.to_datetime(s[:10]))
        except (KeyError, TypeError):
            return __pd.DataFrame()
        else:
            return df


def gddk(baslangic_tarihi=__dt.datetime.today().strftime("%Y-%m-%d"),
         bitis_tarihi=__dt.datetime.today().strftime("%Y-%m-%d")):
    """
    İlgili tarih aralığı için geriye dönük düzeltme kalemi (GDDK) bilgisini vermektedir.

    Parametreler
    ------------
    baslangic_tarihi : %YYYY-%AA-%GG formatında başlangıç tarihi (Varsayılan: bugün)
    bitis_tarihi     : %YYYY-%AA-%GG formatında bitiş tarihi (Varsayılan: bugün)

    Geri Dönüş Değeri
    -----------------
    GDDK (TL)
    """
    if __dogrulama.__baslangic_bitis_tarih_dogrulama(baslangic_tarihi, bitis_tarihi):
        try:
            particular_url = \
                __first_part_url + "past-invoice" + "?startDate=" + baslangic_tarihi + "&endDate=" + \
                bitis_tarihi
            json = __make_requests(particular_url)
            df = __pd.DataFrame(json["body"]["pastInvoices"])
            df = df.rename(columns={"period": "Dönem", "version": "Versiyon", "debt": "GDDK Borç Tutarı (TL)",
                                    "credit": "GDDK Alacak Tutarı (TL)"})
        except (KeyError, TypeError):
            return __pd.DataFrame()
        else:
            return df


def islem_akisi(baslangic_tarihi=__dt.datetime.today().strftime("%Y-%m-%d"),
                bitis_tarihi=__dt.datetime.today().strftime("%Y-%m-%d")):
    """
    İlgili tarih aralığı için işlem akışı bilgisini vermektedir.

    Parametreler
    ------------
    baslangic_tarihi : %YYYY-%AA-%GG formatında başlangıç tarihi (Varsayılan: bugün)
    bitis_tarihi     : %YYYY-%AA-%GG formatında bitiş tarihi (Varsayılan: bugün)

    Geri Dönüş Değeri
    -----------------
    İşlem Akışı (Fiyat: TL/1000 sm3, Miktar: x1000 sm3)
    """
    if __dogrulama.__baslangic_bitis_tarih_dogrulama(baslangic_tarihi, bitis_tarihi):
        try:
            particular_url = \
                __first_part_url + "transaction-history" + "?startDate=" + baslangic_tarihi + "&endDate=" + \
                bitis_tarihi
            json = __make_requests(particular_url)
            df = __pd.DataFrame(json["body"]["transactionHistories"])
            df = df.rename(
                columns={"contractName": "Kontrat", "mathcingDate": "Tarih", "price": "Fiyat", "quantity": "Miktar"})
            df["Tarih"] = __pd.to_datetime(df["Tarih"])
            df = df[["Kontrat", "Tarih", "Fiyat", "Miktar"]]
        except (KeyError, TypeError):
            return __pd.DataFrame()
        else:
            return df
