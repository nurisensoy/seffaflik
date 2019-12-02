import pandas as __pd
import datetime as __dt

from seffaflik.__ortak.__araclar import make_requests as __make_requests
from seffaflik.__ortak import __dogrulama as __dogrulama

__first_part_url = "market/"


def aof(baslangic_tarihi=__dt.datetime.today().strftime("%Y-%m-%d"),
        bitis_tarihi=__dt.datetime.today().strftime("%Y-%m-%d")):
    """
    İlgili tarih aralığı için gün içi piyasası (GİP) saatlik ağırlıklı ortalama fiyat (AOF) bilgisini vermektedir.

    Parametreler
    ------------
    baslangic_tarihi : %YYYY-%AA-%GG formatında başlangıç tarihi (Varsayılan: bugün)
    bitis_tarihi     : %YYYY-%AA-%GG formatında bitiş tarihi (Varsayılan: bugün)

    Geri Dönüş Değeri
    -----------------
    Saatlik Ağırlıklı Ortalama Fiyat (₺/MWh)
    """
    if __dogrulama.__baslangic_bitis_tarih_dogrulama(baslangic_tarihi, bitis_tarihi):
        try:
            particular_url = \
                __first_part_url + "intra-day-aof" + "?startDate=" + baslangic_tarihi + "&endDate=" + bitis_tarihi
            json = __make_requests(particular_url)
            df = __pd.DataFrame(json["body"]["idmAofList"])
            df["Saat"] = df["date"].apply(lambda h: int(h[11:13]))
            df["Tarih"] = __pd.to_datetime(df["date"].apply(lambda d: d[:10]))
            df.rename(index=str, columns={"price": "AOF"}, inplace=True)
            df = df[["Tarih", "Saat", "AOF"]]
        except KeyError:
            return __pd.DataFrame()
        else:
            return df


def ozet(baslangic_tarihi=__dt.datetime.today().strftime("%Y-%m-%d"),
         bitis_tarihi=__dt.datetime.today().strftime("%Y-%m-%d"), ):
    """
    İlgili tarih aralığı için gün içi piyasasına dair özet bilgilerini vermektedir.

    Parametreler
    ------------
    baslangic_tarihi : %YYYY-%AA-%GG formatında başlangıç tarihi (Varsayılan: bugün)
    bitis_tarihi   : %YYYY-%AA-%GG formatında bitiş tarihi (Varsayılan: bugün)

    Geri Dönüş Değeri
    -----------------
    Özet (Tarih, Saat, Id, Kontrat Adı, Teklif Edilen Alış/Satış Miktarları, Eşleşme Miktarı, İşlem Hacmi,
    Min. Alış/Satış Fiyatları, Max. Alış/Satış Fiyatları, Min./Max. Eşleşme Fiyatları)
    """
    if __dogrulama.__baslangic_bitis_tarih_dogrulama(baslangic_tarihi, bitis_tarihi):
        try:
            particular_url = \
                __first_part_url + "intra-day-summary" + "?startDate=" + baslangic_tarihi + "&endDate=" + bitis_tarihi
            json = __make_requests(particular_url)
            df = __pd.DataFrame(json["body"]["intraDaySummaryList"])
            df["Saat"] = df["date"].apply(lambda h: int(h[11:13]))
            df["Tarih"] = __pd.to_datetime(df["date"].apply(lambda d: d[:10]))
            df.rename(index=str,
                      columns={"id": "Id", "contract": "Kontrat Adı",
                               "quantityOfAsk": "Teklif Edilen Talep Miktarı",
                               "quantityOfBid": "Teklif Edilen Arz Miktarı", "volume": "Eşleşme Miktarı",
                               "tradingVolume": "İşlem Hacmi",
                               "minAskPrice": "Min. Talep Fiyatı", "maxAskPrice": "Max. Talep Fiyatı",
                               "minBidPrice": "Min. Arz Fiyatı", "maxBidPrice": "Max. Arz Fiyatı",
                               "minMatchPrice": "Min. Eşleşme Fiyatı",
                               "maxMatchPrice": "Max. Eşleşme Fiyatı"},
                      inplace=True)
            df = df[
                ["Tarih", "Saat", "Id", "Kontrat Adı", "Teklif Edilen Talep Miktarı", "Teklif Edilen Arz Miktarı",
                 "Eşleşme Miktarı", "İşlem Hacmi", "Min. Talep Fiyatı", "Max. Talep Fiyatı",
                 "Min. Arz Fiyatı", "Max. Arz Fiyatı",
                 "Min. Eşleşme Fiyatı", "Max. Eşleşme Fiyatı"]]
        except KeyError:
            return __pd.DataFrame()
        else:
            return df


def hacim(baslangic_tarihi=__dt.datetime.today().strftime("%Y-%m-%d"),
          bitis_tarihi=__dt.datetime.today().strftime("%Y-%m-%d")):
    """
    İlgili tarih aralığı için gün içi piyasası (GİP) hacim bilgilerini vermektedir.

    Parametreler
    ----------
    baslangic_tarihi : %YYYY-%AA-%GG formatında başlangıç tarihi (Varsayılan: bugün)
    bitis_tarihi   : %YYYY-%AA-%GG formatında bitiş tarihi (Varsayılan: bugün)

    Geri Dönüş Değeri
    ------
    GİP Saatlik Hacim (MWh)
    """
    if __dogrulama.__baslangic_bitis_tarih_dogrulama(baslangic_tarihi, bitis_tarihi):
        try:
            particular_url = \
                __first_part_url + "intra-day-volume" + "?startDate=" + baslangic_tarihi + "&endDate=" + bitis_tarihi
            json = __make_requests(particular_url)
            df = __pd.DataFrame(json["body"]["matchDetails"])
            df["Saat"] = df["date"].apply(lambda h: int(h[11:13]))
            df["Tarih"] = __pd.to_datetime(df["date"].apply(lambda d: d[:10]))
            df.rename(index=str,
                      columns={"blockMatchQuantity": "Blok Eşleşme Miktarı",
                               "hourlyMatchQuantity": "Saatlik Eşleşme Miktarı"},
                      inplace=True)
            df.fillna(0.0, inplace=True)
            df["Eşleşme Miktarı"] = df["Blok Eşleşme Miktarı"] + df["Saatlik Eşleşme Miktarı"]
            df = df[["Tarih", "Saat", "Blok Eşleşme Miktarı", "Saatlik Eşleşme Miktarı", "Eşleşme Miktarı"]]
        except KeyError:
            return __pd.DataFrame()
        else:
            return df


def islem_hacmi(baslangic_tarihi=__dt.datetime.today().strftime("%Y-%m-%d"),
                bitis_tarihi=__dt.datetime.today().strftime("%Y-%m-%d")):
    """
    İlgili tarih aralığı için gün içi piyasası (GİP) arz/talep işlem hacmi bilgilerini vermektedir.

    Parametreler
    ------------
    baslangic_tarihi : %YYYY-%AA-%GG formatında başlangıç tarihi (Varsayılan: bugün)
    bitis_tarihi     : %YYYY-%AA-%GG formatında bitiş tarihi (Varsayılan: bugün)

    Geri Dönüş Değeri
    -----------------
    Arz/Talep İşlem Hacmi (₺)
    """
    if __dogrulama.__baslangic_bitis_tarih_dogrulama(baslangic_tarihi, bitis_tarihi):
        try:
            particular_url = \
                __first_part_url + "intra-day-income" + "?startDate=" + baslangic_tarihi + "&endDate=" + bitis_tarihi
            json = __make_requests(particular_url)
            df = __pd.DataFrame(json["body"]["incomes"])
            df["Saat"] = df["date"].apply(lambda h: int(h[11:13]))
            df["Tarih"] = __pd.to_datetime(df["date"].apply(lambda d: d[:10]))
            df.rename(index=str, columns={"income": "İşlem Hacmi"}, inplace=True)
            df = df[["Tarih", "Saat", "İşlem Hacmi"]]
        except KeyError:
            return __pd.DataFrame()
        else:
            return df


def islem_akisi(baslangic_tarihi=__dt.datetime.today().strftime("%Y-%m-%d"),
                bitis_tarihi=__dt.datetime.today().strftime("%Y-%m-%d")):
    """
    İlgili tarih aralığı için gün içi piyasası işlem akış bilgilerini vermektedir.

    Parametreler
    ------------
    baslangic_tarihi : %YYYY-%AA-%GG formatında başlangıç tarihi (Varsayılan: bugün)
    bitis_tarihi   : %YYYY-%AA-%GG formatında bitiş tarihi (Varsayılan: bugün)

    Geri Dönüş Değeri
    -----------------
    İşlem Akışları (Tarih, Saat, Id, Kontrat Adı, Fiyat, Miktar)
    """
    if __dogrulama.__baslangic_bitis_tarih_dogrulama(baslangic_tarihi, bitis_tarihi):
        try:
            particular_url = __first_part_url + "intra-day-trade-history" + "?startDate=" + \
                             baslangic_tarihi + "&endDate=" + bitis_tarihi
            json = __make_requests(particular_url)
            df = __pd.DataFrame(json["body"]["intraDayTradeHistoryList"])
            df["Saat"] = __pd.to_timedelta(df["date"].apply(lambda d: d[11:18]))
            df["Tarih"] = __pd.to_datetime(df["date"].apply(lambda d: d[:10]))
            df.rename(index=str,
                      columns={"id": "Id", "conract": "Kontrat Adı", "price": "Fiyat", "quantity": "Miktar"},
                      inplace=True)
            df = df[["Tarih", "Saat", "Id", "Kontrat Adı", "Fiyat", "Miktar"]]
        except KeyError:
            return __pd.DataFrame()
        else:
            return df


def teklif_edilen_miktarlar(baslangic_tarihi=__dt.datetime.today().strftime("%Y-%m-%d"),
                            bitis_tarihi=__dt.datetime.today().strftime("%Y-%m-%d")):
    """
    İlgili tarih aralığı için gün içi piyasasına (GİP) teklif edilen saatlik ve blok arz/talep miktar bilgilerini
    vermektedir.

    Parametreler
    ------------
    baslangic_tarihi : %YYYY-%AA-%GG formatında başlangıç tarihi (Varsayılan: bugün)
    bitis_tarihi     : %YYYY-%AA-%GG formatında bitiş tarihi (Varsayılan: bugün)

    Geri Dönüş Değeri
    -----------------
    GİP Teklif Edilen Saatlik/Blok Talep/Arz Miktarları
    """
    if __dogrulama.__baslangic_bitis_tarih_dogrulama(baslangic_tarihi, bitis_tarihi):
        try:
            particular_url = \
                __first_part_url + "intra-day-quantity" + "?startDate=" + baslangic_tarihi + "&endDate=" + bitis_tarihi
            json = __make_requests(particular_url)
            df = __pd.DataFrame(json["body"]["offerQuantities"])
            df["Saat"] = df["effectiveDate"].apply(lambda h: int(h[11:13]))
            df["Tarih"] = __pd.to_datetime(df["effectiveDate"].apply(lambda d: d[:10]))
            df.rename(index=str, columns={"blockPurchaseQuantity": "Blok Talep Miktarı",
                                          "blockSaleQuantity": "Blok Arz Miktarı",
                                          "hourlyPurchaseQuantity": "Saatlik Talep Miktarı",
                                          "hourlySaleQuantity": "Saatlik Arz Miktarı"},
                      inplace=True)
            df = df[["Tarih", "Saat", "Saatlik Talep Miktarı", "Blok Talep Miktarı",
                     "Saatlik Arz Miktarı", "Blok Arz Miktarı"]]
        except KeyError:
            return __pd.DataFrame()
        else:
            return df


def min_max_fiyatlar(baslangic_tarihi=__dt.datetime.today().strftime("%Y-%m-%d"),
                     bitis_tarihi=__dt.datetime.today().strftime("%Y-%m-%d"), teklif_tipi="SAATLIK"):
    """
    İlgili tarih aralığı ve teklif tipi için gün içi piyasasında (GİP) teklif edilen ve eşleşme fiyatlarının minimum ve
    maksimum değerlerinin bilgilerini vermektedir.

    Parametreler
    ------------
    baslangic_tarihi : %YYYY-%AA-%GG formatında başlangıç tarihi (Varsayılan: bugün)
    bitis_tarihi     : %YYYY-%AA-%GG formatında bitiş tarihi (Varsayılan: bugün)
    teklif_tipi      : metin formatında teklif tipi ("SAATLIK" ya da "BLOK") (Varsayılan: "SAATLIK")

    Geri Dönüş Değeri
    -----------------
    GİP Teklif Edilen ve Eşleşen Tekliflerin Min./Max. Fiyat Değerleri (₺/MWh)
    """
    if __dogrulama.__baslangic_bitis_tarih_dogrulama(baslangic_tarihi, bitis_tarihi):
        try:
            bid_types = {"saatlik": "HOURLY", "blok": "BLOCK"}
            particular_url = \
                __first_part_url + "intra-day-min-max-price" + "?startDate=" + baslangic_tarihi + "&endDate=" \
                + bitis_tarihi + "&offerType=" + bid_types[teklif_tipi.lower()]
            json = __make_requests(particular_url)
            df = __pd.DataFrame(json["body"]["minMaxPriceList"])
            df["Saat"] = df["date"].apply(lambda h: int(h[11:13]))
            df["Tarih"] = __pd.to_datetime(df["date"].apply(lambda d: d[:10]))
            df.rename(index=str, columns={"minAskPrice": "Min. Alış Fiyatı", "maxAskPrice": "Max. Alış Fiyatı",
                                          "minBidPrice": "Min. Satış Fiyatı", "maxBidPrice": "Max. Satış Fiyatı",
                                          "minMatchPrice": "Min. Eşleşme Fiyatı",
                                          "maxMatchPrice": "Max. Eşleşme Fiyatı"},
                      inplace=True)
            df = df[["Tarih", "Saat", "Min. Alış Fiyatı", "Max. Alış Fiyatı", "Min. Satış Fiyatı",
                     "Max. Satış Fiyatı", "Min. Eşleşme Fiyatı", "Max. Eşleşme Fiyatı"]]
        except KeyError:
            return __pd.DataFrame()
        else:
            return df
