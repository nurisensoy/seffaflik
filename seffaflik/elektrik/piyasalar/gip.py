import requests as __requests
import pandas as __pd
import datetime as __dt
import logging as __logging

from seffaflik.ortak import dogrulama as __dogrulama, parametreler as __param, anahtar as __api

__transparency_url = __param.SEFFAFLIK_URL + "market/"
__headers = __api.HEADERS


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
    while __dogrulama.__baslangic_bitis_tarih_dogrulama(baslangic_tarihi, bitis_tarihi):
        try:
            resp = __requests.get(__transparency_url + "intra-day-summary" +
                                  "?startDate=" + baslangic_tarihi + "&endDate=" + bitis_tarihi,
                                  headers=__headers)
            list_ozet = resp.json()["body"]["intraDaySummaryList"]
            df_ozet = __pd.DataFrame(list_ozet)
            df_ozet["Saat"] = df_ozet["date"].apply(lambda h: int(h[11:13]))
            df_ozet["Tarih"] = __pd.to_datetime(df_ozet["date"].apply(lambda d: d[:10]))
            df_ozet.drop("date", axis=1, inplace=True)
            df_ozet.rename(index=str,
                           columns={"id": "Id", "contract": "Kontrat Adı",
                                    "quantityOfAsk": "Teklif Edilen Alış Miktarı",
                                    "quantityOfBid": "Teklif Edilen Satış Miktarı", "volume": "Eşleşme Miktarı",
                                    "tradingVolume": "İşlem Hacmi",
                                    "minAskPrice": "Min. Alış Fiyatı", "maxAskPrice": "Max. Alış Fiyatı",
                                    "minBidPrice": "Min. Satış Fiyatı", "maxBidPrice": "Max. Satış Fiyatı",
                                    "minMatchPrice": "Min. Eşleşme Fiyatı",
                                    "maxMatchPrice": "Max. Eşleşme Fiyatı"},
                           inplace=True)
            df_ozet = df_ozet[
                ["Tarih", "Saat", "Id", "Kontrat Adı", "Teklif Edilen Alış Miktarı", "Teklif Edilen Satış Miktarı",
                 "Eşleşme Miktarı", "İşlem Hacmi", "Min. Alış Fiyatı", "Max. Alış Fiyatı",
                 "Min. Satış Fiyatı", "Max. Satış Fiyatı",
                 "Min. Eşleşme Fiyatı", "Max. Eşleşme Fiyatı"]]
        except __requests.exceptions.RequestException:
            __logging.error(__param.__request_error, exc_info=True)
        except KeyError:
            return __pd.DataFrame()
        else:
            return df_ozet


def aof(baslangic_tarihi=__dt.datetime.today().strftime("%Y-%m-%d"),
        bitis_tarihi=__dt.datetime.today().strftime("%Y-%m-%d")):
    """
    İlgili tarih aralığı için saatlik pağırlıklı ortalama fiyat vermektedir.

    Parametreler
    ------------
    baslangic_tarihi : %YYYY-%AA-%GG formatında başlangıç tarihi (Varsayılan: bugün)
    bitis_tarihi     : %YYYY-%AA-%GG formatında bitiş tarihi (Varsayılan: bugün)

    Geri Dönüş Değeri
    -----------------
    Ağırlıklı Ortalama Fiyat (₺/MWh)
    """
    if __dogrulama.__baslangic_bitis_tarih_dogrulama(baslangic_tarihi, bitis_tarihi):
        try:
            resp = __requests.get(
                __transparency_url + "intra-day-aof" + "?startDate=" + baslangic_tarihi + "&endDate=" + bitis_tarihi,
                headers=__headers)
            list_aof = resp.json()["body"]["idmAofList"]
            df_aof = __pd.DataFrame(list_aof)
            df_aof["Saat"] = df_aof["date"].apply(lambda h: int(h[11:13]))
            df_aof["Tarih"] = __pd.to_datetime(df_aof["date"].apply(lambda d: d[:10]))
            df_aof.drop("date", axis=1, inplace=True)
            df_aof.rename(index=str, columns={"price": "AOF"}, inplace=True)
            df_aof = df_aof[["Tarih", "Saat", "AOF"]]
        except __requests.exceptions.RequestException:
            __logging.error(__param.__request_error, exc_info=True)
        except KeyError:
            return __pd.DataFrame()
        else:
            return df_aof


def eslesme_miktari(baslangic_tarihi=__dt.datetime.today().strftime("%Y-%m-%d"),
                    bitis_tarihi=__dt.datetime.today().strftime("%Y-%m-%d")):
    """
    İlgili tarih aralığı için gün içi piyasası (GİP) hacim bilgilerini vermektedir.

    Parametreler
    ----------
    baslangic_tarihi : %YYYY-%AA-%GG formatında başlangıç tarihi (Varsayılan: bugün)
    bitis_tarihi   : %YYYY-%AA-%GG formatında bitiş tarihi (Varsayılan: bugün)

    Geri Dönüş Değeri
    ------
    Eşleşme Miktarı (MWh)
    """
    while __dogrulama.__baslangic_bitis_tarih_dogrulama(baslangic_tarihi, bitis_tarihi):
        try:
            resp = __requests.get(__transparency_url + "intra-day-volume" +
                                  "?startDate=" + baslangic_tarihi + "&endDate=" + bitis_tarihi,
                                  headers=__headers)
            list_hacim = resp.json()["body"]["matchDetails"]
            df_hacim = __pd.DataFrame(list_hacim)
            df_hacim["Saat"] = df_hacim["date"].apply(lambda h: int(h[11:13]))
            df_hacim["Tarih"] = __pd.to_datetime(df_hacim["date"].apply(lambda d: d[:10]))
            df_hacim.drop("date", axis=1, inplace=True)
            df_hacim.rename(index=str,
                            columns={"blockMatchQuantity": "Blok Eşleşme Miktarı",
                                     "hourlyMatchQuantity": "Saatlik Eşleşme Miktarı"},
                            inplace=True)
            df_hacim.fillna(0.0, inplace=True)
            df_hacim["Eşleşme Miktarı"] = df_hacim["Blok Eşleşme Miktarı"] + df_hacim["Saatlik Eşleşme Miktarı"]
            df_hacim = df_hacim[["Tarih", "Saat", "Blok Eşleşme Miktarı", "Saatlik Eşleşme Miktarı", "Eşleşme Miktarı"]]
        except __requests.exceptions.RequestException:
            __logging.error(__param.__request_error, exc_info=True)
        except KeyError:
            return __pd.DataFrame()
        else:
            return df_hacim


def islem_hacmi(baslangic_tarihi=__dt.datetime.today().strftime("%Y-%m-%d"),
                bitis_tarihi=__dt.datetime.today().strftime("%Y-%m-%d")):
    """
    İlgili tarih aralığı için gün içi piyasası arz/talep işlem hacmi bilgilerini vermektedir.

    Parametreler
    ------------
    baslangic_tarihi : %YYYY-%AA-%GG formatında başlangıç tarihi (Varsayılan: bugün)
    bitis_tarihi   : %YYYY-%AA-%GG formatında bitiş tarihi (Varsayılan: bugün)

    Geri Dönüş Değeri
    -----------------
    Arz/Talep İşlem Hacmi (₺)
    """
    while __dogrulama.__baslangic_bitis_tarih_dogrulama(baslangic_tarihi, bitis_tarihi):
        try:
            resp = __requests.get(__transparency_url + "intra-day-income" +
                                  "?startDate=" + baslangic_tarihi + "&endDate=" + bitis_tarihi,
                                  headers=__headers)
            list_hacim = resp.json()["body"]["incomes"]
            df_hacim = __pd.DataFrame(list_hacim)
            df_hacim["Saat"] = df_hacim["date"].apply(lambda h: int(h[11:13]))
            df_hacim["Tarih"] = __pd.to_datetime(df_hacim["date"].apply(lambda d: d[:10]))
            df_hacim.drop("date", axis=1, inplace=True)
            df_hacim.rename(index=str, columns={"income": "İşlem Hacmi"}, inplace=True)
            df_hacim = df_hacim[["Tarih", "Saat", "İşlem Hacmi"]]
        except __requests.exceptions.RequestException:
            __logging.error(__param.__request_error, exc_info=True)
        except KeyError:
            return __pd.DataFrame()
        else:
            return df_hacim


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
    while __dogrulama.__baslangic_bitis_tarih_dogrulama(baslangic_tarihi, bitis_tarihi):
        try:
            resp = __requests.get(__transparency_url + "intra-day-trade-history" +
                                  "?startDate=" + baslangic_tarihi + "&endDate=" + bitis_tarihi,
                                  headers=__headers)
            list_akis = resp.json()["body"]["intraDayTradeHistoryList"]
            df_akis = __pd.DataFrame(list_akis)
            df_akis["Saat"] = __pd.to_timedelta(df_akis["date"].apply(lambda d: d[11:18]))
            df_akis["Tarih"] = __pd.to_datetime(df_akis["date"].apply(lambda d: d[:10]))
            df_akis.drop("date", axis=1, inplace=True)
            df_akis.rename(index=str,
                           columns={"id": "Id", "conract": "Kontrat Adı", "price": "Fiyat", "quantity": "Miktar"},
                           inplace=True)
            df_akis = df_akis[["Tarih", "Saat", "Id", "Kontrat Adı", "Fiyat", "Miktar"]]
        except __requests.exceptions.RequestException:
            __logging.error(__param.__request_error, exc_info=True)
        except KeyError:
            return __pd.DataFrame()
        else:
            return df_akis


def teklif_edilen_miktarlar(baslangic_tarihi=__dt.datetime.today().strftime("%Y-%m-%d"),
                            bitis_tarihi=__dt.datetime.today().strftime("%Y-%m-%d")):
    """
    İlgili tarih aralığı için gün içi piyasasına teklif edilen saatlik ve blok satış/alış miktar bilgilerini
    vermektedir.

    Parametreler
    ------------
    baslangic_tarihi : %YYYY-%AA-%GG formatında başlangıç tarihi (Varsayılan: bugün)
    bitis_tarihi     : %YYYY-%AA-%GG formatında bitiş tarihi (Varsayılan: bugün)

    Geri Dönüş Değeri
    -----------------
    Teklif Edilen Saatlik/Blok Alış/Satış Miktarları
    """
    if __dogrulama.__baslangic_bitis_tarih_dogrulama(baslangic_tarihi, bitis_tarihi):
        try:
            resp = __requests.get(
                __transparency_url + "intra-day-quantity" + "?startDate=" + baslangic_tarihi + "&endDate=" +
                bitis_tarihi, headers=__headers)
            list_tem = resp.json()["body"]["offerQuantities"]
            df_tem = __pd.DataFrame(list_tem)
            df_tem["Saat"] = df_tem["effectiveDate"].apply(lambda h: int(h[11:13]))
            df_tem["Tarih"] = __pd.to_datetime(df_tem["effectiveDate"].apply(lambda d: d[:10]))
            df_tem.drop("effectiveDate", axis=1, inplace=True)
            df_tem.rename(index=str, columns={"blockPurchaseQuantity": "Blok Alış Miktarı",
                                              "blockSaleQuantity": "Blok Satış Miktarı",
                                              "hourlyPurchaseQuantity": "Saatlik Alış Miktarı",
                                              "hourlySaleQuantity": "Saatlik Satış Miktarı"},
                          inplace=True)
            df_tem = df_tem[["Tarih", "Saat", "Saatlik Alış Miktarı", "Blok Alış Miktarı",
                             "Saatlik Satış Miktarı", "Blok Satış Miktarı"]]
        except __requests.exceptions.RequestException:
            __logging.error(__param.__request_error, exc_info=True)
        except KeyError:
            return __pd.DataFrame()
        else:
            return df_tem


def min_max_fiyatlar(baslangic_tarihi=__dt.datetime.today().strftime("%Y-%m-%d"),
                     bitis_tarihi=__dt.datetime.today().strftime("%Y-%m-%d"), teklif_tipi="HOURLY"):
    """
    İlgili tarih aralığı ve teklif tipi teklif edilen ve eşleşme fiyatlarının minimum ve maksimum değerlerinin
    bilgilerini vermektedir.

    Parametreler
    ------------
    baslangic_tarihi : %YYYY-%AA-%GG formatında başlangıç tarihi (Varsayılan: bugün)
    bitis_tarihi     : %YYYY-%AA-%GG formatında bitiş tarihi (Varsayılan: bugün)
    teklif_tipi      : metin formatında teklif tipi ("HOURLY" ya da "BLOCK") (Varsayılan: "HOURLY")

    Geri Dönüş Değeri
    -----------------
    Teklif Edilen ve Eşleşen Tekliferin Min./Max. Fiyat Değerleri (₺/MWh)
    """
    if __dogrulama.__baslangic_bitis_tarih_dogrulama(baslangic_tarihi, bitis_tarihi):
        try:
            resp = __requests.get(
                __transparency_url + "intra-day-min-max-price" + "?startDate=" + baslangic_tarihi +
                "&endDate=" + bitis_tarihi + "&offerType=" + teklif_tipi, headers=__headers)
            list_fiyat = resp.json()["body"]["minMaxPriceList"]
            df_fiyat = __pd.DataFrame(list_fiyat)
            df_fiyat["Saat"] = df_fiyat["date"].apply(lambda h: int(h[11:13]))
            df_fiyat["Tarih"] = __pd.to_datetime(df_fiyat["date"].apply(lambda d: d[:10]))
            df_fiyat.drop("date", axis=1, inplace=True)
            df_fiyat.rename(index=str, columns={"minAskPrice": "Min. Alış Fiyatı", "maxAskPrice": "Max. Alış Fiyatı",
                                                "minBidPrice": "Min. Satış Fiyatı", "maxBidPrice": "Max. Satış Fiyatı",
                                                "minMatchPrice": "Min. Eşleşme Fiyatı",
                                                "maxMatchPrice": "Max. Eşleşme Fiyatı"},
                            inplace=True)
            df_fiyat = df_fiyat[["Tarih", "Saat", "Min. Alış Fiyatı", "Max. Alış Fiyatı", "Min. Satış Fiyatı",
                                 "Max. Satış Fiyatı", "Min. Eşleşme Fiyatı", "Max. Eşleşme Fiyatı"]]
        except __requests.exceptions.RequestException:
            __logging.error(__param.__request_error, exc_info=True)
        except KeyError:
            return __pd.DataFrame()
        else:
            return df_fiyat
