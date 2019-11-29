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
from seffaflik.elektrik.uretim import organizasyonlar as __organizasyonlar

__transparency_url = __param.SEFFAFLIK_URL + "market/"
__headers = __api.HEADERS


def ptf(baslangic_tarihi=__dt.datetime.today().strftime("%Y-%m-%d"),
        bitis_tarihi=__dt.datetime.today().strftime("%Y-%m-%d")):
    """
    İlgili tarih aralığı için saatlik gün öncesi piyasası (GÖP) piyasa takas fiyatlarını (PTF) vermektedir.

    Parametreler
    ------------
    baslangic_tarihi : %YYYY-%AA-%GG formatında başlangıç tarihi (Varsayılan: bugün)
    bitis_tarihi     : %YYYY-%AA-%GG formatında bitiş tarihi (Varsayılan: bugün)

    Geri Dönüş Değeri
    -----------------
    Saatlik PTF (₺/MWh)
    """
    if __dogrulama.__baslangic_bitis_tarih_dogrulama(baslangic_tarihi, bitis_tarihi):
        try:
            resp = __requests.get(
                __transparency_url + "day-ahead-mcp" + "?startDate=" + baslangic_tarihi + "&endDate=" + bitis_tarihi,
                headers=__headers, timeout=__param.__timeout)
            resp.raise_for_status()
            list_ptf = resp.json()["body"]["dayAheadMCPList"]
            df_ptf = __pd.DataFrame(list_ptf)
            df_ptf["Saat"] = df_ptf["date"].apply(lambda h: int(h[11:13]))
            df_ptf["Tarih"] = __pd.to_datetime(df_ptf["date"].apply(lambda d: d[:10]))
            df_ptf.rename(index=str, columns={"price": "PTF"}, inplace=True)
            df_ptf = df_ptf[["Tarih", "Saat", "PTF"]]
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
            return df_ptf


def hacim(baslangic_tarihi=__dt.datetime.today().strftime("%Y-%m-%d"),
          bitis_tarihi=__dt.datetime.today().strftime("%Y-%m-%d"), organizasyon_eic=""):
    """
    İlgili tarih aralığı için gün öncesi piyasası (GÖP) saatlik hacim bilgilerini vermektedir.
    Not:
    1) "organizasyon_eic" değeri girildiği taktirde organizasyona ait saatlik arz/talep eşleşme miktarı bilgisini
    vermektedir.
    2) "organizasyon_eic" değeri girilmediği taktirde saatlik arz/talep eşleşme miktarı, fiyattan bağımsız teklif edilen
    miktar, blok teklif eşleşme miktarı, ve maksimum teklif edilen miktar bilgisini vermektedir.

    Parametreler
    ------------
    baslangic_tarihi : %YYYY-%AA-%GG formatında başlangıç tarihi (Varsayılan: bugün)
    bitis_tarihi     : %YYYY-%AA-%GG formatında bitiş tarihi (Varsayılan: bugün)
    organizasyon_eic : metin formatında organizasyon eic kodu (Varsayılan: "")

    Geri Dönüş Değeri
    -----------------
    Arz/Talep Saatlik GÖP Hacmi (MWh)
    """
    if __dogrulama.__baslangic_bitis_tarih_eic_dogrulama(baslangic_tarihi, bitis_tarihi, organizasyon_eic):
        try:
            resp = __requests.get(__transparency_url + "day-ahead-market-volume" +
                                  "?startDate=" + baslangic_tarihi + "&endDate=" + bitis_tarihi + "&eic=" +
                                  organizasyon_eic, headers=__headers, timeout=__param.__timeout)
            resp.raise_for_status()
            list_hacim = resp.json()["body"]["dayAheadMarketVolumeList"]
            df_hacim = __pd.DataFrame(list_hacim)
            df_hacim["Saat"] = df_hacim["date"].apply(lambda h: int(h[11:13]))
            df_hacim["Tarih"] = __pd.to_datetime(df_hacim["date"].apply(lambda d: d[:10]))
            df_hacim.rename(index=str,
                            columns={"matchedBids": "Talep Eşleşme Miktarı", "matchedOffers": "Arz Eşleşme Miktarı",
                                     "volume": "Eşleşme Miktarı", "blockBid": "Arz Blok Teklif Eşleşme Miktarı",
                                     "blockOffer": "Talep Blok Teklif Eşleşme Miktarı",
                                     "priceIndependentBid": "Fiyattan Bağımsız Talep Miktarı",
                                     "priceIndependentOffer": "Fiyattan Bağımsız Arz Miktarı",
                                     "quantityOfAsk": "Maksimum Talep Miktarı",
                                     "quantityOfBid": "Maksimum Arz Miktarı"},
                            inplace=True)
            if organizasyon_eic == "":
                df_hacim = df_hacim[["Tarih", "Saat", "Talep Eşleşme Miktarı", "Eşleşme Miktarı", "Arz Eşleşme Miktarı",
                                     "Fiyattan Bağımsız Talep Miktarı", "Fiyattan Bağımsız Arz Miktarı",
                                     "Maksimum Talep Miktarı", "Maksimum Arz Miktarı"]]
            else:
                df_hacim = df_hacim[["Tarih", "Saat", "Talep Eşleşme Miktarı", "Arz Eşleşme Miktarı"]]
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
            return df_hacim


def tum_organizasyonlar_hacim(baslangic_tarihi=__dt.datetime.today().strftime("%Y-%m-%d"),
                              bitis_tarihi=__dt.datetime.today().strftime("%Y-%m-%d"), hacim_tipi="NET"):
    """
    İlgili tarih aralığı için tüm organizasyonların saatlik net hacim bilgilerini vermektedir.

    Parametreler
    ------------
    baslangic_tarihi : %YYYY-%AA-%GG formatında başlangıç tarihi (Varsayılan: bugün)
    bitis_tarihi     : %YYYY-%AA-%GG formatında bitiş tarihi (Varsayılan: bugün)
    hacim_tipi       : metin formatında hacim tipi ("NET", "ARZ", yada "TALEP") (varsayılan: "NET")

    Geri Dönüş Değeri
    -----------------
    Tüm Organizasyonların Saatlik GÖP Hacmi (Tarih, Saat, Hacim)
    """
    if __dogrulama.__baslangic_bitis_tarih_dogrulama(baslangic_tarihi, bitis_tarihi):
        org = __organizasyonlar()
        list_org = org[["EIC Kodu", "Kısa Adı"]].to_dict("records")
        org_len = len(list_org)
        list_date_org_eic = list(zip([baslangic_tarihi] * org_len, [bitis_tarihi] * org_len, list_org))
        list_date_org_eic = list(map(list, list_date_org_eic))
        with __Pool(__mp.cpu_count()) as p:
            if hacim_tipi.lower() == "net":
                list_df_unit = p.starmap(__organizasyonel_net_hacim, list_date_org_eic, chunksize=1)
            elif hacim_tipi.lower() == "arz":
                list_df_unit = p.starmap(__organizasyonel_arz_hacim, list_date_org_eic, chunksize=1)
            elif hacim_tipi.lower() == "talep":
                list_df_unit = p.starmap(__organizasyonel_talep_hacim, list_date_org_eic, chunksize=1)
            else:
                __logging.error("Lütfen geçerli bir hacim tipi giriniz: Net, Arz, Talep", exc_info=False)
        list_df_unit = list(filter(lambda x: len(x) > 0, list_df_unit))
        df_unit = __red(lambda left, right: __pd.merge(left, right, how="outer", on=["Tarih", "Saat"], sort=True),
                        list_df_unit)
        return df_unit


def arz_talep_egrisi(tarih=__dt.datetime.today().strftime("%Y-%m-%d")):
    """
    İlgili tarih için saatlik arz-talep eğrisinde bulunan fiyat-miktar ikililerini vermektedir.
    Kabul edilen blok ve esnek tekliflerin eklenmiş halidir.

    Parametreler
    ------------
    tarih : %YYYY-%AA-%GG formatında tarih (Varsayılan: bugün)

    Geri Dönüş Değeri
    -----------------
    Arz-Talep Eğrisi Fiyat ve Alış/Satış Miktarı (₺/MWh, MWh)
    """
    if __dogrulama.__tarih_dogrulama(tarih):
        try:
            resp = __requests.get(__transparency_url + "supply-demand-curve" + "?period=" + tarih, headers=__headers,
                                  timeout=__param.__timeout)
            resp.raise_for_status()
            list_egri = resp.json()["body"]["supplyDemandCurves"]
            df_egri = __pd.DataFrame(list_egri)
            df_egri["Saat"] = df_egri["date"].apply(lambda x: int(x[11:13]))
            df_egri.rename(index=str, columns={"demand": "Talep", "supply": "Arz", "price": "Fiyat"}, inplace=True)
            df_egri = df_egri[["Saat", "Talep", "Fiyat", "Arz"]]
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
            return df_egri


def islem_hacmi(baslangic_tarihi=__dt.datetime.today().strftime("%Y-%m-%d"),
                bitis_tarihi=__dt.datetime.today().strftime("%Y-%m-%d")):
    """
    İlgili tarih aralığı için arz/talep işlem hacmi bilgilerini vermektedir.

    Parametreler
    ------------
    baslangic_tarihi : %YYYY-%AA-%GG formatın
    Parametreler
    ------------
    baslangic_tarihi : %YYYY-%AA-%GG formatında başlangıç tarihi (Varsayılan: bugün)
    bitis_tarihi   : %YYYY-%AA-%GG formatında bitiş tarihi (Varsayılan: bugün)

    Geri Dönüş Değeri
    -----------------
    Arz/Talep İşlem Hacmi (₺)
    """
    if __dogrulama.__baslangic_bitis_tarih_dogrulama(baslangic_tarihi, bitis_tarihi):
        try:
            resp = __requests.get(__transparency_url + "day-ahead-market-trade-volume" +
                                  "?startDate=" + baslangic_tarihi + "&endDate=" + bitis_tarihi,
                                  headers=__headers, timeout=__param.__timeout)
            resp.raise_for_status()
            list_hacim = resp.json()["body"]["dayAheadMarketTradeVolumeList"]
            df_hacim = __pd.DataFrame(list_hacim)
            df_hacim["Saat"] = df_hacim["date"].apply(lambda h: int(h[11:13]))
            df_hacim["Tarih"] = __pd.to_datetime(df_hacim["date"].apply(lambda d: d[:10]))
            df_hacim.rename(index=str,
                            columns={"volumeOfBid": "Talep İşlem Hacmi", "volumeOfAsk": "Arz İşlem Hacmi"},
                            inplace=True)
            df_hacim = df_hacim[["Tarih", "Saat", "Talep İşlem Hacmi", "Arz İşlem Hacmi"]]
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
            return df_hacim


def blok_miktari(baslangic_tarihi=__dt.datetime.today().strftime("%Y-%m-%d"),
                 bitis_tarihi=__dt.datetime.today().strftime("%Y-%m-%d")):
    """
    İlgili tarih aralığı için saatlik gün öncesi piyasası teklif edilen ve eşleşen blok teklif miktar bilgilerini
    vermektedir.

    Parametreler
    ------------
    baslangic_tarihi : %YYYY-%AA-%GG formatında başlangıç tarihi (Varsayılan: bugün)
    bitis_tarihi   : %YYYY-%AA-%GG formatında bitiş tarihi (Varsayılan: bugün)

    Geri Dönüş Değeri
    -----------------
    Teklif Edilen ve Eşleşen Blok Teklif Miktarları (MWh)
    """
    if __dogrulama.__baslangic_bitis_tarih_dogrulama(baslangic_tarihi, bitis_tarihi):
        try:
            resp = __requests.get(__transparency_url + "amount-of-block"
                                  + "?startDate=" + baslangic_tarihi + "&endDate=" + bitis_tarihi,
                                  headers=__headers, timeout=__param.__timeout)
            resp.raise_for_status()
            list_blok = resp.json()["body"]["amountOfBlockList"]
            df_blok = __pd.DataFrame(list_blok)
            df_blok["Saat"] = df_blok["date"].apply(lambda h: int(h[11:13]))
            df_blok["Tarih"] = __pd.to_datetime(df_blok["date"].apply(lambda d: d[:10]))
            df_blok.rename(index=str,
                           columns={"amountOfPurchasingTowardsBlock": "Talep Blok Teklif Miktarı",
                                    "amountOfPurchasingTowardsMatchBlock": "Eşleşen Talep Blok Teklif Miktarı",
                                    "amountOfSalesTowardsBlock": "Arz Blok Teklif Miktarı",
                                    "amountOfSalesTowardsMatchBlock": "Eşleşen Arz Blok Teklif Miktarı"},
                           inplace=True)
            df_blok = df_blok[["Tarih", "Saat", "Talep Blok Teklif Miktarı", "Eşleşen Talep Blok Teklif Miktarı",
                               "Arz Blok Teklif Miktarı", "Eşleşen Arz Blok Teklif Miktarı"]]
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
            return df_blok


def fark_tutari(baslangic_tarihi=__dt.datetime.today().strftime("%Y-%m-%d"),
                bitis_tarihi=__dt.datetime.today().strftime("%Y-%m-%d")):
    """
    İlgili tarih aralığı için günlük gün öncesi piyasasında arz, talep ve yuvarlama kaynaklı oluşan fark tutarı
    bilgilerini vermektedir.

    Parametreler
    ------------
    baslangic_tarihi : %YYYY-%AA-%GG formatında başlangıç tarihi (Varsayılan: bugün)
    bitis_tarihi   : %YYYY-%AA-%GG formatında bitiş tarihi (Varsayılan: bugün)

    Geri Dönüş Değeri
    -----------------
    Arz, Talep, Yuvarlama Kaynaklı Fark Tutarı (₺)
    """
    if __dogrulama.__baslangic_bitis_tarih_dogrulama(baslangic_tarihi, bitis_tarihi):
        try:
            resp = __requests.get(__transparency_url + "day-ahead-diff-funds"
                                  + "?startDate=" + baslangic_tarihi + "&endDate=" + bitis_tarihi,
                                  headers=__headers, timeout=__param.__timeout)
            resp.raise_for_status()
            list_ft = resp.json()["body"]["diffFundList"]
            df_ft = __pd.DataFrame(list_ft)
            df_ft["date"] = __pd.to_datetime(df_ft["date"].apply(lambda d: d[:10]))
            df_ft.rename(index=str, columns={"originatingFromBids": "Talep",
                                             "originatingFromOffers": "Arz",
                                             "originatingFromRounding": "Yuvarlama",
                                             "date": "Tarih", "total": "Toplam"}, inplace=True)
            df_ft = df_ft[["Tarih", "Talep", "Arz", "Yuvarlama", "Toplam"]]
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
            return df_ft


def kptf(tarih=(__dt.datetime.today() + __dt.timedelta(days=1)).strftime("%Y-%m-%d")):
    """
    İlgili tarih için saatlik gün öncesi piyasası kesinleşmemiş piyasa takas fiyatını vermektedir.

    Parametreler
    ------------
    tarih : %YYYY-%AA-%GG formatında tarih (Varsayılan: yarın)

    Geri Dönüş Değeri
    -----------------
    Kesinleşmemiş Piyasa Takas Fiyatı (₺/MWh)
    """
    if __dogrulama.__tarih_dogrulama(tarih):
        try:
            resp = __requests.get(__transparency_url + "day-ahead-interim-mcp" + "?period=" + tarih,
                                  headers=__headers, timeout=__param.__timeout)
            resp.raise_for_status()
            list_kptf = resp.json()["body"]["interimMCPList"]
            df_kptf = __pd.DataFrame(list_kptf)
            df_kptf["Saat"] = df_kptf["date"].apply(lambda x: int(x[11:13]))
            df_kptf.rename(index=str, columns={"marketTradePrice": "KPTF"}, inplace=True)
            df_kptf = df_kptf[["Saat", "KPTF"]]
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
            return df_kptf


def __organizasyonel_net_hacim(baslangic_tarihi, bitis_tarihi, org):
    """
    İlgili tarih aralığı ve organizasyon için gün öncesi piyasası toplam eşleşme miktar bilgisini vermektedir.

    Parametreler
    ------------
    baslangic_tarihi : %YYYY-%AA-%GG formatında başlangıç tarihi
    bitis_tarihi     : %YYYY-%AA-%GG formatında bitiş tarihi
    org              : dict formatında organizasyon EIC Kodu, Kısa Adı

    Geri Dönüş Değeri
    -----------------
    Organizasyonel Net Eşleşme Miktarı (MWh)
    """
    try:
        resp = __requests.get(__transparency_url + "day-ahead-market-volume" +
                              "?startDate=" + baslangic_tarihi + "&endDate=" + bitis_tarihi + "&eic=" + org["EIC Kodu"],
                              headers=__headers, timeout=__param.__timeout)
        resp.raise_for_status()
        list_hacim = resp.json()["body"]["dayAheadMarketVolumeList"]
        df_hacim = __pd.DataFrame(list_hacim)
        df_hacim["Saat"] = df_hacim["date"].apply(lambda h: int(h[11:13]))
        df_hacim["Tarih"] = __pd.to_datetime(df_hacim["date"].apply(lambda d: d[:10]))
        df_hacim.rename(index=str,
                        columns={"matchedBids": "Talep Eşleşme Miktarı", "matchedOffers": "Arz Eşleşme Miktarı"},
                        inplace=True)
        df_hacim[org["EIC Kodu"]] = df_hacim["Talep Eşleşme Miktarı"] - df_hacim["Arz Eşleşme Miktarı"]
        df_hacim = df_hacim[["Tarih", "Saat", org["EIC Kodu"]]]
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
        return df_hacim


def __organizasyonel_arz_hacim(baslangic_tarihi, bitis_tarihi, org):
    """
    İlgili tarih aralığı ve organizasyon için gün öncesi piyasası saatlik arz eşleşme miktar bilgisini vermektedir.

    Parametreler
    ------------
    baslangic_tarihi : %YYYY-%AA-%GG formatında başlangıç tarihi
    bitis_tarihi     : %YYYY-%AA-%GG formatında bitiş tarihi
    org              : dict formatında organizasyon EIC Kodu, Kısa Adı

    Geri Dönüş Değeri
    -----------------
    Organizasyonel Arz Eşleşme Miktarı (MWh)
    """
    try:
        resp = __requests.get(__transparency_url + "day-ahead-market-volume" +
                              "?startDate=" + baslangic_tarihi + "&endDate=" + bitis_tarihi + "&eic=" + org["EIC Kodu"],
                              headers=__headers, timeout=__param.__timeout)
        resp.raise_for_status()
        list_hacim = resp.json()["body"]["dayAheadMarketVolumeList"]
        df_hacim = __pd.DataFrame(list_hacim)
        df_hacim["Saat"] = df_hacim["date"].apply(lambda h: int(h[11:13]))
        df_hacim["Tarih"] = __pd.to_datetime(df_hacim["date"].apply(lambda d: d[:10]))
        df_hacim.drop("date", axis=1, inplace=True)
        df_hacim.rename(index=str,
                        columns={"matchedOffers": "Arz Eşleşme Miktarı"},
                        inplace=True)
        df_hacim[org["Kısa Adı"]] = df_hacim["Arz Eşleşme Miktarı"]
        df_hacim = df_hacim[["Tarih", "Saat", org["Kısa Adı"]]]
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
        return df_hacim


def __organizasyonel_talep_hacim(baslangic_tarihi, bitis_tarihi, org):
    """
    İlgili tarih aralığı ve organizasyon için gün öncesi piyasası saatlik talep eşleşme miktar bilgisini vermektedir.

    Parametreler
    ------------
    baslangic_tarihi : %YYYY-%AA-%GG formatında başlangıç tarihi
    bitis_tarihi     : %YYYY-%AA-%GG formatında bitiş tarihi
    org              : dict formatında organizasyon EIC Kodu, Kısa Adı

    Geri Dönüş Değeri
    -----------------
    Organizasyonel Talep Eşleşme Miktarı (MWh)
    """
    try:
        resp = __requests.get(__transparency_url + "day-ahead-market-volume" +
                              "?startDate=" + baslangic_tarihi + "&endDate=" + bitis_tarihi + "&eic=" + org["EIC Kodu"],
                              headers=__headers, timeout=__param.__timeout)
        resp.raise_for_status()
        list_hacim = resp.json()["body"]["dayAheadMarketVolumeList"]
        df_hacim = __pd.DataFrame(list_hacim)
        df_hacim["Saat"] = df_hacim["date"].apply(lambda h: int(h[11:13]))
        df_hacim["Tarih"] = __pd.to_datetime(df_hacim["date"].apply(lambda d: d[:10]))
        df_hacim.rename(index=str,
                        columns={"matchedBids": "Talep Eşleşme Miktarı"},
                        inplace=True)
        df_hacim[org["Kısa Adı"]] = df_hacim["Talep Eşleşme Miktarı"]
        df_hacim = df_hacim[["Tarih", "Saat", org["Kısa Adı"]]]
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
        return df_hacim
