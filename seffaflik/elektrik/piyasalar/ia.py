import requests as __requests
from requests import ConnectionError as __ConnectionError
from requests.exceptions import HTTPError as __HTTPError, RequestException as __RequestException, Timeout as __Timeout
import pandas as __pd
import datetime as __dt
from multiprocessing import Pool as __Pool
import multiprocessing as __mp
from functools import reduce as __red
import logging as __logging

from seffaflik.ortak import araclar as __araclar, dogrulama as __dogrulama, parametreler as __param, anahtar as __api
from seffaflik.elektrik.uretim.uretim import organizasyonlar as __organizasyonlar

__transparency_url = __param.SEFFAFLIK_URL + "market/"
__headers = __api.HEADERS


def hacim(baslangic_tarihi=__dt.datetime.today().strftime("%Y-%m-%d"),
          bitis_tarihi=__dt.datetime.today().strftime("%Y-%m-%d")):
    """
    İlgili tarih aralığı için ikili anlaşma hacim bilgilerini vermektedir.

    Parametreler
    ----------
    baslangic_tarihi : %YYYY-%AA-%GG formatında başlangıç tarihi (Varsayılan: bugün)
    bitis_tarihi   : %YYYY-%AA-%GG formatında bitiş tarihi (Varsayılan: bugün)

    Geri Dönüş Değeri
    ------
    Arz/Talep Miktarı (MWh)
    """
    if __dogrulama.__baslangic_bitis_tarih_dogrulama(baslangic_tarihi, bitis_tarihi):
        try:
            resp = __requests.get(__transparency_url + "bilateral-contract-all" +
                                  "?startDate=" + baslangic_tarihi + "&endDate=" + bitis_tarihi,
                                  headers=__headers, timeout=__param.__timeout)
            list_hacim = resp.json()["body"]["bilateralContracts"]
            df_hacim = __pd.DataFrame(list_hacim)
            df_hacim["Saat"] = df_hacim["date"].apply(lambda h: int(h[11:13]))
            df_hacim["Tarih"] = __pd.to_datetime(df_hacim["date"].apply(lambda d: d[:10]))
            df_hacim.rename(index=str,
                            columns={"quantityBid": "Arz Miktarı", "quantityBidAsk": "Talep Miktarı"},
                            inplace=True)
            df_hacim = df_hacim[["Tarih", "Saat", "Talep Miktarı", "Arz Miktarı"]]
        except __ConnectionError:
            __logging.error(__param.__requestsConnectionErrorLogging, exc_info=False)
        except __Timeout:
            __logging.error(__param.__requestsTimeoutErrorLogging, exc_info=False)
        except __HTTPError as e:
            __dogrulama.__check_HTTPError(e.response.status_code)
        except __RequestException:
            __logging.error(__param.__request_error, exc_info=False)
        except KeyError:
            return __pd.DataFrame()
        else:
            return df_hacim


def organizasyonel_hacim(baslangic_tarihi=__dt.datetime.today().strftime("%Y-%m-%d"),
                         bitis_tarihi=__dt.datetime.today().strftime("%Y-%m-%d"), eic=""):
    """
    İlgili tarih aralığı ve organizasyon için ikili anlaşma hacim bilgilerini vermektedir.

    Önemli Bilgi
    ----------
    Organizasyon bilgisi girilmediği taktirde toplam piyasa hacmi bilgisi verilmektedir.

    Parametreler
    ----------
    baslangic_tarihi: %YYYY-%AA-%GG formatında başlangıç tarihi (Varsayılan: bugün)
    bitis_tarihi    : %YYYY-%AA-%GG formatında bitiş tarihi (Varsayılan: bugün)
    eic             : Organizasyonlara verilmiş olunan EIC kodu (Varsayılan: boş)

    Geri Dönüş Değeri
    ------
    Arz/Talep Miktarı (MWh)
    """
    if __dogrulama.__baslangic_bitis_tarih_dogrulama(baslangic_tarihi, bitis_tarihi):
        try:
            resp = __requests.get(__transparency_url + "bilateral-contract-sell" +
                                  "?startDate=" + baslangic_tarihi + "&endDate=" + bitis_tarihi + "&eic=" + eic,
                                  headers=__headers, timeout = __param.__timeout)
            list_arz = resp.json()["body"]["bilateralContractSellList"]
            resp = __requests.get(__transparency_url + "bilateral-contract-buy" +
                                  "?startDate=" + baslangic_tarihi + "&endDate=" + bitis_tarihi + "&eic=" + eic,
                                  headers=__headers, timeout = __param.__timeout)
            list_talep = resp.json()["body"]["bilateralContractBuyList"]
            df_arz = __pd.DataFrame(list_arz)
            df_talep = __pd.DataFrame(list_talep)
            df_hacim = __araclar.__merge_ia_dfs_evenif_empty(df_arz, df_talep)
            df_hacim["Saat"] = df_hacim["date"].apply(lambda h: int(h[11:13]))
            df_hacim["Tarih"] = __pd.to_datetime(df_hacim["date"].apply(lambda d: d[:10]))
            df_hacim = df_hacim[["Tarih", "Saat", "Talep Miktarı", "Arz Miktarı"]]
        except __ConnectionError:
            __logging.error(__param.__requestsConnectionErrorLogging, exc_info=False)
        except __Timeout:
            __logging.error(__param.__requestsTimeoutErrorLogging, exc_info=False)
        except __HTTPError as e:
            __dogrulama.__check_HTTPError(e.response.status_code)
        except __RequestException:
            __logging.error(__param.__request_error, exc_info=False)
        except KeyError:
            return __pd.DataFrame()
        else:
            return df_hacim


def tum_organizasyonlar_hacim(baslangic_tarihi=__dt.datetime.today().strftime("%Y-%m-%d"),
                              bitis_tarihi=__dt.datetime.today().strftime("%Y-%m-%d")):
    """
    İlgili tarih aralığı için tüm organizasyonların saatlik net ikili anlaşma hacim bilgilerini vermektedir.

    Parametreler
    ------------
    baslangic_tarihi : %YYYY-%AA-%GG formatında başlangıç tarihi (Varsayılan: bugün)
    bitis_tarihi     : %YYYY-%AA-%GG formatında bitiş tarihi (Varsayılan: bugün)

    Geri Dönüş Değeri
    -----------------
    Tüm Organizasyonların net İA Hacim Bilgileri (Tarih, Saat, Hacim)
    """
    if __dogrulama.__baslangic_bitis_tarih_dogrulama(baslangic_tarihi, bitis_tarihi):
        list_org_eic = list(__organizasyonlar()["EIC Kodu"])
        org_len = len(list_org_eic)
        list_date_org_eic = list(zip([baslangic_tarihi] * org_len, [bitis_tarihi] * org_len, list_org_eic))
        list_date_org_eic = list(map(list, list_date_org_eic))
        p = __Pool(__mp.cpu_count())
        list_df_unit = p.starmap(__organizasyonel_hacim, list_date_org_eic)
        list_df_unit = list(filter(lambda x: len(x) > 0, list_df_unit))
        return __red(lambda left, right: __pd.merge(left, right, how="outer", on=["Tarih", "Saat"], sort=True),
                     list_df_unit)


def __organizasyonel_hacim(baslangic_tarihi=__dt.datetime.today().strftime("%Y-%m-%d"),
                           bitis_tarihi=__dt.datetime.today().strftime("%Y-%m-%d"), eic=""):
    """
    İlgili tarih aralığı ve organizasyon için ikili anlaşma hacim bilgilerini vermektedir.

    Önemli Bilgi
    ----------
    Organizasyon bilgisi girilmediği taktirde toplam piyasa hacmi bilgisi verilmektedir.

    Parametreler
    ----------
    baslangic_tarihi: %YYYY-%AA-%GG formatında başlangıç tarihi (Varsayılan: bugün)
    bitis_tarihi    : %YYYY-%AA-%GG formatında bitiş tarihi (Varsayılan: bugün)
    eic             : Organizasyonlara verilmiş olunan EIC kodu (Varsayılan: boş)

    Geri Dönüş Değeri
    ------
    Net İA Miktarı (MWh)
    """
    if __dogrulama.__baslangic_bitis_tarih_dogrulama(baslangic_tarihi, bitis_tarihi):
        try:
            resp = __requests.get(__transparency_url + "bilateral-contract-sell" +
                                  "?startDate=" + baslangic_tarihi + "&endDate=" + bitis_tarihi + "&eic=" + eic,
                                  headers=__headers, timeout=__param.__timeout)
            list_arz = resp.json()["body"]["bilateralContractSellList"]
            resp = __requests.get(__transparency_url + "bilateral-contract-buy" +
                                  "?startDate=" + baslangic_tarihi + "&endDate=" + bitis_tarihi + "&eic=" + eic,
                                  headers=__headers,timeout=__param.__timeout)
            list_talep = resp.json()["body"]["bilateralContractBuyList"]
            df_arz = __pd.DataFrame(list_arz)
            df_talep = __pd.DataFrame(list_talep)
            df_hacim = __araclar.__merge_ia_dfs_evenif_empty(df_arz, df_talep)
            df_hacim["Saat"] = df_hacim["date"].apply(lambda h: int(h[11:13]))
            df_hacim["Tarih"] = __pd.to_datetime(df_hacim["date"].apply(lambda d: d[:10]))
            df_hacim[eic] = df_hacim["Talep Miktarı"] - df_hacim["Arz Miktarı"]
            df_hacim = df_hacim[["Tarih", "Saat", eic]]
        except __ConnectionError:
            __logging.error(__param.__requestsConnectionErrorLogging, exc_info=False)
        except __Timeout:
            __logging.error(__param.__requestsTimeoutErrorLogging, exc_info=False)
        except __HTTPError as e:
            __dogrulama.__check_HTTPError(e.response.status_code)
        except __RequestException:
            __logging.error(__param.__request_error, exc_info=False)
        except KeyError:
            return __pd.DataFrame()
        else:
            return df_hacim
