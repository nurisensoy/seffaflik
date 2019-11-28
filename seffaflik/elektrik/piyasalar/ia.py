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
from seffaflik.elektrik.uretim import organizasyonlar as __organizasyonlar

__transparency_url = __param.SEFFAFLIK_URL + "market/"
__headers = __api.HEADERS


def hacim(baslangic_tarihi=__dt.datetime.today().strftime("%Y-%m-%d"),
          bitis_tarihi=__dt.datetime.today().strftime("%Y-%m-%d"), organizasyon_eic=""):
    """
    İlgili tarih aralığı için ikili anlaşma arz/talep hacim bilgilerini vermektedir.
    Not: "organizasyon_eic" değeri girildiği taktirde organizasyona ait saatlik arz/talep hacim bilgisini vermektedir.

    Parametreler
    ------------
    baslangic_tarihi : %YYYY-%AA-%GG formatında başlangıç tarihi (Varsayılan: bugün)
    bitis_tarihi     : %YYYY-%AA-%GG formatında bitiş tarihi (Varsayılan: bugün)
    organizasyon_eic : metin formatında organizasyon eic kodu (Varsayılan: "")

    Geri Dönüş Değeri
    ----------------
    Arz/Talep İkili Anlaşma Miktarları (MWh)
    """
    if __dogrulama.__baslangic_bitis_tarih_eic_dogrulama(baslangic_tarihi, bitis_tarihi, organizasyon_eic):
        try:
            resp = __requests.get(__transparency_url + "bilateral-contract-sell" +
                                  "?startDate=" + baslangic_tarihi + "&endDate=" + bitis_tarihi + "&eic=" +
                                  organizasyon_eic, headers=__headers, timeout=__param.__timeout)
            resp.raise_for_status()
            list_arz = resp.json()["body"]["bilateralContractSellList"]
            resp = __requests.get(__transparency_url + "bilateral-contract-buy" +
                                  "?startDate=" + baslangic_tarihi + "&endDate=" + bitis_tarihi + "&eic=" +
                                  organizasyon_eic, headers=__headers, timeout=__param.__timeout)
            resp.raise_for_status()
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
    İlgili tarih aralığı ve hacim tipi için tüm organizasyonların saatlik ikili anlaşma hacim bilgilerini vermektedir.

    Parametreler
    ------------
    baslangic_tarihi : %YYYY-%AA-%GG formatında başlangıç tarihi (Varsayılan: bugün)
    bitis_tarihi     : %YYYY-%AA-%GG formatında bitiş tarihi (Varsayılan: bugün)
    hacim_tipi       : metin formatında hacim tipi ("NET", "ARZ", yada "TALEP") (varsayılan: "NET")

    Geri Dönüş Değeri
    -----------------
    Tüm Organizasyonların İA Hacim Bilgileri (Tarih, Saat, Hacim)
    """
    if __dogrulama.__baslangic_bitis_tarih_dogrulama(baslangic_tarihi, bitis_tarihi):
        list_org = __organizasyonlar()[["EIC Kodu", "Kısa Adı"]].to_dict("records")[:100]
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


def __organizasyonel_net_hacim(baslangic_tarihi, bitis_tarihi, org):
    """
    İlgili tarih aralığı ve organizasyon için saatlik ikili anlaşma net hacim bilgilerini vermektedir.

    Önemli Bilgi
    ------------
    Organizasyon bilgisi girilmediği taktirde toplam piyasa hacmi bilgisi verilmektedir.

    Parametreler
    -----------
    baslangic_tarihi: %YYYY-%AA-%GG formatında başlangıç tarihi
    bitis_tarihi    : %YYYY-%AA-%GG formatında bitiş tarihi
    org             : dict formatında organizasyon EIC Kodu, Kısa Adı

    Geri Dönüş Değeri
    -----------------
    Net İA Miktarı (MWh)
    """
    try:
        resp = __requests.get(__transparency_url + "bilateral-contract-sell" +
                              "?startDate=" + baslangic_tarihi + "&endDate=" + bitis_tarihi + "&eic=" + org["EIC Kodu"],
                              headers=__headers, timeout=__param.__timeout)
        resp.raise_for_status()
        list_arz = resp.json()["body"]["bilateralContractSellList"]
        resp = __requests.get(__transparency_url + "bilateral-contract-buy" +
                              "?startDate=" + baslangic_tarihi + "&endDate=" + bitis_tarihi + "&eic=" + org["EIC Kodu"],
                              headers=__headers, timeout=__param.__timeout)
        resp.raise_for_status()
        list_talep = resp.json()["body"]["bilateralContractBuyList"]
        df_arz = __pd.DataFrame(list_arz)
        df_talep = __pd.DataFrame(list_talep)
        df_hacim = __araclar.__merge_ia_dfs_evenif_empty(df_arz, df_talep)
        df_hacim["Saat"] = df_hacim["date"].apply(lambda h: int(h[11:13]))
        df_hacim["Tarih"] = __pd.to_datetime(df_hacim["date"].apply(lambda d: d[:10]))
        df_hacim[org["Kısa Adı"]] = df_hacim["Talep Miktarı"] - df_hacim["Arz Miktarı"]
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


def __organizasyonel_arz_hacim(baslangic_tarihi, bitis_tarihi, org):
    """
    İlgili tarih aralığı ve organizasyon için saatlik ikili anlaşma arz hacim bilgilerini vermektedir.

    Önemli Bilgi
    -----------
    Organizasyon bilgisi girilmediği taktirde toplam piyasa hacmi bilgisi verilmektedir.

    Parametreler
    ----------
    baslangic_tarihi: %YYYY-%AA-%GG formatında başlangıç tarihi
    bitis_tarihi    : %YYYY-%AA-%GG formatında bitiş tarihi
    org             : dict formatında organizasyon EIC Kodu, Kısa Adı

    Geri Dönüş Değeri
    -----------------
    Arz İA Miktarı (MWh)
    """
    try:
        resp = __requests.get(__transparency_url + "bilateral-contract-sell" +
                              "?startDate=" + baslangic_tarihi + "&endDate=" + bitis_tarihi + "&eic=" + org["EIC Kodu"],
                              headers=__headers, timeout=__param.__timeout)
        resp.raise_for_status()
        list_arz = resp.json()["body"]["bilateralContractSellList"]
        df_arz = __pd.DataFrame(list_arz)
        df_arz["Saat"] = df_arz["date"].apply(lambda h: int(h[11:13]))
        df_arz["Tarih"] = __pd.to_datetime(df_arz["date"].apply(lambda d: d[:10]))
        df_arz.rename(index=str, columns={"quantity": org["Kısa Adı"]}, inplace=True)
        df_arz = df_arz[["Tarih", "Saat", org["Kısa Adı"]]]
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
        return df_arz


def __organizasyonel_talep_hacim(baslangic_tarihi, bitis_tarihi, org):
    """
    İlgili tarih aralığı ve organizasyon için saatlik ikili anlaşma (İA) talep hacim bilgilerini vermektedir.

    Önemli Bilgi
    ------------
    Organizasyon bilgisi girilmediği taktirde toplam piyasa hacmi bilgisi verilmektedir.

    Parametreler
    ------------
    baslangic_tarihi: %YYYY-%AA-%GG formatında başlangıç tarihi
    bitis_tarihi    : %YYYY-%AA-%GG formatında bitiş tarihi
    org             : dict formatında organizasyon EIC Kodu, Kısa Adı

    Geri Dönüş Değeri
    ----------------
    Talep İA Miktarı (MWh)
    """
    try:
        resp = __requests.get(__transparency_url + "bilateral-contract-buy" +
                              "?startDate=" + baslangic_tarihi + "&endDate=" + bitis_tarihi + "&eic=" + org["EIC Kodu"],
                              headers=__headers, timeout=__param.__timeout)
        resp.raise_for_status()
        list_talep = resp.json()["body"]["bilateralContractBuyList"]
        df_talep = __pd.DataFrame(list_talep)
        df_talep["Saat"] = df_talep["date"].apply(lambda h: int(h[11:13]))
        df_talep["Tarih"] = __pd.to_datetime(df_talep["date"].apply(lambda d: d[:10]))
        df_talep.rename(index=str, columns={"quantity": org["Kısa Adı"]}, inplace=True)
        df_talep = df_talep[["Tarih", "Saat", org["Kısa Adı"]]]
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
        return df_talep
