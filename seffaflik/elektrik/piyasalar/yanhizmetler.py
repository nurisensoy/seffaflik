import requests as __requests
from requests import ConnectionError as __ConnectionError
from requests.exceptions import HTTPError as __HTTPError, RequestException as __RequestException, Timeout as __Timeout
import pandas as __pd
import datetime as __dt
import logging as __logging

from seffaflik.__ortak import __dogrulama as __dogrulama, __parametreler as __param, __anahtar as __api

__transparency_url = __param.SEFFAFLIK_URL + "market/"
__headers = __api.HEADERS


def primer_frekans_rezerv_miktari(baslangic_tarihi=__dt.datetime.today().strftime("%Y-%m-%d"),
                                  bitis_tarihi=__dt.datetime.today().strftime("%Y-%m-%d")):
    """
    İlgili tarih aralığı için saatlik primer frekans rezerv miktar bilgisini vermektedir.

    Parametreler
    ------------
    baslangic_tarihi : %YYYY-%AA-%GG formatında başlangıç tarihi (Varsayılan: bugün)
    bitis_tarihi     : %YYYY-%AA-%GG formatında bitiş tarihi (Varsayılan: bugün)

    Geri Dönüş Değeri
    -----------------
    PFK Yükümlülük (MWh)
    """
    if __dogrulama.__baslangic_bitis_tarih_dogrulama(baslangic_tarihi, bitis_tarihi):
        try:
            resp = __requests.get(
                __transparency_url + "pfc-amount" + "?startDate=" + baslangic_tarihi + "&endDate=" + bitis_tarihi,
                headers=__headers, timeout=__param.__timeout)
            resp.raise_for_status()
            list_pfk = resp.json()["body"]["frequencyReservePriceList"]
            df_pfk = __pd.DataFrame(list_pfk)
            df_pfk["Saat"] = df_pfk["effectiveDate"].apply(lambda h: int(h[11:13]))
            df_pfk["Tarih"] = __pd.to_datetime(df_pfk["effectiveDate"].apply(lambda d: d[:10]))
            df_pfk.rename(index=str, columns={"totalAmount": "PFK Yükümlülük (MWh)"}, inplace=True)
            df_pfk = df_pfk[["Tarih", "Saat", "PFK Yükümlülük (MWh)"]]
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
            return df_pfk


def sekonder_frekans_rezerv_miktari(baslangic_tarihi=__dt.datetime.today().strftime("%Y-%m-%d"),
                                    bitis_tarihi=__dt.datetime.today().strftime("%Y-%m-%d")):
    """
    İlgili tarih aralığı için saatlik primer frekans rezerv miktar bilgisini vermektedir.

    Parametreler
    ------------
    baslangic_tarihi : %YYYY-%AA-%GG formatında başlangıç tarihi (Varsayılan: bugün)
    bitis_tarihi     : %YYYY-%AA-%GG formatında bitiş tarihi (Varsayılan: bugün)

    Geri Dönüş Değeri
    -----------------
    SFK Yükümlülük (MWh)
    """
    if __dogrulama.__baslangic_bitis_tarih_dogrulama(baslangic_tarihi, bitis_tarihi):
        try:
            resp = __requests.get(
                __transparency_url + "sfc-amount" + "?startDate=" + baslangic_tarihi + "&endDate=" + bitis_tarihi,
                headers=__headers, timeout=__param.__timeout)
            resp.raise_for_status()
            list_sfk = resp.json()["body"]["frequencyReservePriceList"]
            df_sfk = __pd.DataFrame(list_sfk)
            df_sfk["Saat"] = df_sfk["effectiveDate"].apply(lambda h: int(h[11:13]))
            df_sfk["Tarih"] = __pd.to_datetime(df_sfk["effectiveDate"].apply(lambda d: d[:10]))
            df_sfk.rename(index=str, columns={"totalAmount": "SFK Yükümlülük (MWh)"}, inplace=True)
            df_sfk = df_sfk[["Tarih", "Saat", "SFK Yükümlülük (MWh)"]]
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
            return df_sfk


def primer_rezerv_fiyati(baslangic_tarihi=__dt.datetime.today().strftime("%Y-%m-%d"),
                         bitis_tarihi=__dt.datetime.today().strftime("%Y-%m-%d")):
    """
    İlgili tarih aralığı için saatlik primer frekans kontrol (PFK) kapasite bedeli bilgisini vermektedir.

    Parametreler
    ------------
    baslangic_tarihi : %YYYY-%AA-%GG formatında başlangıç tarihi (Varsayılan: bugün)
    bitis_tarihi     : %YYYY-%AA-%GG formatında bitiş tarihi (Varsayılan: bugün)

    Geri Dönüş Değeri
    -----------------
    PFK Fiyat (TL/MWh)
    """
    if __dogrulama.__baslangic_bitis_tarih_dogrulama(baslangic_tarihi, bitis_tarihi):
        try:
            resp = __requests.get(
                __transparency_url + "pfc-price" + "?startDate=" + baslangic_tarihi + "&endDate=" + bitis_tarihi,
                headers=__headers, timeout=__param.__timeout)
            resp.raise_for_status()
            list_pfk = resp.json()["body"]["frequencyReservePriceList"]
            df_pfk = __pd.DataFrame(list_pfk)
            df_pfk["Saat"] = df_pfk["effectiveDate"].apply(lambda h: int(h[11:13]))
            df_pfk["Tarih"] = __pd.to_datetime(df_pfk["effectiveDate"].apply(lambda d: d[:10]))
            df_pfk.rename(index=str, columns={"price": "PFK Fiyat (TL/MWh)"}, inplace=True)
            df_pfk = df_pfk[["Tarih", "Saat", "PFK Fiyat (TL/MWh)"]]
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
            return df_pfk


def sekonder_rezerv_fiyati(baslangic_tarihi=__dt.datetime.today().strftime("%Y-%m-%d"),
                           bitis_tarihi=__dt.datetime.today().strftime("%Y-%m-%d")):
    """
    İlgili tarih aralığı için saatlik sekonder frekans kontrol (SFK) kapasite bedeli bilgisini vermektedir.

    Parametreler
    ------------
    baslangic_tarihi : %YYYY-%AA-%GG formatında başlangıç tarihi (Varsayılan: bugün)
    bitis_tarihi     : %YYYY-%AA-%GG formatında bitiş tarihi (Varsayılan: bugün)

    Geri Dönüş Değeri
    -----------------
    SFK Fiyat (TL/MWh)
    """
    if __dogrulama.__baslangic_bitis_tarih_dogrulama(baslangic_tarihi, bitis_tarihi):
        try:
            resp = __requests.get(
                __transparency_url + "sfc-price" + "?startDate=" + baslangic_tarihi + "&endDate=" + bitis_tarihi,
                headers=__headers, timeout=__param.__timeout)
            resp.raise_for_status()
            list_pfk = resp.json()["body"]["frequencyReservePriceList"]
            df_pfk = __pd.DataFrame(list_pfk)
            df_pfk["Saat"] = df_pfk["effectiveDate"].apply(lambda h: int(h[11:13]))
            df_pfk["Tarih"] = __pd.to_datetime(df_pfk["effectiveDate"].apply(lambda d: d[:10]))
            df_pfk.rename(index=str, columns={"price": "SFK Fiyat (TL/MWh)"}, inplace=True)
            df_pfk = df_pfk[["Tarih", "Saat", "SFK Fiyat (TL/MWh)"]]
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
            return df_pfk
