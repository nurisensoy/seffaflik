import requests as __requests
from requests import ConnectionError as __ConnectionError
from requests.exceptions import HTTPError as __HTTPError, RequestException as __RequestException, Timeout as __Timeout
import pandas as __pd
import datetime as __dt
from multiprocessing import Pool as __Pool
import multiprocessing as __mp
from dateutil import relativedelta as __rd
from functools import reduce as __red
import logging as __logging

from seffaflik.elektrik.piyasalar import ia as __ia, gop as __gop, gip as __gip, dgp as __dgp
from seffaflik.ortak import dogrulama as __dogrulama, parametreler as __param, anahtar as __api

__transparency_url = __param.SEFFAFLIK_URL + "market/"
__headers = __api.HEADERS


def katilimci_sayisi(baslangic_tarihi=__dt.datetime.today().strftime("%Y-%m-%d"),
                     bitis_tarihi=__dt.datetime.today().strftime("%Y-%m-%d")):
    """
    İlgili tarih aralığına tekabül eden dönemler için EPİAŞ sistemine kayıtlı katılımcıların lisans tipine göre sayısını
    vermektedir.

    Parametreler
    ------------
    baslangic_tarihi : %YYYY-%AA-%GG formatında başlangıç tarihi (Varsayılan: bugün)
    bitis_tarihi     : %YYYY-%AA-%GG formatında bitiş tarihi (Varsayılan: bugün)

    Geri Dönüş Değeri
    -----------------
    Katılımcı Sayısı
    """
    if __dogrulama.__baslangic_bitis_tarih_dogrulama(baslangic_tarihi, bitis_tarihi):
        ilk = __dt.datetime.strptime(baslangic_tarihi[:7], '%Y-%m')
        son = __dt.datetime.strptime(bitis_tarihi[:7], '%Y-%m')
        date_list = []
        while ilk <= son:
            date_list.append(ilk.strftime("%Y-%m-%d"))
            ilk = ilk + __rd.relativedelta(months=+1)
        with __Pool(__mp.cpu_count()) as p:
            df_list = p.map(__katilimci_sayisi, date_list)
        return __pd.concat(df_list, sort=False)


def hacim(baslangic_tarihi=__dt.datetime.today().strftime("%Y-%m-%d"),
          bitis_tarihi=__dt.datetime.today().strftime("%Y-%m-%d"), periyot="gunluk"):
    """
    İlgili tarih aralığı için GÖP, GİP, DGP, İA hacim bilgilerini vermektedir.

    Parametreler
    ------------
    baslangic_tarihi : %YYYY-%AA-%GG formatında başlangıç tarihi (Varsayılan: bugün)
    bitis_tarihi     : %YYYY-%AA-%GG formatında bitiş tarihi (Varsayılan: bugün)
    periyot          : metin formatında periyot(saatlik, günlük, aylik, yillik) (Varsayılan: "günlük")

    Geri Dönüş Değeri
    -----------------
    GÖP, GİP, DGP, İA Hacimleri (MWh)
    """
    if __dogrulama.__baslangic_bitis_tarih__periyot_dogrulama(baslangic_tarihi, bitis_tarihi, periyot):
        if periyot.lower() == "saatlik":
            df_ia = __ia.hacim(baslangic_tarihi, bitis_tarihi)[["Tarih", "Saat", "Arz Miktarı"]]
            df_ia.rename(index=str, columns={"Arz Miktarı": "İA"}, inplace=True)
            df_gop = __gop.hacim(baslangic_tarihi, bitis_tarihi)[["Tarih", "Saat", "Eşleşme Miktarı"]]
            df_gop.rename(index=str, columns={"Eşleşme Miktarı": "GÖP"}, inplace=True)
            df_gip = __gip.hacim(baslangic_tarihi, bitis_tarihi)[["Tarih", "Saat", "Eşleşme Miktarı"]]
            df_gip.rename(index=str, columns={"Eşleşme Miktarı": "GİP"}, inplace=True)
            df_dgp = __dgp.hacim(baslangic_tarihi, bitis_tarihi)
            df_dgp["Toplam"] = df_dgp["Teslim Edilen YAL"] - df_dgp["Teslim Edilen YAT"]
            df_dgp = df_dgp[["Tarih", "Saat", "Toplam"]]
            df_dgp.rename(index=str, columns={"Toplam": "DGP"}, inplace=True)
            df_hacim = __red(lambda left, right: __pd.merge(left, right, on=["Tarih", "Saat"]),
                             [df_ia, df_gop, df_gip, df_dgp])
            return df_hacim
        else:
            periods = {"gunluk": "DAILY", "aylik": "MONTHLY", "yillik": "YEAR"}
            try:
                resp = __requests.get(
                    __transparency_url + "market-volume" + "?startDate=" + baslangic_tarihi + "&endDate=" + bitis_tarihi
                    + "&period=" + periods[periyot.lower()], headers=__headers, timeout=__param.__timeout)
                resp.raise_for_status()
                list_hacim = resp.json()["body"]["marketVolumeList"]
                df_hacim = __pd.DataFrame(list_hacim)
                df_hacim["Tarih"] = __pd.to_datetime(df_hacim["date"].apply(lambda d: d[:10]))
                df_hacim.rename(index=str,
                                columns={"bilateralContractAmount": "İA Miktarı", "dayAheadMarketVolume": "GÖP Mİktarı",
                                         "intradayVolume": "GİP Mİktarı", "balancedPowerMarketVolume": "DGP Miktarı"},
                                inplace=True)
                df_hacim = df_hacim[["Tarih", "İA Miktarı", "GÖP Mİktarı", "GİP Mİktarı", "DGP Miktarı"]]
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


def fiyat(baslangic_tarihi=__dt.datetime.today().strftime("%Y-%m-%d"),
          bitis_tarihi=__dt.datetime.today().strftime("%Y-%m-%d"), periyot="saatlik"):
    """
    İlgili tarih aralığı için saatlik GÖP, GİP, DGP fiyat bilgilerini vermektedir.

    Parametreler
    ------------
    baslangic_tarihi : %YYYY-%AA-%GG formatında başlangıç tarihi (Varsayılan: bugün)
    bitis_tarihi     : %YYYY-%AA-%GG formatında bitiş tarihi (Varsayılan: bugün)
    periyot          : metin formatında periyot(saatlik, günlük, aylik, yillik) (Varsayılan: "günlük")

    Geri Dönüş Değeri
    -----------------
    GÖP, GİP, DGP Fiyatları (MWh)
    """
    if __dogrulama.__baslangic_bitis_tarih__periyot_dogrulama(baslangic_tarihi, bitis_tarihi, periyot):
        if periyot.lower() == "saatlik":
            df_gop = __gop.ptf(baslangic_tarihi, bitis_tarihi)
            df_gip = __gip.aof(baslangic_tarihi, bitis_tarihi)
            df_dgp = __dgp.smf(baslangic_tarihi, bitis_tarihi)[["Tarih", "Saat", "SMF"]]
            df_fiyat = __red(lambda left, right: __pd.merge(left, right, on=["Tarih", "Saat"]),
                             [df_gop, df_gip, df_dgp])
            return df_fiyat
        else:
            periods = {"gunluk": "DAILY", "aylik": "MONTHLY", "yillik": "YEAR"}
            try:
                resp = __requests.get(
                    __transparency_url + "market-volume" + "?startDate=" + baslangic_tarihi + "&endDate=" + bitis_tarihi
                    + "&period=" + periods[periyot.lower()], headers=__headers, timeout=__param.__timeout)
                resp.raise_for_status()
                list_hacim = resp.json()["body"]["marketVolumeList"]
                df_hacim = __pd.DataFrame(list_hacim)
                df_hacim["Tarih"] = __pd.to_datetime(df_hacim["date"].apply(lambda d: d[:10]))
                df_hacim.rename(index=str,
                                columns={"bilateralContractAmount": "İA Miktarı", "dayAheadMarketVolume": "GÖP Mİktarı",
                                         "intradayVolume": "GİP Mİktarı", "balancedPowerMarketVolume": "DGP Miktarı"},
                                inplace=True)
                df_hacim = df_hacim[["Tarih", "İA Miktarı", "GÖP Mİktarı", "GİP Mİktarı", "DGP Miktarı"]]
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


def __katilimci_sayisi(tarih):
    """
    İlgili tarih için EPİAŞ sistemine kayıtlı katılımcıların lisans tipine göre sayısını vermektedir.

    Parametre
    ----------
    tarih : %YYYY-%AA-%GG formatında tarih (Varsayılan: bugün)

    Geri Dönüş Değeri
    -----------------
    Katılımcı Sayısı
    """
    try:
        resp = __requests.get(__transparency_url + "participant?period=" + tarih, headers=__headers,
                              timeout=__param.__timeout)
        resp.raise_for_status()
        list_katilimci = resp.json()["body"]["participantList"]
        df_katilimci = __pd.DataFrame(list_katilimci)
        columns = __pd.MultiIndex.from_product(
            [["Özel Sektör", "Kamu Kuruluşu"], list(df_katilimci["licence"]) + ["Toplam"]], names=['', ''])
        df_katilimci = __pd.DataFrame([list(df_katilimci["privateSector"]) + list(
            df_katilimci["privateSectorOfSum"].unique()) + list(df_katilimci["publicCompany"]) + list(
            df_katilimci["publicCompanyOfSum"].unique())], index=[tarih], columns=columns)
        df_katilimci["Toplam"] = df_katilimci["Kamu Kuruluşu"]["Toplam"] + df_katilimci["Özel Sektör"]["Toplam"]
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
        return df_katilimci
