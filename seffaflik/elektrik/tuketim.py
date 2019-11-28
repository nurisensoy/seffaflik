import requests as __requests
from requests import ConnectionError as __ConnectionError
from requests.exceptions import HTTPError as __HTTPError, RequestException as __RequestException, Timeout as __Timeout
import pandas as __pd
import datetime as __dt
from dateutil import relativedelta as __rd
from multiprocessing import Pool as __Pool
import multiprocessing as __mp
from functools import reduce as __red
import logging as __logging

from seffaflik.ortak import dogrulama as __dogrulama, parametreler as __param, anahtar as __api

__transparency_url = __param.SEFFAFLIK_URL + "consumption/"
__headers = __api.HEADERS


def sehir():
    """
    Şehir ve şehirlere ait ilçelerin bilgisini vermektedir.

    Parametreler
    ------------

    Geri Dönüş Değeri
    -----------------
    Şehir ve Şehirlere Ait İlçeler (Şehir Id, İlçe Id, Şehir İsmi, İlçe İsmi)

    """
    try:
        resp = __requests.get(
            __transparency_url + "city", headers=__headers, timeout=__param.__timeout)
        resp.raise_for_status()
        list_sehir = resp.json()["body"]["cityList"]
        df_sehir = __pd.DataFrame(list_sehir)
        df_sehir.rename(index=str, columns={"cityId": "Şehir Id", "districtId": "İlçe Id", "cityName": "Şehir İsmi",
                                            "districtName": "İlçe İsmi"}, inplace=True)
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
        return df_sehir.drop_duplicates().reset_index(drop=True)


def gerceklesen(baslangic_tarihi=__dt.datetime.today().strftime("%Y-%m-%d"),
                bitis_tarihi=__dt.datetime.today().strftime("%Y-%m-%d")):
    """
    İlgili tarih aralığı için saatlik gerçek zamanlı tüketim bilgisini vermektedir.

    Parametreler
    ------------
    baslangic_tarihi : %YYYY-%AA-%GG formatında başlangıç tarihi (Varsayılan: bugün)
    bitis_tarihi     : %YYYY-%AA-%GG formatında bitiş tarihi (Varsayılan: bugün)

    Geri Dönüş Değeri
    -----------------
    Gerçek Zamanlı Tüketim (Tarih, Saat, Tüketim)

    """
    if __dogrulama.__baslangic_bitis_tarih_dogrulama(baslangic_tarihi, bitis_tarihi):
        try:
            resp = __requests.get(
                __transparency_url + "real-time-consumption" + "?startDate=" + baslangic_tarihi +
                "&endDate=" + bitis_tarihi, headers=__headers, timeout=__param.__timeout)
            resp.raise_for_status()
            list_tuketim = resp.json()["body"]["hourlyConsumptions"]
            df_tuketim = __pd.DataFrame(list_tuketim)
            df_tuketim["Saat"] = df_tuketim["date"].apply(lambda h: int(h[11:13]))
            df_tuketim["Tarih"] = __pd.to_datetime(df_tuketim["date"].apply(lambda d: d[:10]))
            df_tuketim.rename(index=str, columns={"consumption": "Tüketim"}, inplace=True)
            df_tuketim = df_tuketim[["Tarih", "Saat", "Tüketim"]]
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
            return df_tuketim


def uecm(baslangic_tarihi=__dt.datetime.today().strftime("%Y-%m-%d"),
         bitis_tarihi=__dt.datetime.today().strftime("%Y-%m-%d")):
    """
    İlgili tarih aralığı için saatlik Uzlaştırmaya Esas Çekiş Miktarı (UEÇM) bilgisini vermektedir.

    Parametreler
    ------------
    baslangic_tarihi : %YYYY-%AA-%GG formatında başlangıç tarihi (Varsayılan: bugün)
    bitis_tarihi     : %YYYY-%AA-%GG formatında bitiş tarihi (Varsayılan: bugün)

    Geri Dönüş Değeri
    -----------------
    Uzlaştırmaya Esas Çekiş Miktarı (Tarih, Saat, UEÇM)

    """
    if __dogrulama.__baslangic_bitis_tarih_dogrulama(baslangic_tarihi, bitis_tarihi):
        try:
            resp = __requests.get(
                __transparency_url + "swv" + "?startDate=" + baslangic_tarihi +
                "&endDate=" + bitis_tarihi, headers=__headers, timeout=__param.__timeout)
            resp.raise_for_status()
            list_uecm = resp.json()["body"]["swvList"]
            df_uecm = __pd.DataFrame(list_uecm)
            df_uecm["Saat"] = df_uecm["date"].apply(lambda h: int(h[11:13]))
            df_uecm["Tarih"] = __pd.to_datetime(df_uecm["date"].apply(lambda d: d[:10]))
            df_uecm.rename(index=str, columns={"swv": "UEÇM"}, inplace=True)
            df_uecm = df_uecm[["Tarih", "Saat", "UEÇM"]]
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
            return df_uecm


def uecm_donemlik(baslangic_tarihi=__dt.datetime.today().strftime("%Y-%m-%d"),
                  bitis_tarihi=__dt.datetime.today().strftime("%Y-%m-%d")):
    """
    İlgili tarih aralığına tekabül eden uzlaştırma dönemleri için serbest tüketici hakkını kullanan serbest
    tüketicilerin, tedarik yükümlülüğü kapsamındaki ve toplam Uzlaştırmaya Esas Çekiş Miktarı (UEÇM) bilgisini
    vermektedir.

    Parametreler
    ------------
    baslangic_tarihi : %YYYY-%AA-%GG formatında başlangıç tarihi (Varsayılan: bugün)
    bitis_tarihi     : %YYYY-%AA-%GG formatında bitiş tarihi (Varsayılan: bugün)

    Geri Dönüş Değeri
    -----------------
    Serbest Tüketici, Tedarik Kapsamındaki ve Toplam Uzlaştırmaya Esas Çekiş Miktarı (Tarih, Saat, UEÇM,
    Serbest Tüketici UEÇM, Tedarik Yükümlülüğü Kapsamındaki UEÇM)
    """
    if __dogrulama.__baslangic_bitis_tarih_dogrulama(baslangic_tarihi, bitis_tarihi):
        ilk = __dt.datetime.strptime(baslangic_tarihi[:7], '%Y-%m')
        son = __dt.datetime.strptime(bitis_tarihi[:7], '%Y-%m')
        date_list = []
        while ilk <= son:
            date_list.append(ilk.strftime("%Y-%m-%d"))
            ilk = ilk + __rd.relativedelta(months=+1)
        with __Pool(__mp.cpu_count()) as p:
            df_list = p.map(__uecm_donemlik, date_list, chunksize=1)
        return __pd.concat(df_list, sort=False)


def uecm_serbest_tuketici(baslangic_tarihi=__dt.datetime.today().strftime("%Y-%m-%d"),
                          bitis_tarihi=__dt.datetime.today().strftime("%Y-%m-%d")):
    """
    İlgili tarih aralığına tekabül eden uzlaştırma dönemleri için serbest tüketici hakkını kullanan serbest
    tüketicilerin saatlik Uzlaştırmaya Esas Çekiş Miktarı (UEÇM) bilgisini vermektedir.

    Parametreler
    ------------
    baslangic_tarihi : %YYYY-%AA-%GG formatında başlangıç tarihi (Varsayılan: bugün)
    bitis_tarihi     : %YYYY-%AA-%GG formatında bitiş tarihi (Varsayılan: bugün)

    Geri Dönüş Değeri
    -----------------
    Serbest Tüketici UEÇM (Tarih, Saat, Tüketim)
    """
    if __dogrulama.__baslangic_bitis_tarih_dogrulama(baslangic_tarihi, bitis_tarihi):
        ilk = __dt.datetime.strptime(baslangic_tarihi[:7], '%Y-%m')
        son = __dt.datetime.strptime(bitis_tarihi[:7], '%Y-%m')
        date_list = []
        while ilk <= son:
            date_list.append(ilk.strftime("%Y-%m-%d"))
            ilk = ilk + __rd.relativedelta(months=+1)
        with __Pool(__mp.cpu_count()) as p:
            df_list = p.map(__uecm_serbest_tuketici, date_list, chunksize=1)
        return __pd.concat(df_list, sort=False)


def uecm_donemlik_tedarik(baslangic_tarihi=__dt.datetime.today().strftime("%Y-%m-%d"),
                          bitis_tarihi=__dt.datetime.today().strftime("%Y-%m-%d")):
    """
    İlgili tarih aralığına tekabül eden uzlaştırma dönemleri için tedarik yükümlülüğü kapsamındaki dönemlik bazlı toplam
    Uzlaştırmaya Esas Çekiş Miktarı (UEÇM) bilgisini vermektedir.

    Parametreler
    ------------
    baslangic_tarihi : %YYYY-%AA-%GG formatında başlangıç tarihi (Varsayılan: bugün)
    bitis_tarihi     : %YYYY-%AA-%GG formatında bitiş tarihi (Varsayılan: bugün)

    Geri Dönüş Değeri
    -----------------
    Tedarik Yükümlülüğü Kapsamındaki UEÇM (Dönem, Tüketim)
    """
    if __dogrulama.__baslangic_bitis_tarih_dogrulama(baslangic_tarihi, bitis_tarihi):
        ilk = __dt.datetime.strptime(baslangic_tarihi[:7], '%Y-%m')
        son = __dt.datetime.strptime(bitis_tarihi[:7], '%Y-%m')
        date_list = []
        while ilk <= son:
            date_list.append(ilk.strftime("%Y-%m-%d"))
            ilk = ilk + __rd.relativedelta(months=+1)
        with __Pool(__mp.cpu_count()) as p:
            df_list = p.map(__uecm_tedarik, date_list, chunksize=1)
        return __pd.concat(df_list, sort=False)


def tahmin(baslangic_tarihi=__dt.datetime.today().strftime("%Y-%m-%d"),
           bitis_tarihi=__dt.datetime.today().strftime("%Y-%m-%d")):
    """
    İlgili tarih aralığı için saatlik yük tahmin plan bilgisini vermektedir.

    Parametreler
    ------------
    baslangic_tarihi : %YYYY-%AA-%GG formatında başlangıç tarihi (Varsayılan: bugün)
    bitis_tarihi     : %YYYY-%AA-%GG formatında bitiş tarihi (Varsayılan: bugün)

    Geri Dönüş Değeri
    -----------------
    Yük Tahmin Planı (Tarih, Saat, Tüketim)

    """
    if __dogrulama.__baslangic_bitis_tarih_dogrulama(baslangic_tarihi, bitis_tarihi):
        try:
            resp = __requests.get(
                __transparency_url + "load-estimation-plan" + "?startDate=" + baslangic_tarihi +
                "&endDate=" + bitis_tarihi, headers=__headers, timeout=__param.__timeout)
            resp.raise_for_status()
            list_tuketim = resp.json()["body"]["loadEstimationPlanList"]
            df_tuketim = __pd.DataFrame(list_tuketim)
            df_tuketim["Saat"] = df_tuketim["date"].apply(lambda h: int(h[11:13]))
            df_tuketim["Tarih"] = __pd.to_datetime(df_tuketim["date"].apply(lambda d: d[:10]))
            df_tuketim.rename(index=str, columns={"lep": "Tüketim"}, inplace=True)
            df_tuketim = df_tuketim[["Tarih", "Saat", "Tüketim"]]
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
            return df_tuketim


def serbest_tuketici_sayisi(baslangic_tarihi=__dt.datetime.today().strftime("%Y-%m-%d"),
                            bitis_tarihi=__dt.datetime.today().strftime("%Y-%m-%d")):
    """
    İlgili tarih aralığına tekabül eden uzlaştırma dönemleri için profil abone grubuna göre serbest tüketici hakkını
    kullanan serbest tüketici sayıları bilgisini vermektedir.

    Parametreler
    ------------
    baslangic_tarihi : %YYYY-%AA-%GG formatında başlangıç tarihi (Varsayılan: bugün)
    bitis_tarihi     : %YYYY-%AA-%GG formatında bitiş tarihi (Varsayılan: bugün)

    Geri Dönüş Değeri
    -----------------
    Profil Abone Grubuna Göre Serbest Tüketici Sayıları (Tarih, Aydınlatma,  Diğer,  Mesken,  Sanayi,  Tarimsal,
    Sulama, Ticarethane, Toplam)
    """
    if __dogrulama.__baslangic_bitis_tarih_dogrulama(baslangic_tarihi, bitis_tarihi):
        ilk = __dt.datetime.strptime(baslangic_tarihi[:7], '%Y-%m')
        son = __dt.datetime.strptime(bitis_tarihi[:7], '%Y-%m')
        date_list = []
        while ilk <= son:
            date_list.append(ilk.strftime("%Y-%m-%d"))
            ilk = ilk + __rd.relativedelta(months=+1)
        with __Pool(__mp.cpu_count()) as p:
            df_list = p.map(__profil_serbest_tuketici_sayisi, date_list, chunksize=1)
        df_st = __pd.concat(df_list, sort=False)
        df_toplam = __serbest_tuketici_sayisi()
        return __pd.merge(df_st, df_toplam, how="left", on=["Dönem"])


def sayac_okuyan_kurum(tarih=__dt.datetime.today().strftime("%Y-%m-%d")):
    """
    Sayaç okuyan kurumların bilgisini vermektedir.

    Parametreler
    ------------

    Geri Dönüş Değeri
    -----------------
    Serbest Tüketici Sayısı (Tarih, Serbest Tüketici Sayısı, Artış Oranı)
    """
    if __dogrulama.__tarih_dogrulama(tarih):
        try:
            resp = __requests.get(__transparency_url + "meter-reading-company" + "?period=" + tarih, headers=__headers,
                                  timeout=__param.__timeout)
            resp.raise_for_status()
            list_kurum = resp.json()["body"]["meterReadingCompanyList"]
            df_kurum = __pd.DataFrame(list_kurum)
            df_kurum.rename(index=str,
                            columns={"id": "Id", "name": "Şirket Adı",
                                     "status": "Durum"},
                            inplace=True)
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
            return df_kurum


def dagitim_bolgeleri():
    """
    Dağıtım bölgelerine dair bilgileri vermektedir.

    Parametreler
    ------------

    Geri Dönüş Değeri
    -----------------
    Dağıtım Bölgeleri (Id, Dağıtım Bölgesi)
    """
    try:
        resp = __requests.get(__transparency_url + "distribution", headers=__headers)
        resp.raise_for_status()
        list_dagitim = resp.json()["body"]["distributionList"]
        df_dagitim = __pd.DataFrame(list_dagitim)
        df_dagitim.rename(index=str,
                          columns={"id": "Id", "name": "Dağıtım Şirket Adı"},
                          inplace=True)
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
        return df_dagitim


def profil_abone_grubu(tarih=__dt.datetime.today().strftime("%Y-%m-%d"), distribution_id=""):
    """
    İlgili tarihe tekabül eden uzlaştırma dönemi ve ağıtım bölgesi için profil abone grup listesini vermektedir.

    Parametreler
    ------------
    periyot : %YYYY-%AA-%GG formatında tarih (Varsayılan: bugün)

    Geri Dönüş Değeri
    -----------------
    Serbest Tüketici, Tedarik Kapsamındaki ve Toplam Uzlaştırmaya Esas Çekiş Miktarı (Tarih, Saat, UEÇM,
    Serbest Tüketici UEÇM, Tedarik Yükümlülüğü Kapsamındaki UEÇM)

    """
    if __dogrulama.__tarih_dogrulama(tarih):
        try:
            resp = __requests.get(
                __transparency_url + "subscriber-profile-group" + "?period=" + tarih + "&distributionId=" +
                str(distribution_id), headers=__headers, timeout=__param.__timeout)
            resp.raise_for_status()
            list_profil = resp.json()["body"]["subscriberProfileGroupList"]
            df_profil = __pd.DataFrame(list_profil)
            df_profil.rename(index=str,
                             columns={"id": "Id", "name": "Profil Adı"},
                             inplace=True)
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
            return df_profil


def tum_dagitimlar_profil_gruplari(tarih=__dt.datetime.today().strftime("%Y-%m-%d")):
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
    if __dogrulama.__tarih_dogrulama(tarih):
        dist = dagitim_bolgeleri()
        list_dist = list(dist["Id"])
        org_len = len(list_dist)
        list_date_dist = list(zip([tarih] * org_len, list_dist))
        list_date_dist = list(map(list, list_date_dist))
        with __Pool(__mp.cpu_count()) as p:
            list_df_unit = p.starmap(profil_gruplari, list_date_dist, chunksize=1)
        list_df_unit = list(filter(lambda x: len(x) > 0, list_df_unit))
        df_unit = __red(lambda left, right: __pd.merge(left, right, how="outer", on=["Id"], sort=True),
                        list_df_unit)
        df_unit.columns = ["Id"] + list(dist["Dağıtım Şirket Adı"])
        return df_unit


def sayac_okuma_tipi():
    """
    Sayaç okuma tip bilgileri vermektedir.

    Parametreler
    ------------

    Geri Dönüş Değeri
    -----------------
    Sayaç Okuma Tipleri (Id, Dağıtım Bölgesi)
    """
    try:
        resp = __requests.get(__transparency_url + "meter-reading-type", headers=__headers)
        resp.raise_for_status()
        list_sayac = resp.json()["body"]["meterReadingTypeList"]
        df_sayac = __pd.DataFrame(list_sayac)
        df_sayac.rename(index=str,
                        columns={"id": "Id", "name": "Sayaç Tipi"},
                        inplace=True)
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
        return df_sayac


def __uecm_donemlik(tarih=__dt.datetime.today().strftime("%Y-%m-%d")):
    """
    İlgili tarihe tekabül eden uzlaştırma dönemi için serbest tüketici hakkını kullanan serbest tüketicilerin, tedarik
    yükümlülüğü kapsamındaki ve toplam Uzlaştırmaya Esas Çekiş Miktarı (UEÇM) bilgisini vermektedir.

    Parametreler
    ------------
    periyot : %YYYY-%AA-%GG formatında tarih (Varsayılan: bugün)

    Geri Dönüş Değeri
    -----------------
    Serbest Tüketici, Tedarik Kapsamındaki ve Toplam Uzlaştırmaya Esas Çekiş Miktarı (Tarih, Saat, UEÇM,
    Serbest Tüketici UEÇM, Tedarik Yükümlülüğü Kapsamındaki UEÇM)

    """
    if __dogrulama.__tarih_dogrulama(tarih):
        try:
            resp = __requests.get(__transparency_url + "consumption" + "?period=" + tarih, headers=__headers,
                                  timeout=__param.__timeout)
            resp.raise_for_status()
            list_uecm = resp.json()["body"]["consumptions"]
            df_uecm = __pd.DataFrame(list_uecm)
            df_uecm["Dönem"] = df_uecm["period"].apply(lambda d: d[:7])
            df_uecm.rename(index=str,
                           columns={"consumption": "UEÇM", "eligibleCustomerConsumption": "Serbest Tüketici UEÇM",
                                    "underSupplyLiabilityConsumption": "Tedarik Yükümlülüğü Kapsamındaki UEÇM"},
                           inplace=True)
            df_uecm = df_uecm[
                ["Dönem", "UEÇM", "Serbest Tüketici UEÇM", "Tedarik Yükümlülüğü Kapsamındaki UEÇM"]]
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
            return df_uecm


def __uecm_serbest_tuketici(tarih=__dt.datetime.today().strftime("%Y-%m-%d")):
    """
    İlgili tarihe tekabül eden uzlaştırma dönemi için serbest tüketici hakkını kullanan serbest tüketicilerin saatlik
    Uzlaştırmaya Esas Çekiş Miktarı (UEÇM) bilgisini vermektedir.

    Parametreler
    ------------
    periyot : %YYYY-%AA-%GG formatında tarih (Varsayılan: bugün)

    Geri Dönüş Değeri
    -----------------
    Serbest Tüketici Uzlaştırmaya Esas Çekiş Miktarı (Tarih, Saat, Tüketim)

    """
    if __dogrulama.__tarih_dogrulama(tarih):
        try:
            resp = __requests.get(__transparency_url + "swv-v2" + "?period=" + tarih, headers=__headers,
                                  timeout=__param.__timeout)
            resp.raise_for_status()
            list_uecm = resp.json()["body"]["swvV2List"]
            df_uecm = __pd.DataFrame(list_uecm)
            df_uecm["Saat"] = df_uecm["vc_gec_trh"].apply(lambda h: int(h[11:13]))
            df_uecm["Tarih"] = __pd.to_datetime(df_uecm["vc_gec_trh"].apply(lambda d: d[:10]))
            df_uecm.rename(index=str, columns={"st": "Serbest Tüketici UEÇM"}, inplace=True)
            df_uecm = df_uecm[["Tarih", "Saat", "Serbest Tüketici UEÇM"]]
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
            return df_uecm


def __uecm_tedarik(tarih=__dt.datetime.today().strftime("%Y-%m-%d")):
    """
    İlgili tarihe tekabül eden uzlaştırma dönemi için tedarik yükümlülüğü kapsamındaki toplam Uzlaştırmaya Esas Çekiş
    Miktarı (UEÇM) bilgisini vermektedir.

    Parametreler
    ------------
    baslangic_tarihi : %YYYY-%AA-%GG formatında başlangıç tarihi (Varsayılan: bugün)
    bitis_tarihi     : %YYYY-%AA-%GG formatında bitiş tarihi (Varsayılan: bugün)

    Geri Dönüş Değeri
    -----------------
    Tedarik Yükümlülüğü Kapsamındaki UEÇM (Tarih, Saat, UEÇM)

    """
    if __dogrulama.__tarih_dogrulama(tarih):
        try:
            resp = __requests.get(
                __transparency_url + "under-supply-liability-consumption" + "?startDate=" + tarih + "&endDate=" + tarih,
                headers=__headers, timeout=__param.__timeout)
            resp.raise_for_status()
            list_uecm = resp.json()["body"]["swvList"]
            df_uecm = __pd.DataFrame(list_uecm)
            df_uecm["Dönemi"] = df_uecm["date"].apply(lambda d: d[:7])
            df_uecm.rename(index=str, columns={"swv": "Tedarik Yükümlülüğü Kapsamındaki UEÇM"}, inplace=True)
            df_uecm = df_uecm[["Dönemi", "Tedarik Yükümlülüğü Kapsamındaki UEÇM"]]
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
            return df_uecm


def __serbest_tuketici_sayisi():
    """
    İlgili tarih aralığına tekabül eden uzlaştırma dönemleri için serbest tüketici hakkını kullanan serbest
    tüketicilerin aylık toplam sayısını vermektedir.

    Parametreler
    ------------

    Geri Dönüş Değeri
    -----------------
    Serbest Tüketici Sayısı (Tarih, Serbest Tüketici Sayısı, Artış Oranı)
    """
    try:
        resp = __requests.get(__transparency_url + "eligible-consumer-quantity", headers=__headers,
                              timeout=__param.__timeout)
        resp.raise_for_status()
        list_st = resp.json()["body"]["eligibleConsumerQuantityList"]
        df_st = __pd.DataFrame(list_st)
        df_st["Dönem"] = __pd.to_datetime(df_st["date"].apply(lambda d: d[:10]))
        df_st.rename(index=str,
                     columns={"meterQuantity": "Serbest Tüketici Sayısı", "meterIncreaseRate": "Artış Oranı"},
                     inplace=True)
        df_st = df_st[["Dönem", "Serbest Tüketici Sayısı", "Artış Oranı"]]
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
        return df_st


def __profil_serbest_tuketici_sayisi(tarih=__dt.datetime.today().strftime("%Y-%m-%d")):
    """
    İlgili tarihe tekabül eden uzlaştırma dönemi için profil abone grubuna göre serbest tüketici hakkını
    kullanan serbest tüketici sayıları bilgisini vermektedir.

    Parametreler
    ------------
    baslangic_tarihi : %YYYY-%AA-%GG formatında başlangıç tarihi (Varsayılan: bugün)
    bitis_tarihi     : %YYYY-%AA-%GG formatında bitiş tarihi (Varsayılan: bugün)

    Geri Dönüş Değeri
    -----------------
    Profil Abone Grubuna Göre Serbest Tüketici Sayıları (Tarih, Aydınlatma,  Diğer,  Mesken,  Sanayi,  Tarimsal,
    Sulama, Ticarethane)

    """
    try:
        resp = __requests.get(
            __transparency_url + "st" + "?startDate=" + tarih +
            "&endDate=" + tarih, headers=__headers, timeout=__param.__timeout)
        resp.raise_for_status()
        list_st = resp.json()["body"]["stList"]
        df_st = __pd.DataFrame(list_st)
        df_st["Profil"] = df_st["id"].apply(lambda x: x["profilAboneGrupAdi"])
        df_st["Dönem"] = df_st["id"].apply(lambda x: __pd.to_datetime(x["date"][:10]))
        df_st = df_st.pivot(index='Dönem', columns='Profil', values='stCount').reset_index()
        df_st.columns.name = None
        df_st.columns = df_st.columns.str.title()
        df_st.rename(index=str,
                     columns={"Aydinlatma": "Aydınlatma", "Diger": "Diğer",
                              "Tarimsal": "Tarımsal"},
                     inplace=True)
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
        return df_st
