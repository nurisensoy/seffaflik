import requests as __requests
from requests import ConnectionError as __ConnectionError
from requests.exceptions import HTTPError as __HTTPError, RequestException as __RequestException, Timeout as __Timeout
import pandas as __pd
import datetime as __dt
from dateutil import relativedelta as __rd
from multiprocessing import Pool as __Pool
import multiprocessing as __mp
import logging as __logging

from seffaflik.__ortak import __dogrulama as __dogrulama, __parametreler as __param, __anahtar as __api

__transparency_url = __param.SEFFAFLIK_URL + "production/"
__headers = __api.HEADERS


def santraller(tarih=__dt.datetime.now().strftime("%Y-%m-%d")):
    """
    İlgili tarihte EPİAŞ sistemine kayıtlı santrallerin bilgilerini vermektedir.

    Parametre
    ----------
    tarih : %YYYY-%AA-%GG formatında tarih (Varsayılan: bugün)

    Geri Dönüş Değeri
    -----------------
    Santral Bilgileri(Id, Adı, EIC Kodu, Kısa Adı)
    """
    if __dogrulama.__tarih_dogrulama(tarih):
        try:
            resp = __requests.get(__transparency_url + "power-plant?period=" + tarih, headers=__headers,
                                  timeout=__param.__timeout)
            resp.raise_for_status()
            list_santral = resp.json()["body"]["powerPlantList"]
            df_santral = __pd.DataFrame(list_santral)
            df_santral.rename(index=str, columns={"id": "Id", "name": "Adı", "eic": "EIC Kodu",
                                                  "shortName": "Kısa Adı"}, inplace=True)
            df_santral = df_santral[["Id", "Adı", "EIC Kodu", "Kısa Adı"]]
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
            return df_santral


def santral_veris_cekis_birimleri(tarih=__dt.datetime.today().strftime("%Y-%m-%d"), santral_id=""):
    """
    İlgili tarih ve santral ID için santralin altında tanımlanmış uzlaştırmaya
    esas veriş-çekiş birim (UEVÇB) bilgilerini vermektedir.

    Parametreler
    ------------
    tarih      : %YYYY-%AA-%GG formatında tarih (Varsayılan: bugün)
    santral_id : metin yada tam sayı formatında santral id (Varsayılan: "")

    Geri Dönüş Değeri
    -----------------
    İlgili  UEVÇB Bilgileri(Id, Adı, EIC Kodu)
    """

    if __dogrulama.__tarih_id_dogrulama(tarih, santral_id):
        try:
            resp = __requests.get(__transparency_url + "uevcb?period=" + tarih +
                                  "&powerPlantId=" + str(santral_id), headers=__headers, timeout=__param.__timeout)
            resp.raise_for_status()
            list_unit = resp.json()["body"]["uevcbList"]
            df_unit = __pd.DataFrame(list_unit)
            df_unit.rename(index=str, columns={"id": "Id", "name": "Adı", "eic": "EIC Kodu"}, inplace=True)
            df_unit = df_unit[["Id", "Adı", "EIC Kodu"]]
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
            return df_unit


def tum_santraller_veris_cekis_birimleri(tarih=__dt.datetime.today().strftime("%Y-%m-%d")):
    """
    İlgili tarih için EPİAŞ sistemine kayıtlı tüm santrallerin altında tanımlanmış uzlaştırmaya
    esas veriş-çekiş birim (UEVÇB) bilgilerini vermektedir.

    Parametreler
    ------------
    tarih      : %YYYY-%AA-%GG formatında tarih (Varsayılan: bugün)

    Geri Dönüş Değeri
    -----------------
    EPİAŞ Sistemine Kayıtlı Santraller ve UEVÇB Bilgileri("Santral Id, "Santral Adı", "Santral EIC Kodu",
    "Santral Kısa Adı", "UEVÇB Id", "UEVÇB Adı", "UEVÇB EIC Kodu")
    """
    if __dogrulama.__tarih_dogrulama(tarih):
        list_santral = santraller()[["Id", "Adı", "EIC Kodu", "Kısa Adı"]].to_dict("records")
        santral_len = len(list_santral)
        list_date_santral_id = list(zip([tarih] * santral_len, list_santral))
        list_date_santral_id = list(map(list, list_date_santral_id))
        with __Pool(__mp.cpu_count()) as p:
            list_df_unit = p.starmap(__santral_veris_cekis_birimleri, list_date_santral_id, chunksize=1)
        return __pd.concat(list_df_unit).reset_index(drop=True)


def gercek_zamanli_uretim_yapan_santraller():
    """
    İsteğin yapıldığı tarihte gerçek zamanlı üretim yapan UEVÇB bazında santral bilgilerini vermektedir.

    Parametre
    ----------

    Geri Dönüş Değeri
    -----------------
    Santral Bilgileri(Id, Adı, EIC Kodu, Kısa Adı)
    """
    try:
        resp = __requests.get(__transparency_url + "real-time-generation-power-plant-list", headers=__headers,
                              timeout=__param.__timeout)
        resp.raise_for_status()
        list_santral = resp.json()["body"]["powerPlantList"]
        df_santral = __pd.DataFrame(list_santral)
        df_santral.rename(index=str, columns={"id": "Id", "name": "Adı", "eic": "EIC Kodu",
                                              "shortName": "Kısa Adı"}, inplace=True)
        df_santral = df_santral[["Id", "Adı", "EIC Kodu", "Kısa Adı"]]
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
        return df_santral


def yekdem_santralleri(tarih=__dt.datetime.now().strftime("%Y-%m-%d")):
    """
    İlgili tarihte EPİAŞ sistemine kayıtlı YEKDEM santral bilgilerini vermektedir.

    Parametre
    ----------
    tarih : %YYYY-%AA-%GG formatında tarih (Varsayılan: bugün)

    Geri Dönüş Değeri
    -----------------
    Santral Bilgileri(Id, Adı, EIC Kodu, Kısa Adı)
    """
    if __dogrulama.__tarih_dogrulama(tarih):
        try:
            resp = __requests.get(__transparency_url + "renewable-sm-licensed-power-plant-list?period=" + tarih,
                                  headers=__headers, timeout=__param.__timeout)
            resp.raise_for_status()
            list_santral = resp.json()["body"]["powerPlantList"]
            df_santral = __pd.DataFrame(list_santral)
            df_santral.rename(index=str, columns={"id": "Id", "name": "Adı", "eic": "EIC Kodu",
                                                  "shortName": "Kısa Adı"}, inplace=True)
            df_santral = df_santral[["Id", "Adı", "EIC Kodu", "Kısa Adı"]]
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
            return df_santral


def kurulu_guc(baslangic_tarihi=__dt.datetime.today().strftime("%Y-%m-%d"),
               bitis_tarihi=__dt.datetime.today().strftime("%Y-%m-%d")):
    """
    İlgili tarih aralığına tekabül eden aylar için EPİAŞ sistemine kayıtlı santrallerin toplam kurulu güç bilgisini
    vermektedir.

    Parametreler
    ------------
    baslangic_tarihi : %YYYY-%AA-%GG formatında başlangıç tarihi (Varsayılan: bugün)
    bitis_tarihi     : %YYYY-%AA-%GG formatında bitiş tarihi (Varsayılan: bugün)

    Geri Dönüş Değeri
    -----------------
    Kurulu Güç Bilgisi (Tarih, Kurulu Güç)
    """
    if __dogrulama.__baslangic_bitis_tarih_dogrulama(baslangic_tarihi, bitis_tarihi):
        ilk = __dt.datetime.strptime(baslangic_tarihi[:7], '%Y-%m')
        son = __dt.datetime.strptime(bitis_tarihi[:7], '%Y-%m')
        date_list = []
        while ilk <= son:
            date_list.append(ilk.strftime("%Y-%m-%d"))
            ilk = ilk + __rd.relativedelta(months=+1)
        with __Pool(__mp.cpu_count()) as p:
            df_list = p.map(__kurulu_guc, date_list)
        return __pd.concat(df_list, sort=False)


def yekdem_kurulu_guc(baslangic_tarihi=__dt.datetime.today().strftime("%Y-%m-%d"),
                      bitis_tarihi=__dt.datetime.today().strftime("%Y-%m-%d")):
    """
    İlgili tarih aralığına tekabül eden aylar için EPİAŞ sistemine kayıtlı YEKDEM santrallerin kaynak bazlı toplam
    kurulu güç bilgisini vermektedir.

    Parametreler
    ------------
    baslangic_tarihi : %YYYY-%AA-%GG formatında başlangıç tarihi (Varsayılan: bugün)
    bitis_tarihi     : %YYYY-%AA-%GG formatında bitiş tarihi (Varsayılan: bugün)

        with __Pool(__mp.cpu_count()) as p:
    Geri Dönüş Değeri
    -----------------
    Kurulu Güç Bilgisi (Tarih, Kurulu Güç)
    """
    if __dogrulama.__baslangic_bitis_tarih_dogrulama(baslangic_tarihi, bitis_tarihi):
        ilk = __dt.datetime.strptime(baslangic_tarihi[:7], '%Y-%m')
        son = __dt.datetime.strptime(bitis_tarihi[:7], '%Y-%m')
        date_list = []
        while ilk <= son and ilk <= __dt.datetime.today():
            date_list.append(ilk.strftime("%Y-%m-%d"))
            ilk = ilk + __rd.relativedelta(months=+1)
        with __Pool(__mp.cpu_count()) as p:
            df_list = p.map(__yekdem_kurulu_guc, date_list)
        return __pd.concat(df_list, sort=False)


def ariza_bakim_bildirimleri(baslangic_tarihi=__dt.datetime.today().strftime("%Y-%m-%d"),
                             bitis_tarihi=__dt.datetime.today().strftime("%Y-%m-%d"), bolge_id="1"):
    """
    İlgili tarih aralığı için santrallerin bildirmiş oldukları arıza/bakımların bilgilerini vermektedir.

    Parametre
    ----------
    tarih : %YYYY-%AA-%GG formatında tarih (Varsayılan: bugün)

    Geri Dönüş Değeri
    -----------------
    Santral Bilgileri(Id, Adı, EIC Kodu, Kısa Adı)
    """
    if __dogrulama.__baslangic_bitis_tarih_dogrulama(baslangic_tarihi, bitis_tarihi):
        try:
            resp = __requests.get(
                __transparency_url + "urgent-market-message" + "?startDate=" + baslangic_tarihi +
                "&endDate=" + bitis_tarihi + "&regionId=" + bolge_id, headers=__headers, timeout=__param.__timeout)
            resp.raise_for_status()
            list_bildirim = resp.json()["body"]["urgentMarketMessageList"]
            df_bildirim = __pd.DataFrame(list_bildirim)
            df_bildirim["caseAddDate"] = __pd.to_datetime(df_bildirim["caseAddDate"])
            df_bildirim["caseStartDate"] = __pd.to_datetime(df_bildirim["caseStartDate"])
            df_bildirim["caseEndDate"] = __pd.to_datetime(df_bildirim["caseEndDate"])
            df_bildirim["Gerekçe Tipi"] = df_bildirim["messageType"].map({0: "Arıza", 2: "Bakım"})
            df_bildirim.rename(index=str,
                               columns={"capacityAtCaseTime": "Olay Sırasında Kapasite",
                                        "powerPlantName": "Santral Adı", "city": "Şehir",
                                        "operatorPower": "İşletmedeki Kurulu Güç", "uevcbName": "UEVCB Adı",
                                        "reason": "Gerekçe", "caseStartDate": "Olay Balangıç Tarihi",
                                        "caseEndDate": "Olay Bitiş Tarihi", "caseAddDate": "Olay Bildirim Tarihi",
                                        "fuelType": "Yakıt Tipi"}, inplace=True)
            df_bildirim = df_bildirim[
                ["Olay Bildirim Tarihi", "Santral Adı", "UEVCB Adı", "Şehir", "Olay Balangıç Tarihi",
                 "Olay Bitiş Tarihi", "İşletmedeki Kurulu Güç", "Olay Sırasında Kapasite", "Yakıt Tipi", "Gerekçe",
                 "Gerekçe Tipi"]]
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
            return df_bildirim


def __kurulu_guc(tarih):
    """
    İlgili tarih için EPİAŞ sistemine kayıtlı santrallerin toplam kurulu güç bilgisini vermektedir.

    Parametre
    ----------
    tarih : %YYYY-%AA-%GG formatında tarih (Varsayılan: bugün)

    Geri Dönüş Değeri
    -----------------
    Kurulu Güç Bilgisi (Tarih, Kurulu Güç)
    """
    try:
        resp = __requests.get(__transparency_url + "installed-capacity?period=" + tarih, headers=__headers,
                              timeout=__param.__timeout)
        resp.raise_for_status()
        list_guc = resp.json()["body"]["installedCapacityList"]
        df_guc = __pd.DataFrame(list_guc)
        df_guc = df_guc[df_guc["capacityType"] == "ALL"]
        df_guc.insert(loc=0, column="Tarih", value=__pd.to_datetime(tarih))
        df_guc.rename(index=str, columns={"capacity": "Kurulu Güç"}, inplace=True)
        df_guc = df_guc[["Tarih", "Kurulu Güç"]]
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
        return df_guc


def __yekdem_kurulu_guc(tarih):
    """
    İlgili tarih için EPİAŞ sistemine kayıtlı yekdem kapsamındaki santrallerin kaynak bazlı toplam kurulu güç bilgisini
    vermektedir.

    Parametre
    ----------
    tarih : %YYYY-%AA-%GG formatında tarih (Varsayılan: bugün)

    Geri Dönüş Değeri
    -----------------
    Kurulu Güç Bilgisi (Tarih, Kurulu Güç)
    """
    try:
        resp = __requests.get(__transparency_url + "installed-capacity-of-renewable?period=" + tarih, headers=__headers,
                              timeout=__param.__timeout)
        resp.raise_for_status()
        list_guc = resp.json()["body"]["installedCapacityOfRenewableList"]
        df_guc = __pd.DataFrame(list_guc)
        columns = df_guc["capacityType"].values
        df_guc = df_guc[["capacity"]].transpose()
        df_guc.set_axis(columns, axis=1, inplace=True)
        df_guc.reset_index(drop=True, inplace=True)
        df_guc.insert(loc=0, column="Tarih", value=__pd.to_datetime(tarih))
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
        return df_guc


def __santral_veris_cekis_birimleri(tarih, santral):
    """
   İlgili tarih ve santral için santralin altında tanımlanmış uzlaştırmaya
   esas veriş-çekiş birim (UEVÇB) bilgilerini vermektedir.

   Parametreler
   ------------
   tarih      : %YYYY-%AA-%GG formatında tarih
   santral    : dict formatında santral id, Adı, EIC Kodu, Kısa Adı

   Geri Dönüş Değeri
   -----------------
   İlgili  UEVÇB Bilgileri(Santral Id, Santral Adı, Santral EIC Kodu, Santral Kısa Adı, UEVÇB Id,
                           UEVÇB Adı, UEVÇB EIC Kodu)
   """

    try:
        resp = __requests.get(__transparency_url + "uevcb?period=" + tarih +
                              "&powerPlantId=" + str(santral["Id"]), headers=__headers, timeout=__param.__timeout)
        resp.raise_for_status()
        list_unit = resp.json()["body"]["uevcbList"]
        df_unit = __pd.DataFrame(list_unit)
        df_unit["Santral Id"] = santral["Id"]
        df_unit["Santral Adı"] = santral["Adı"]
        df_unit["Santral EIC Kodu"] = santral["EIC Kodu"]
        df_unit["Santral Kısa Adı"] = santral["Kısa Adı"]
        df_unit.rename(index=str,
                       columns={"id": "UEVÇB Id", "name": "UEVÇB Adı", "eic": "UEVÇB EIC Kodu"},
                       inplace=True)
        df_unit = df_unit[["Santral Id", "Santral Adı", "Santral EIC Kodu", "Santral Kısa Adı", "UEVÇB Id",
                           "UEVÇB Adı", "UEVÇB EIC Kodu"]]
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
        return df_unit
