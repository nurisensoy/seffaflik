import pandas as __pd
import datetime as __dt
from dateutil import relativedelta as __rd
from multiprocessing import Pool as __Pool
import multiprocessing as __mp

from seffaflik.__ortak.__araclar import make_requests as __make_requests
from seffaflik.__ortak import __dogrulama as __dogrulama

__first_part_url = "production/"


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
            particular_url = __first_part_url + "power-plant?period=" + tarih
            json = __make_requests(particular_url)
            df = __pd.DataFrame(json["body"]["powerPlantList"])
            df.rename(index=str, columns={"id": "Id", "name": "Adı", "eic": "EIC Kodu",
                                          "shortName": "Kısa Adı"}, inplace=True)
            df = df[["Id", "Adı", "EIC Kodu", "Kısa Adı"]]
        except (KeyError, TypeError):
            return __pd.DataFrame()
        else:
            return df


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
            particular_url = __first_part_url + "uevcb?period=" + tarih + "&powerPlantId=" + str(santral_id)
            json = __make_requests(particular_url)
            df = __pd.DataFrame(json["body"]["uevcbList"])
            df.rename(index=str, columns={"id": "Id", "name": "Adı", "eic": "EIC Kodu"}, inplace=True)
            df = df[["Id", "Adı", "EIC Kodu"]]
        except (KeyError, TypeError):
            return __pd.DataFrame()
        else:
            return df


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
        particular_url = __first_part_url + "real-time-generation-power-plant-list"
        json = __make_requests(particular_url)
        df = __pd.DataFrame(json["body"]["powerPlantList"])
        df.rename(index=str, columns={"id": "Id", "name": "Adı", "eic": "EIC Kodu",
                                      "shortName": "Kısa Adı"}, inplace=True)
        df = df[["Id", "Adı", "EIC Kodu", "Kısa Adı"]]
    except (KeyError, TypeError):
        return __pd.DataFrame()
    else:
        return df


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


def ariza_bakim_bildirimleri(baslangic_tarihi=__dt.datetime.today().strftime("%Y-%m-%d"),
                             bitis_tarihi=__dt.datetime.today().strftime("%Y-%m-%d")):
    """
    İlgili tarih aralığı için santrallerin bildirmiş oldukları arıza/bakımların bilgilerini vermektedir.

    Parametreler
    ------------
    baslangic_tarihi : %YYYY-%AA-%GG formatında başlangıç tarihi (Varsayılan: bugün)
    bitis_tarihi     : %YYYY-%AA-%GG formatında bitiş tarihi (Varsayılan: bugün)

    Geri Dönüş Değeri
    -----------------
    Santral Arıza/Bakım Bildirimleri(Olay Bildirim Tarihi, Santral Adı, UEVCB Adı, Şehir, Olay Balangıç Tarihi,
                 Olay Bitiş Tarihi, İşletmedeki Kurulu Güç, Olay Sırasında Kapasite, Yakıt Tipi, Gerekçe,
                 Gerekçe Tipi)
    """
    if __dogrulama.__baslangic_bitis_tarih_dogrulama(baslangic_tarihi, bitis_tarihi):
        try:
            particular_url = __first_part_url + "urgent-market-message" + "?startDate=" + baslangic_tarihi + \
                             "&endDate=" + bitis_tarihi + "&regionId=1"
            json = __make_requests(particular_url)
            df = __pd.DataFrame(json["body"]["urgentMarketMessageList"])
            df["caseAddDate"] = __pd.to_datetime(df["caseAddDate"])
            df["caseStartDate"] = __pd.to_datetime(df["caseStartDate"])
            df["caseEndDate"] = __pd.to_datetime(df["caseEndDate"])
            df["Gerekçe Tipi"] = df["messageType"].map({0: "Arıza", 2: "Bakım"})
            df.rename(index=str,
                      columns={"capacityAtCaseTime": "Olay Sırasında Kapasite",
                               "powerPlantName": "Santral Adı", "city": "Şehir",
                               "operatorPower": "İşletmedeki Kurulu Güç", "uevcbName": "UEVCB Adı",
                               "reason": "Gerekçe", "caseStartDate": "Olay Balangıç Tarihi",
                               "caseEndDate": "Olay Bitiş Tarihi", "caseAddDate": "Olay Bildirim Tarihi",
                               "fuelType": "Yakıt Tipi"}, inplace=True)
            df = df[
                ["Olay Bildirim Tarihi", "Santral Adı", "UEVCB Adı", "Şehir", "Olay Balangıç Tarihi",
                 "Olay Bitiş Tarihi", "İşletmedeki Kurulu Güç", "Olay Sırasında Kapasite", "Yakıt Tipi", "Gerekçe",
                 "Gerekçe Tipi"]]
        except (KeyError, TypeError):
            return __pd.DataFrame()
        else:
            return df


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
        particular_url = __first_part_url + "installed-capacity?period=" + tarih
        json = __make_requests(particular_url)
        df = __pd.DataFrame(json["body"]["installedCapacityList"])
        df = df[df["capacityType"] == "ALL"]
        df.insert(loc=0, column="Tarih", value=__pd.to_datetime(tarih))
        df.rename(index=str, columns={"capacity": "Kurulu Güç"}, inplace=True)
        df = df[["Tarih", "Kurulu Güç"]]
    except (KeyError, TypeError):
        return __pd.DataFrame()
    else:
        return df


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
        particular_url = __first_part_url + "uevcb?period=" + tarih + "&powerPlantId=" + str(santral["Id"])
        json = __make_requests(particular_url)
        df = __pd.DataFrame(json["body"]["uevcbList"])
        df["Santral Id"] = santral["Id"]
        df["Santral Adı"] = santral["Adı"]
        df["Santral EIC Kodu"] = santral["EIC Kodu"]
        df["Santral Kısa Adı"] = santral["Kısa Adı"]
        df.rename(index=str,
                  columns={"id": "UEVÇB Id", "name": "UEVÇB Adı", "eic": "UEVÇB EIC Kodu"},
                  inplace=True)
        df = df[["Santral Id", "Santral Adı", "Santral EIC Kodu", "Santral Kısa Adı", "UEVÇB Id",
                 "UEVÇB Adı", "UEVÇB EIC Kodu"]]
    except (KeyError, TypeError):
        return __pd.DataFrame()
    else:
        return df
