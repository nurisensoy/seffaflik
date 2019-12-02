import pandas as __pd
import datetime as __dt
from dateutil import relativedelta as __rd
from multiprocessing import Pool as __Pool
import multiprocessing as __mp
from functools import reduce as __red

from seffaflik.__ortak.__araclar import make_requests as __make_requests
from seffaflik.__ortak import __dogrulama as __dogrulama

__first_part_url = "consumption/"


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
        particular_url = __first_part_url + "city"
        json = __make_requests(particular_url)
        df = __pd.DataFrame(json["body"]["cityList"])
        df.rename(index=str, columns={"cityId": "Şehir Id", "districtId": "İlçe Id", "cityName": "Şehir İsmi",
                                      "districtName": "İlçe İsmi"}, inplace=True)
    except KeyError:
        return __pd.DataFrame()
    else:
        return df.drop_duplicates().reset_index(drop=True)


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
            particular_url = __first_part_url + "real-time-consumption" + "?startDate=" + baslangic_tarihi + \
                             "&endDate=" + bitis_tarihi
            json = __make_requests(particular_url)
            df = __pd.DataFrame(json["body"]["hourlyConsumptions"])
            df["Saat"] = df["date"].apply(lambda h: int(h[11:13]))
            df["Tarih"] = __pd.to_datetime(df["date"].apply(lambda d: d[:10]))
            df.rename(index=str, columns={"consumption": "Tüketim"}, inplace=True)
            df = df[["Tarih", "Saat", "Tüketim"]]
        except KeyError:
            return __pd.DataFrame()
        else:
            return df


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
            particular_url = __first_part_url + "swv" + "?startDate=" + baslangic_tarihi + "&endDate=" + bitis_tarihi
            json = __make_requests(particular_url)
            df = __pd.DataFrame(json["body"]["swvList"])
            df["Saat"] = df["date"].apply(lambda h: int(h[11:13]))
            df["Tarih"] = __pd.to_datetime(df["date"].apply(lambda d: d[:10]))
            df.rename(index=str, columns={"swv": "UEÇM"}, inplace=True)
            df = df[["Tarih", "Saat", "UEÇM"]]
        except KeyError:
            return __pd.DataFrame()
        else:
            return df


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
            particular_url = __first_part_url + "load-estimation-plan" + "?startDate=" + baslangic_tarihi + \
                             "&endDate=" + bitis_tarihi
            json = __make_requests(particular_url)
            df = __pd.DataFrame(json["body"]["loadEstimationPlanList"])
            df["Saat"] = df["date"].apply(lambda h: int(h[11:13]))
            df["Tarih"] = __pd.to_datetime(df["date"].apply(lambda d: d[:10]))
            df.rename(index=str, columns={"lep": "Tüketim"}, inplace=True)
            df = df[["Tarih", "Saat", "Tüketim"]]
        except KeyError:
            return __pd.DataFrame()
        else:
            return df


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
            particular_url = __first_part_url + "meter-reading-company" + "?period=" + tarih
            json = __make_requests(particular_url)
            df = __pd.DataFrame(json["body"]["meterReadingCompanyList"])
            df.rename(index=str,
                      columns={"id": "Id", "name": "Şirket Adı", "status": "Durum"},
                      inplace=True)
        except KeyError:
            return __pd.DataFrame()
        else:
            return df


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
        particular_url = __first_part_url + "distribution"
        json = __make_requests(particular_url)
        df = __pd.DataFrame(json["body"]["distributionList"])
        df.rename(index=str, columns={"id": "Id", "name": "Dağıtım Şirket Adı"}, inplace=True)
    except KeyError:
        return __pd.DataFrame()
    else:
        return df


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
            particular_url = __first_part_url + "subscriber-profile-group" + "?period=" + tarih + "&distributionId=" \
                             + str(distribution_id)
            json = __make_requests(particular_url)
            df = __pd.DataFrame(json["body"]["subscriberProfileGroupList"])
            df.rename(index=str, columns={"id": "Id", "name": "Profil Adı"}, inplace=True)
        except KeyError:
            return __pd.DataFrame()
        else:
            return df


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
            list_df_unit = p.starmap(profil_abone_grubu, list_date_dist, chunksize=1)
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
        particular_url = __first_part_url + "meter-reading-type"
        json = __make_requests(particular_url)
        df = __pd.DataFrame(json["body"]["meterReadingTypeList"])
        df.rename(index=str, columns={"id": "Id", "name": "Sayaç Tipi"}, inplace=True)
    except KeyError:
        return __pd.DataFrame()
    else:
        return df


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
            particular_url = __first_part_url + "consumption" + "?period=" + tarih
            json = __make_requests(particular_url)
            df = __pd.DataFrame(json["body"]["consumptions"])
            df["Dönem"] = df["period"].apply(lambda d: d[:7])
            df.rename(index=str,
                      columns={"consumption": "UEÇM", "eligibleCustomerConsumption": "Serbest Tüketici UEÇM",
                               "underSupplyLiabilityConsumption": "Tedarik Yükümlülüğü Kapsamındaki UEÇM"},
                      inplace=True)
            df = df[["Dönem", "UEÇM", "Serbest Tüketici UEÇM", "Tedarik Yükümlülüğü Kapsamındaki UEÇM"]]
        except KeyError:
            return __pd.DataFrame()
        else:
            return df


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
            particular_url = __first_part_url + "swv-v2" + "?period=" + tarih
            json = __make_requests(particular_url)
            df = __pd.DataFrame(json["body"]["swvV2List"])
            df["Saat"] = df["vc_gec_trh"].apply(lambda h: int(h[11:13]))
            df["Tarih"] = __pd.to_datetime(df["vc_gec_trh"].apply(lambda d: d[:10]))
            df.rename(index=str, columns={"st": "Serbest Tüketici UEÇM"}, inplace=True)
            df = df[["Tarih", "Saat", "Serbest Tüketici UEÇM"]]
        except KeyError:
            return __pd.DataFrame()
        else:
            return df


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
            particular_url = __first_part_url + "under-supply-liability-consumption" + "?startDate=" + tarih + \
                             "&endDate=" + tarih
            json = __make_requests(particular_url)
            df = __pd.DataFrame(json["body"]["swvList"])
            df["Dönem"] = df["date"].apply(lambda d: d[:7])
            df.rename(index=str, columns={"swv": "Tedarik Yükümlülüğü Kapsamındaki UEÇM"}, inplace=True)
            df = df[["Dönem", "Tedarik Yükümlülüğü Kapsamındaki UEÇM"]]
        except KeyError:
            return __pd.DataFrame()
        else:
            return df


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
        particular_url = __first_part_url + "eligible-consumer-quantity"
        json = __make_requests(particular_url)
        df = __pd.DataFrame(json["body"]["eligibleConsumerQuantityList"])
        df["Dönem"] = __pd.to_datetime(df["date"].apply(lambda d: d[:10]))
        df.rename(index=str,
                  columns={"meterQuantity": "Serbest Tüketici Sayısı", "meterIncreaseRate": "Artış Oranı"},
                  inplace=True)
        df = df[["Dönem", "Serbest Tüketici Sayısı", "Artış Oranı"]]
    except KeyError:
        return __pd.DataFrame()
    else:
        return df


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
        particular_url = __first_part_url + "st" + "?startDate=" + tarih + "&endDate=" + tarih
        json = __make_requests(particular_url)
        df = __pd.DataFrame(json["body"]["stList"])
        df["Profil"] = df["id"].apply(lambda x: x["profilAboneGrupAdi"])
        df["Dönem"] = df["id"].apply(lambda x: __pd.to_datetime(x["date"][:10]))
        df = df.pivot(index='Dönem', columns='Profil', values='stCount').reset_index()
        df.columns.name = None
        df.columns = df.columns.str.title()
        df.rename(index=str,
                  columns={"Aydinlatma": "Aydınlatma", "Diger": "Diğer", "Tarimsal": "Tarımsal"},
                  inplace=True)
    except KeyError:
        return __pd.DataFrame()
    else:
        return df
