import requests as __requests
import pandas as __pd
import datetime as __dt

from seffaflik.ortak import dogrulama as __dogrulama, parametreler as __param, anahtar as __api

__hata = __param.BILINMEYEN_HATA_MESAJI
__transparency_url = __param.SEFFAFLIK_URL + "consumption/"
__headers = __api.HEADERS


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
    while __dogrulama.baslangic_bitis_tarih_dogrulama(baslangic_tarihi, bitis_tarihi):
        try:
            resp = __requests.get(
                __transparency_url + "real-time-consumption" + "?startDate=" + baslangic_tarihi +
                "&endDate=" + bitis_tarihi, headers=__headers)
            list_tuketim = resp.json()["body"]["hourlyConsumptions"]
            df_tuketim = __pd.DataFrame(list_tuketim)
            df_tuketim["Saat"] = df_tuketim["date"].apply(lambda h: int(h[11:13]))
            df_tuketim["Tarih"] = __pd.to_datetime(df_tuketim["date"].apply(lambda d: d[:10]))
            df_tuketim.rename(index=str, columns={"consumption": "Tüketim"}, inplace=True)
            df_tuketim = df_tuketim[["Tarih", "Saat", "Tüketim"]]
        except __requests.exceptions.RequestException as e:
            print(e)
        except KeyError:
            return print("İlgili tarih için veri bulunmamaktadır!")
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
    while __dogrulama.baslangic_bitis_tarih_dogrulama(baslangic_tarihi, bitis_tarihi):
        try:
            resp = __requests.get(
                __transparency_url + "swv" + "?startDate=" + baslangic_tarihi +
                "&endDate=" + bitis_tarihi, headers=__headers)
            list_uecm = resp.json()["body"]["swvList"]
            df_uecm = __pd.DataFrame(list_uecm)
            df_uecm["Saat"] = df_uecm["date"].apply(lambda h: int(h[11:13]))
            df_uecm["Tarih"] = __pd.to_datetime(df_uecm["date"].apply(lambda d: d[:10]))
            df_uecm.rename(index=str, columns={"swv": "UEÇM"}, inplace=True)
            df_uecm = df_uecm[["Tarih", "Saat", "UEÇM"]]
        except __requests.exceptions.RequestException as e:
            print(e)
        except KeyError:
            return print("İlgili tarih için veri bulunmamaktadır!")
        else:
            return df_uecm


def uecm_serbest_tuketici_donemsel(tarih=__dt.datetime.today().strftime("%Y-%m-%d")):
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
    while __dogrulama.tarih_dogrulama(tarih):
        try:
            resp = __requests.get(__transparency_url + "swv-v2" + "?period=" + tarih, headers=__headers)
            list_uecm = resp.json()["body"]["swvV2List"]
            df_uecm = __pd.DataFrame(list_uecm)
            df_uecm["Saat"] = df_uecm["vc_gec_trh"].apply(lambda h: int(h[11:13]))
            df_uecm["Tarih"] = __pd.to_datetime(df_uecm["vc_gec_trh"].apply(lambda d: d[:10]))
            df_uecm.rename(index=str, columns={"st": "Serbest Tüketici UEÇM"}, inplace=True)
            df_uecm = df_uecm[["Tarih", "Saat", "Serbest Tüketici UEÇM"]]
        except __requests.exceptions.RequestException as e:
            print(e)
        except KeyError:
            print("İlgili tarih için veri bulunmamaktadır!")
            return __pd.DataFrame()
        else:
            return df_uecm


def uecm_tedarik(tarih=__dt.datetime.today().strftime("%Y-%m-%d")):
    """
    İlgili tarihe tekabül eden uzlaştırma dönemi için tedarik yükümlülüğü kapsamındaki toplam Uzlaştırmaya Esas Çekiş
    Miktarı (UEÇM) bilgisini vermektedir.

    Parametreler
    ------------
    baslangic_tarihi : %YYYY-%AA-%GG formatında başlangıç tarihi (Varsayılan: bugün)
    bitis_tarihi     : %YYYY-%AA-%GG formatında bitiş tarihi (Varsayılan: bugün)

    Geri Dönüş Değeri
    -----------------
    Tedarik Yükümlülüğü Kapsamındaki Uzlaştırmaya Esas Çekiş Miktarı (Tarih, Saat, UEÇM)

    """
    while __dogrulama.tarih_dogrulama(tarih):
        try:
            resp = __requests.get(
                __transparency_url + "under-supply-liability-consumption" + "?startDate=" + tarih + "&endDate=" + tarih,
                headers=__headers)
            list_uecm = resp.json()["body"]["swvList"]
            df_uecm = __pd.DataFrame(list_uecm)
            df_uecm["Uzlaştırma Dönemi"] = df_uecm["date"].apply(lambda d: d[:7])
            df_uecm.rename(index=str, columns={"swv": "Tedarik Yükümlülüğü Kapsamındaki UEÇM"}, inplace=True)
            df_uecm = df_uecm[["Uzlaştırma Dönemi", "Tedarik Yükümlülüğü Kapsamındaki UEÇM"]]
        except __requests.exceptions.RequestException as e:
            print(e)
        except KeyError:
            print("İlgili tarih için veri bulunmamaktadır!")
            return __pd.DataFrame()
        else:
            return df_uecm


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
    while __dogrulama.baslangic_bitis_tarih_dogrulama(baslangic_tarihi, bitis_tarihi):
        try:
            resp = __requests.get(
                __transparency_url + "load-estimation-plan" + "?startDate=" + baslangic_tarihi +
                "&endDate=" + bitis_tarihi, headers=__headers)
            list_tuketim = resp.json()["body"]["loadEstimationPlanList"]
            df_tuketim = __pd.DataFrame(list_tuketim)
            df_tuketim["Saat"] = df_tuketim["date"].apply(lambda h: int(h[11:13]))
            df_tuketim["Tarih"] = __pd.to_datetime(df_tuketim["date"].apply(lambda d: d[:10]))
            df_tuketim.rename(index=str, columns={"lep": "Tüketim"}, inplace=True)
            df_tuketim = df_tuketim[["Tarih", "Saat", "Tüketim"]]
        except __requests.exceptions.RequestException as e:
            print(e)
        except KeyError:
            return print("İlgili tarih için veri bulunmamaktadır!")
        else:
            return df_tuketim


def serbest_tuketici_sayisi():
    """
        Aylık serbest tüketici sayıları bilgisini vermektedir.

        Parametreler
        ------------

        Geri Dönüş Değeri
        -----------------
        Serbest Tüketici Sayısı (Tarih, Serbest Tüketici Sayısı, Artış Oranı)

        """
    while True:
        try:
            resp = __requests.get(__transparency_url + "eligible-consumer-quantity", headers=__headers)
            list_st = resp.json()["body"]["eligibleConsumerQuantityList"]
            df_st = __pd.DataFrame(list_st)
            df_st["Tarih"] = __pd.to_datetime(df_st["date"].apply(lambda d: d[:10]))
            df_st.rename(index=str,
                         columns={"meterQuantity": "Serbest Tüketici Sayısı", "meterIncreaseRate": "Artış Oranı"},
                         inplace=True)
            df_st = df_st[["Tarih", "Serbest Tüketici Sayısı", "Artış Oranı"]]
        except __requests.exceptions.RequestException as e:
            print(e)
        except KeyError:
            return print("İlgili tarih için veri bulunmamaktadır!")
        else:
            return df_st


def dagitim_bolgeleri():
    """
        Dağıtım bölgelerine dair bilgileri vermektedir.

        Parametreler
        ------------
        baslangic_tarihi : %YYYY-%AA-%GG formatında başlangıç tarihi (Varsayılan: bugün)
        bitis_tarihi     : %YYYY-%AA-%GG formatında bitiş tarihi (Varsayılan: bugün)

        Geri Dönüş Değeri
        -----------------
        Yük Tahmin Planı (Tarih, Saat, Tüketim)

        """
    while True:
        try:
            resp = __requests.get(__transparency_url + "distribution", headers=__headers)
            list_dagitim = resp.json()["body"]["distributionList"]
            df_dagitim = __pd.DataFrame(list_dagitim)
            df_dagitim.rename(index=str,
                              columns={"id": "Id", "name": "Dağıtım Şirket Adı"},
                              inplace=True)
        except __requests.exceptions.RequestException as e:
            print(e)
        except KeyError:
            return print("İlgili tarih için veri bulunmamaktadır!")
        else:
            return df_dagitim
