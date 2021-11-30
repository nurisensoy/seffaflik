import pandas as __pd
import datetime as __dt
from multiprocessing import Pool as __Pool
import multiprocessing as __mp
from seffaflik.__ortak import __dogrulama
from seffaflik.__ortak.__araclar import make_requests as __make_requests

__first_part_url = "market/"


def dengesizlik(baslangic_tarihi=__dt.datetime.today().strftime("%Y-%m-%d"),
                bitis_tarihi=__dt.datetime.today().strftime("%Y-%m-%d")):
    """
    İlgili tarih aralığı için dengeden sorumlu grupların oluşturmuş olduğu saatlik enerji dengesizlik miktarı ve
    tutarlarını vermektedir.

    Parametreler
    ------------
    baslangic_tarihi : %YYYY-%AA-%GG formatında başlangıç tarihi (Varsayılan: bugün)
    bitis_tarihi     : %YYYY-%AA-%GG formatında bitiş tarihi (Varsayılan: bugün)

    Geri Dönüş Değeri
    -----------------
    Dengesizlik Miktarı ve Tutarı (MWh, TL)
    """
    if __dogrulama.__baslangic_bitis_tarih_dogrulama(baslangic_tarihi, bitis_tarihi):
        try:
            particular_url = __first_part_url + "energy-imbalance-hourly" + "?startDate=" + baslangic_tarihi + \
                             "&endDate=" + bitis_tarihi
            json = __make_requests(particular_url)
            df = __pd.DataFrame(json["body"]["energyImbalances"])
            df["Saat"] = df["date"].apply(lambda h: int(h[11:13]))
            df["Tarih"] = __pd.to_datetime(df["date"].apply(lambda d: d[:10]))
            df.rename(index=str, columns={"positiveImbalance": "Pozitif Dengesizlik Miktarı (MWh)",
                                          "negativeImbalance": "Negatif Dengesizlik Miktarı (MWh)",
                                          "positiveImbalanceIncome": "Pozitif Dengesizlik Tutarı (TL)",
                                          "negativeImbalanceIncome": "Negatif Dengesizlik Tutarı (TL)"},
                      inplace=True)
            df = df[
                ["Tarih", "Saat", "Pozitif Dengesizlik Miktarı (MWh)", "Negatif Dengesizlik Miktarı (MWh)",
                 "Pozitif Dengesizlik Tutarı (TL)", "Negatif Dengesizlik Tutarı (TL)"]]
            df.dropna(subset=df.columns[2:], how="all", inplace=True)
        except (KeyError, TypeError):
            return __pd.DataFrame()
        else:
            return df


def dengeden_sorumlu_gruplar(baslangic_tarihi=__dt.datetime.today().strftime("%Y-%m-%d"),
                             bitis_tarihi=__dt.datetime.today().strftime("%Y-%m-%d")):
    """
    İlgili tarih aralığı için dengeden sorumlu grupların listesini vermektedir.

    Parametreler
    ------------
    baslangic_tarihi : %YYYY-%AA-%GG formatında başlangıç tarihi (Varsayılan: bugün)
    bitis_tarihi     : %YYYY-%AA-%GG formatında bitiş tarihi (Varsayılan: bugün)

    Geri Dönüş Değeri
    -----------------
    Dengeden Sorumlu Gruplar
    """
    if __dogrulama.__baslangic_bitis_tarih_dogrulama(baslangic_tarihi, bitis_tarihi):
        try:
            particular_url = __first_part_url + "dsg-imbalance-quantity-orgaization-list" + "?startDate=" + \
                             baslangic_tarihi + "&endDate=" + bitis_tarihi
            json = __make_requests(particular_url)
            df = __pd.DataFrame(json["body"]["orgList"])
            df.rename(index=str, columns={"organizationId": "Id", "organizationName": "Adı",
                                          "organizationStatus": "Durum", "organizationETSOCode": "EIC Kodu",
                                          "organizationShortName": "Kısa Adı"},
                      inplace=True)
        except (KeyError, TypeError):
            return __pd.DataFrame()
        else:
            return df


def tum_dengeden_sorumlu_gruplar_dengesizlik(baslangic_tarihi=__dt.datetime.today().strftime("%Y-%m-%d"),
                                             bitis_tarihi=__dt.datetime.today().strftime("%Y-%m-%d")):
    """
    İlgili tarih aralığı için tüm dengeden sorumlu grupların aylık toplam dengesizlik miktarı bilgisini vermektedir.

    Parametreler
    ------------
    baslangic_tarihi : %YYYY-%AA-%GG formatında başlangıç tarihi (Varsayılan: bugün)
    bitis_tarihi     : %YYYY-%AA-%GG formatında bitiş tarihi (Varsayılan: bugün)

    Geri Dönüş Değeri
    -----------------
    Tüm DSG Dengesizlik Miktarı (Tarih, Saat, Miktar)
    """
    if __dogrulama.__baslangic_bitis_tarih_dogrulama(baslangic_tarihi, bitis_tarihi):
        organization = dengeden_sorumlu_gruplar(baslangic_tarihi, bitis_tarihi)
        list_organization = organization[["Id", "Kısa Adı"]].to_dict("records")
        list_organization_len = len(list_organization)
        list_organization = list(
            zip([baslangic_tarihi] * list_organization_len, [bitis_tarihi] * list_organization_len, list_organization))
        list_organization = list(map(list, list_organization))
        with __Pool(__mp.cpu_count()) as p:
            list_df_unit = p.starmap(__dengesizlik_dsg, list_organization, chunksize=1)
        list_df_unit = list(filter(lambda x: len(x) > 0, list_df_unit))
        df_unit = __pd.concat(list_df_unit)
        return df_unit.sort_values(["Tarih", "DST"])


def __dengesizlik_dsg(baslangic_tarihi, bitis_tarihi, organization):
    """
    İlgili tarih aralığı ve dengeden sorumlu gruplar için aylık toplam dengesizlik miktarı bilgisi vermektedir.

    Parametreler
    ------------
    baslangic_tarihi : %YYYY-%AA-%GG formatında başlangıç tarihi (Varsayılan: bugün)
    bitis_tarihi     : %YYYY-%AA-%GG formatında bitiş tarihi (Varsayılan: bugün)
    organization     : metin yada tam sayı formatında santral id

    Geri Dönüş Değeri
    -----------------
    DSG Dengesizlik Miktarı
    """
    try:
        particular_url = __first_part_url + "dsg-imbalance-quantity" + "?startDate=" + \
                         baslangic_tarihi + "&endDate=" + bitis_tarihi + "&organizationId=" + \
                         str(organization["Id"])
        json = __make_requests(particular_url)
        df = __pd.DataFrame(json["body"]["imbalanceQuantityList"])
        df["Tarih"] = __pd.to_datetime(df["date"].apply(lambda d: d[:10]))
        df["DST"] = organization["Kısa Adı"]
        df.rename(columns={"positiveImbalanceQuantity": "Pozitif Dengesizlik Miktarı (MWh)",
                           "negativImbalanceQuantity": "Negatif Dengesizlik Miktarı (MWh)"}, inplace=True)
        df = df[["Tarih", "DST", "Pozitif Dengesizlik Miktarı (MWh)", "Negatif Dengesizlik Miktarı (MWh)"]]
    except (KeyError, TypeError):
        return __pd.DataFrame()
    else:
        return df
