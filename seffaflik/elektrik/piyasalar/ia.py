import pandas as __pd
import datetime as __dt
from multiprocessing import Pool as __Pool
import multiprocessing as __mp
from functools import reduce as __red
import logging as __logging

from seffaflik.__ortak.__araclar import make_requests as __make_requests
from seffaflik.__ortak import __araclar as __araclar, __dogrulama as __dogrulama
from seffaflik.elektrik.uretim import organizasyonlar as __organizasyonlar

__first_part_url = "market/"


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
            particular_url = \
                __first_part_url + "bilateral-contract-sell" + "?startDate=" + baslangic_tarihi + "&endDate=" + \
                bitis_tarihi + "&eic=" + organizasyon_eic
            json = __make_requests(particular_url)
            df_arz = __pd.DataFrame(json["body"]["bilateralContractSellList"])
            particular_url = \
                __first_part_url + "bilateral-contract-buy" + "?startDate=" + baslangic_tarihi + "&endDate=" + \
                bitis_tarihi + "&eic=" + organizasyon_eic
            json = __make_requests(particular_url)
            df_talep = __pd.DataFrame(json["body"]["bilateralContractBuyList"])
            df = __araclar.__merge_ia_dfs_evenif_empty(df_arz, df_talep)
            df["Saat"] = df["date"].apply(lambda h: int(h[11:13]))
            df["Tarih"] = __pd.to_datetime(df["date"].apply(lambda d: d[:10]))
            df = df[["Tarih", "Saat", "Talep Miktarı", "Arz Miktarı"]]
        except (KeyError, TypeError):
            return __pd.DataFrame()
        else:
            return df


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
        list_org = __organizasyonlar()[["EIC Kodu", "Kısa Adı"]].to_dict("records")
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
        particular_url = \
            __first_part_url + "bilateral-contract-sell" + "?startDate=" + baslangic_tarihi + "&endDate=" + \
            bitis_tarihi + "&eic=" + org["EIC Kodu"]
        json = __make_requests(particular_url)
        df_arz = __pd.DataFrame(json["body"]["bilateralContractSellList"])

        particular_url = \
            __first_part_url + "bilateral-contract-buy" + "?startDate=" + baslangic_tarihi + "&endDate=" + \
            bitis_tarihi + "&eic=" + org["EIC Kodu"]
        json = __make_requests(particular_url)
        df_talep = __pd.DataFrame(json["body"]["bilateralContractBuyList"])
        df = __araclar.__merge_ia_dfs_evenif_empty(df_arz, df_talep)
        df["Saat"] = df["date"].apply(lambda h: int(h[11:13]))
        df["Tarih"] = __pd.to_datetime(df["date"].apply(lambda d: d[:10]))
        df[org["Kısa Adı"]] = df["Talep Miktarı"] - df["Arz Miktarı"]
        df = df[["Tarih", "Saat", org["Kısa Adı"]]]
    except (KeyError, TypeError):
        return __pd.DataFrame()
    else:
        return df


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
        particular_url = __first_part_url + "bilateral-contract-sell" + "?startDate=" + baslangic_tarihi + "&endDate=" \
                         + bitis_tarihi + "&eic=" + org["EIC Kodu"]
        json = __make_requests(particular_url)
        df = __pd.DataFrame(json["body"]["bilateralContractSellList"])
        df["Saat"] = df["date"].apply(lambda h: int(h[11:13]))
        df["Tarih"] = __pd.to_datetime(df["date"].apply(lambda d: d[:10]))
        df.rename(index=str, columns={"quantity": org["Kısa Adı"]}, inplace=True)
        df = df[["Tarih", "Saat", org["Kısa Adı"]]]
    except (KeyError, TypeError):
        return __pd.DataFrame()
    else:
        return df


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
        particular_url = __first_part_url + "bilateral-contract-buy" + "?startDate=" + baslangic_tarihi + "&endDate=" \
                         + bitis_tarihi + "&eic=" + org["EIC Kodu"]
        json = __make_requests(particular_url)
        df = __pd.DataFrame(json["body"]["bilateralContractBuyList"])
        df["Saat"] = df["date"].apply(lambda h: int(h[11:13]))
        df["Tarih"] = __pd.to_datetime(df["date"].apply(lambda d: d[:10]))
        df.rename(index=str, columns={"quantity": org["Kısa Adı"]}, inplace=True)
        df = df[["Tarih", "Saat", org["Kısa Adı"]]]
    except (KeyError, TypeError):
        return __pd.DataFrame()
    else:
        return df
