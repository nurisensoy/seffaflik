import pandas as __pd
import datetime as __dt
from seffaflik.__ortak.__araclar import make_requests as __make_requests
from seffaflik.__ortak import __dogrulama as __dogrulama
from functools import reduce as __red

__first_part_url = "market/"


def primer_sekonder_fiyat_miktar(baslangic_tarihi=__dt.datetime.today().strftime("%Y-%m-%d"),
                                 bitis_tarihi=__dt.datetime.today().strftime("%Y-%m-%d")):
    """
    İlgili tarih aralığı için saatlik primer/sekonder frekans rezerv fiyat/miktar bilgilerini vermektedir.

    Parametreler
    ------------
    baslangic_tarihi : %YYYY-%AA-%GG formatında başlangıç tarihi (Varsayılan: bugün)
    bitis_tarihi     : %YYYY-%AA-%GG formatında bitiş tarihi (Varsayılan: bugün)

    Geri Dönüş Değeri
    -----------------
    PFK/SFK Fiyat/Yükümlülük
    """
    if __dogrulama.__baslangic_bitis_tarih_dogrulama(baslangic_tarihi, bitis_tarihi):
        try:
            pfk_miktar = primer_frekans_rezerv_miktari(baslangic_tarihi, bitis_tarihi)
            pfk_fiyat = primer_rezerv_fiyati(baslangic_tarihi, bitis_tarihi)
            sfk_miktar = sekonder_frekans_rezerv_miktari(baslangic_tarihi, bitis_tarihi)
            sfk_fiyat = sekonder_rezerv_fiyati(baslangic_tarihi, bitis_tarihi)
            df = __red(lambda left, right: __pd.merge(left, right, how="outer", on=["Tarih", "Saat"], sort=True),
                       [pfk_miktar, pfk_fiyat, sfk_miktar, sfk_fiyat])
        except KeyError:
            return __pd.DataFrame()
        else:
            return df


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
            particular_url = __first_part_url + "pfc-amount" + "?startDate=" + baslangic_tarihi + "&endDate=" + \
                             bitis_tarihi
            json = __make_requests(particular_url)
            df = __pd.DataFrame(json["body"]["frequencyReservePriceList"])
            df["Saat"] = df["effectiveDate"].apply(lambda h: int(h[11:13]))
            df["Tarih"] = __pd.to_datetime(df["effectiveDate"].apply(lambda d: d[:10]))
            df.rename(index=str, columns={"totalAmount": "PFK Yükümlülük (MWh)"}, inplace=True)
            df = df[["Tarih", "Saat", "PFK Yükümlülük (MWh)"]]
        except KeyError:
            return __pd.DataFrame()
        else:
            return df


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
            particular_url = __first_part_url + "sfc-amount" + "?startDate=" + baslangic_tarihi + "&endDate=" + \
                             bitis_tarihi
            json = __make_requests(particular_url)
            df = __pd.DataFrame(json["body"]["frequencyReservePriceList"])
            df["Saat"] = df["effectiveDate"].apply(lambda h: int(h[11:13]))
            df["Tarih"] = __pd.to_datetime(df["effectiveDate"].apply(lambda d: d[:10]))
            df.rename(index=str, columns={"totalAmount": "SFK Yükümlülük (MWh)"}, inplace=True)
            df = df[["Tarih", "Saat", "SFK Yükümlülük (MWh)"]]
        except KeyError:
            return __pd.DataFrame()
        else:
            return df


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
            particular_url = __first_part_url + "pfc-price" + "?startDate=" + baslangic_tarihi + "&endDate=" + \
                             bitis_tarihi
            json = __make_requests(particular_url)
            df = __pd.DataFrame(json["body"]["frequencyReservePriceList"])
            df["Saat"] = df["effectiveDate"].apply(lambda h: int(h[11:13]))
            df["Tarih"] = __pd.to_datetime(df["effectiveDate"].apply(lambda d: d[:10]))
            df.rename(index=str, columns={"price": "PFK Fiyat (TL/MWh)"}, inplace=True)
            df = df[["Tarih", "Saat", "PFK Fiyat (TL/MWh)"]]
        except KeyError:
            return __pd.DataFrame()
        else:
            return df


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
            particular_url = __first_part_url + "sfc-price" + "?startDate=" + baslangic_tarihi + "&endDate=" + \
                             bitis_tarihi
            json = __make_requests(particular_url)
            df = __pd.DataFrame(json["body"]["frequencyReservePriceList"])
            df["Saat"] = df["effectiveDate"].apply(lambda h: int(h[11:13]))
            df["Tarih"] = __pd.to_datetime(df["effectiveDate"].apply(lambda d: d[:10]))
            df.rename(index=str, columns={"price": "SFK Fiyat (TL/MWh)"}, inplace=True)
            df = df[["Tarih", "Saat", "SFK Fiyat (TL/MWh)"]]
        except KeyError:
            return __pd.DataFrame()
        else:
            return df
