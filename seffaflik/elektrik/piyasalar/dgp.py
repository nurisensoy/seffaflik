import pandas as __pd
import datetime as __dt

from seffaflik.__ortak.__araclar import make_requests as __make_requests
from seffaflik.__ortak import __dogrulama as __dogrulama

__first_part_url = "market/"


def smf(baslangic_tarihi=__dt.datetime.today().strftime("%Y-%m-%d"),
        bitis_tarihi=__dt.datetime.today().strftime("%Y-%m-%d")):
    """
    İlgili tarih aralığı için saatlik sistem marjinal fiyatını (SMF) ve sistem yönünü vermektedir.

    Parametreler
    ----------
    baslangic_tarihi : %YYYY-%AA-%GG formatında başlangıç tarihi (Varsayılan: bugün)
    bitis_tarihi   : %YYYY-%AA-%GG formatında bitiş tarihi (Varsayılan: bugün)

    Geri Dönüş Değeri
    ------
    Sistem Marjinal Fiyatı, Sistem Yönü
    """
    if __dogrulama.__baslangic_bitis_tarih_dogrulama(baslangic_tarihi, bitis_tarihi):
        try:
            particular_url = \
                __first_part_url + "smp" + "?startDate=" + baslangic_tarihi + "&endDate=" + bitis_tarihi
            json = __make_requests(particular_url)
            df = __pd.DataFrame(json["body"]["smpList"])
            df["Saat"] = df["date"].apply(lambda h: int(h[11:13]))
            df["Tarih"] = __pd.to_datetime(df["date"].apply(lambda d: d[:10]))
            df.rename(index=str,
                      columns={"price": "SMF", "smpDirection": "Sistem Yönü"},
                      inplace=True)
            df = df[["Tarih", "Saat", "SMF", "Sistem Yönü"]]
        except (KeyError, TypeError):
            return __pd.DataFrame()
        else:
            return df


def hacim(baslangic_tarihi=__dt.datetime.today().strftime("%Y-%m-%d"),
          bitis_tarihi=__dt.datetime.today().strftime("%Y-%m-%d")):
    """
    İlgili tarih aralığı için dengeleme güç piyasası YAL/YAT talimat miktar bilgilerini vermektedir.

    Parametreler
    ----------
    baslangic_tarihi : %YYYY-%AA-%GG formatında başlangıç tarihi (Varsayılan: bugün)
    bitis_tarihi   : %YYYY-%AA-%GG formatında bitiş tarihi (Varsayılan: bugün)

    Geri Dönüş Değeri
    ------
    YAL/YAT Talimat Miktarları (MWh)
    """
    if __dogrulama.__baslangic_bitis_tarih_dogrulama(baslangic_tarihi, bitis_tarihi):
        try:
            particular_url = \
                __first_part_url + "bpm-order-summary" + "?startDate=" + baslangic_tarihi + "&endDate=" + bitis_tarihi
            json = __make_requests(particular_url)
            df = __pd.DataFrame(json["body"]["bpmorderSummaryList"])
            df["Saat"] = df["date"].apply(lambda h: int(h[11:13]))
            df["Tarih"] = __pd.to_datetime(df["date"].apply(lambda d: d[:10]))
            df.rename(index=str,
                      columns={"net": "Net", "upRegulationZeroCoded": "YAL (0)",
                               "upRegulationOneCoded": "YAL (1)", "upRegulationTwoCoded": "YAL (2)",
                               "downRegulationZeroCoded": "YAT (0)", "downRegulationOneCoded": "YAT (1)",
                               "downRegulationTwoCoded": "YAT (2)", "upRegulationDelivered": "Teslim Edilen YAL",
                               "downRegulationDelivered": "Teslim Edilen YAT", "direction": "Sistem Yönü"},
                      inplace=True)
            df["Sistem Yönü"] = df["Sistem Yönü"].map(
                {"IN_BALANCE": "Dengede", "ENERGY_SURPLUS": "Enerji Fazlası", "ENERGY_DEFICIT": "Enerji Açığı"})
            df = df[
                ["Tarih", "Saat", "Net", "YAL (0)", "YAL (1)", "YAL (2)", "Teslim Edilen YAL", "YAT (0)", "YAT (1)",
                 "YAT (2)", "Teslim Edilen YAT", "Sistem Yönü"]]
        except (KeyError, TypeError):
            return __pd.DataFrame()
        else:
            return df
