import requests as __requests
import pandas as __pd
import datetime as __dt
from seffaflik.ortak import parametreler as __param
from seffaflik.ortak import anahtar as __api
from seffaflik.ortak import dogrulama as __dogrulama

__hata = __param.BILINMEYEN_HATA_MESAJI
__transparency_url = __param.SEFFAFLIK_URL + "market/"
__headers = __api.HEADERS


def smf(baslangic_tarihi=__dt.datetime.today().strftime("%Y-%m-%d"),
        bitis_tarihi=__dt.datetime.today().strftime("%Y-%m-%d")):
    """
    İlgili tarih aralığı için saatlik sistem marjinal fiyatını ve sistem yönünü vermektedir.

    Parametreler
    ----------
    baslangic_tarihi : %YYYY-%AA-%GG formatında başlangıç tarihi (Varsayılan: bugün)
    bitis_tarihi   : %YYYY-%AA-%GG formatında bitiş tarihi (Varsayılan: bugün)

    Geri Dönüş Değeri
    ------
    Sistem Marjinal Fiyatı, Sistem Yönü
    """
    while __dogrulama.baslangic_bitis_tarih_dogrulama(baslangic_tarihi, bitis_tarihi):
        try:
            resp = __requests.get(
                __transparency_url + "smp" + "?startDate=" + baslangic_tarihi + "&endDate=" + bitis_tarihi,
                headers=__headers)
            list_smf = resp.json()["body"]["smpList"]
            df_smf = __pd.DataFrame(list_smf)
            df_smf["Saat"] = df_smf["date"].apply(lambda h: int(h[11:13]))
            df_smf["Tarih"] = __pd.to_datetime(df_smf["date"].apply(lambda d: d[:10]))
            df_smf.drop(["date", "nextHour"], axis=1, inplace=True)
            df_smf.rename(index=str,
                          columns={"price": "SMF", "smpDirection": "Sistem Yönü"},
                          inplace=True)
            df_smf = df_smf[["Tarih", "Saat", "SMF", "Sistem Yönü"]]
        except __requests.exceptions.RequestException as e:
            print(e)
        except KeyError:
            return print("İlgili tarih için veri bulunamamıştır!")
        else:
            return df_smf


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
    YAL/YAT Talimat Miktarları
    """
    while __dogrulama.baslangic_bitis_tarih_dogrulama(baslangic_tarihi, bitis_tarihi):
        try:
            resp = __requests.get(__transparency_url + "bpm-order-summary" +
                                  "?startDate=" + baslangic_tarihi + "&endDate=" + bitis_tarihi,
                                  headers=__headers)
            list_hacim = resp.json()["body"]["bpmorderSummaryList"]
            df_hacim = __pd.DataFrame(list_hacim)
            df_hacim["Saat"] = df_hacim["date"].apply(lambda h: int(h[11:13]))
            df_hacim["Tarih"] = __pd.to_datetime(df_hacim["date"].apply(lambda d: d[:10]))
            df_hacim.drop(["date", "nextHour"], axis=1, inplace=True)
            df_hacim.rename(index=str,
                            columns={"net": "Net", "upRegulationZeroCoded": "YAL (0)",
                                     "upRegulationOneCoded": "YAL (1)", "upRegulationTwoCoded": "YAL (2)",
                                     "downRegulationZeroCoded": "YAT (0)", "downRegulationOneCoded": "YAT (1)",
                                     "downRegulationTwoCoded": "YAT (2)", "upRegulationDelivered": "Teslim Edilen YAL",
                                     "downRegulationDelivered": "Teslim Edilen YAT", "direction": "Sistem Yönü"},
                            inplace=True)
            df_hacim["Sistem Yönü"] = df_hacim["Sistem Yönü"].map(
                {"IN_BALANCE": "Dengede", "ENERGY_SURPLUS": "Enerji Fazlası", "ENERGY_DEFICIT": "Enerji Açığı"})
            df_hacim = df_hacim[
                ["Tarih", "Saat", "Net", "YAL (0)", "YAL (1)", "YAL (2)", "Teslim Edilen YAL", "YAT (0)", "YAT (1)",
                 "YAT (2)", "Teslim Edilen YAT", "Sistem Yönü"]]
        except __requests.exceptions.RequestException as e:
            print(e)
        except KeyError:
            return print("İlgili tarih için veri bulunamamıştır!")
        else:
            return df_hacim
