import requests as __requests
from requests import ConnectionError as __ConnectionError
from requests.exceptions import HTTPError as __HTTPError, RequestException as __RequestException, Timeout as __Timeout
import pandas as __pd
import datetime as __dt
import logging as __logging

from seffaflik.__ortak import __dogrulama, __parametreler, __anahtar

__transparency_url = __parametreler.SEFFAFLIK_URL + "market/"
__headers = __anahtar.HEADERS


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
            resp = __requests.get(
                __transparency_url + "energy-imbalance-hourly" + "?startDate=" + baslangic_tarihi + "&endDate=" +
                bitis_tarihi, headers=__headers, timeout=__parametreler.__timeout)
            resp.raise_for_status()
            list_dengesizlik = resp.json()["body"]["energyImbalances"]
            df_dengesizlik = __pd.DataFrame(list_dengesizlik)
            df_dengesizlik["Saat"] = df_dengesizlik["date"].apply(lambda h: int(h[11:13]))
            df_dengesizlik["Tarih"] = __pd.to_datetime(df_dengesizlik["date"].apply(lambda d: d[:10]))
            df_dengesizlik.rename(index=str, columns={"positiveImbalance": "Pozitif Dengesizlik Miktarı (MWh)",
                                                      "negativeImbalance": "Negatif Dengesizlik Miktarı (MWh)",
                                                      "positiveImbalanceIncome": "Pozitif Dengesizlik Tutarı (TL)",
                                                      "negativeImbalanceIncome": "Negatif Dengesizlik Tutarı (TL)"},
                                  inplace=True)
            df_dengesizlik = df_dengesizlik[
                ["Tarih", "Saat", "Pozitif Dengesizlik Miktarı (MWh)", "Negatif Dengesizlik Miktarı (MWh)",
                 "Pozitif Dengesizlik Tutarı (TL)", "Negatif Dengesizlik Tutarı (TL)"]]
            df_dengesizlik.dropna(subset=df_dengesizlik.columns[2:], how="all", inplace=True)
        except __ConnectionError:
            __logging.error(__parametreler.__requestsConnectionErrorLogging, exc_info=False)
        except __Timeout:
            __logging.error(__parametreler.__requestsTimeoutErrorLogging, exc_info=False)
        except __HTTPError as e:
            __dogrulama.__check_http_error(e.response.status_code)
        except __RequestException:
            __logging.error(__parametreler.__request_error, exc_info=False)
        except KeyError:
            return __pd.DataFrame()
        else:
            return df_dengesizlik
