import requests as __requests
from requests import ConnectionError as __ConnectionError
from requests.exceptions import HTTPError as __HTTPError, RequestException as __RequestException, Timeout as __Timeout
import pandas as __pd
import logging as __logging
from seffaflik.__ortak.__anahtar import HEADERS
from seffaflik.__ortak.__dogrulama import __check_http_error
from seffaflik.__ortak.__parametreler import __timeout, __requestsConnectionErrorLogging, \
    __requestsTimeoutErrorLogging, __request_error


def __merge_ia_dfs_evenif_empty(df_arz, df_talep):
    """
    This method merges the given dataframes. If one of them is empty, then set them zero. If both of them is empty,
    return empty dataframe.

    Parameters
    ----------
    df_arz  : ikili anlaşma arz miktarıları
    df_talep: ikili anlaşma talep miktarıları

    Return
    ------
    Arz/Talep Miktarları
    """

    df = __pd.DataFrame()
    if len(df_arz) > 0 and len(df_talep) > 0:
        df_arz.rename(index=str, columns={"quantity": "Arz Miktarı"}, inplace=True)
        df_talep.rename(index=str, columns={"quantity": "Talep Miktarı"}, inplace=True)
        df = __pd.merge(df_arz, df_talep, on="date")
    elif len(df_arz) == 0 and len(df_talep) > 0:
        df_talep.rename(index=str, columns={"quantity": "Talep Miktarı"}, inplace=True)
        df_talep["Arz Miktarı"] = 0
        df = df_talep
    elif len(df_arz) > 0 and len(df_talep) == 0:
        df_arz.rename(index=str, columns={"quantity": "Arz Miktarı"}, inplace=True)
        df_arz["Talep Miktarı"] = 0
        df = df_arz
    return df


def make_requests(corresponding_url):
    main_url = "https://api.epias.com.tr/epias/exchange/transparency/"
    try:
        resp = __requests.get(main_url + corresponding_url, headers=HEADERS, timeout=__timeout)
        resp.raise_for_status()
        json = resp.json()
    except __ConnectionError:
        __logging.error(__requestsConnectionErrorLogging, exc_info=False)
    except __Timeout:
        __logging.error(__requestsTimeoutErrorLogging, exc_info=False)
    except __HTTPError as e:
        __check_http_error(e.response.status_code)
    except __RequestException:
        __logging.error(__request_error, exc_info=False)
    else:
        return json
