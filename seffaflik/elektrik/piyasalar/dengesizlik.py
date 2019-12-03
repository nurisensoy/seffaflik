import pandas as __pd
import datetime as __dt
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
