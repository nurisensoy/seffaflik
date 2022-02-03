import pandas as __pd
import datetime as __dt

from seffaflik.__ortak.__araclar import make_requests as __make_requests
from seffaflik.__ortak import __dogrulama as __dogrulama

__first_part_url = "transmission/"


def entso_kodlari(tarih=__dt.datetime.now().strftime("%Y-%m-%d")):
    """
    İlgili tarih için Avrupa Elektrik İletim Sistemi İşletmecileri Ağı'nın piyasadaki organizasyonlara Avrupa
    standartlarına uygun formatta tanımladığı Enerji Tanımlama Kodlarını vermektedir.

    Parametre
    ----------
    tarih : %YYYY-%AA-%GG formatında tarih (Varsayılan: bugün)

    Geri Dönüş Değeri
    -----------------
    ENTSO-E (X) Kodları (Adı, EIC Kodu, Kısa Adı)
    """
    if __dogrulama.__tarih_dogrulama(tarih):
        try:
            particular_url = __first_part_url + "ents-organization?organizationId=-1&period=" + tarih
            json = __make_requests(particular_url)
            df = __pd.DataFrame(json["body"]["entsOrganizationList"])
            df.rename(index=str, columns={"organizationName": "Adı", "organizationShortName": "Kısa Adı",
                                          "etsorganizationCode": "EIC Kodu"}, inplace=True)
            df = df[["Adı", "EIC Kodu", "Kısa Adı"]]
        except (KeyError, TypeError):
            return __pd.DataFrame()
        else:
            return df


def sifir_bakiye(baslangic_tarihi=__dt.datetime.today().strftime("%Y-%m-%d"),
                 bitis_tarihi=__dt.datetime.today().strftime("%Y-%m-%d")):
    """
    İlgili tarih aralığı için aylık sıfır bakiye düzeltme tutarı bilgisini vermektedir.

    Parametreler
    ------------
    baslangic_tarihi : %YYYY-%AA-%GG formatında başlangıç tarihi (Varsayılan: bugün)
    bitis_tarihi     : %YYYY-%AA-%GG formatında bitiş tarihi (Varsayılan: bugün)

    Geri Dönüş Değeri
    -----------------
    Sıfır Bakiye Düzeltme Tutarı (TL)
    """
    if __dogrulama.__baslangic_bitis_tarih_dogrulama(baslangic_tarihi, bitis_tarihi):
        try:
            particular_url = __first_part_url + "zero-balance" + "?startDate=" + baslangic_tarihi + \
                             "&endDate=" + bitis_tarihi
            json = __make_requests(particular_url)
            df = __pd.DataFrame(json["body"]["zeroBalances"])
            df = df.rename(columns={"date": "Tarih", "zeroBalanceAdjustment": "Toplam", "downRegulation": "YAT",
                                    "upRegulation": "YAL", "manual": "Manuel",
                                    "negativeImbalance": "Enerji Dengesizliği Tutarı", "kupst": "KÜPST",
                                    "renewableImbalance": "YEK Enerji Dengesizliği Tutarı"})
            df["Tarih"] = df["Tarih"].apply(lambda s: __pd.to_datetime(s[:10]))
        except (KeyError, TypeError):
            return __pd.DataFrame()
        else:
            return df


def kayip_katsayisi(baslangic_tarihi=__dt.datetime.today().strftime("%Y-%m-%d"),
                    bitis_tarihi=__dt.datetime.today().strftime("%Y-%m-%d")):
    """
    İlgili tarih aralığı için uzlaştırmaya esas saatlik iletim sistemi kayıp katsayısı (ISKK) bilgisini vermektedir.

    Parametreler
    ------------
    baslangic_tarihi : %YYYY-%AA-%GG formatında başlangıç tarihi (Varsayılan: bugün)
    bitis_tarihi     : %YYYY-%AA-%GG formatında bitiş tarihi (Varsayılan: bugün)

    Geri Dönüş Değeri
    -----------------
    ISKK
    """
    if __dogrulama.__baslangic_bitis_tarih_dogrulama(baslangic_tarihi, bitis_tarihi):
        try:
            particular_url = __first_part_url + "transmission-system-loss-factor" + "?startDate=" + baslangic_tarihi + \
                             "&endDate=" + bitis_tarihi
            json = __make_requests(particular_url)
            df = __pd.DataFrame(json["body"]["transmissionSystemLossFactorList"])
            df = df.rename(
                columns={"date": "Tarih", "firstVersionValue": "İlk Versiyon", "lastVersionValue": "Son Versiyon",
                         "firstVersionDate": "İlk Versiyon Tarihi", "lastVersionDate": "Son Versiyon Tarihi"})
            df["Saat"] = df["Tarih"].apply(lambda h: int(h[11:13]))
            df["Tarih"] = __pd.to_datetime(df["Tarih"].apply(lambda d: d[:10]))
            df["İlk Versiyon Tarihi"] = df["İlk Versiyon Tarihi"].apply(lambda s: __pd.to_datetime(s[:10]))
            df["Son Versiyon Tarihi"] = df["Son Versiyon Tarihi"].apply(lambda s: __pd.to_datetime(s[:10]))
        except (KeyError, TypeError):
            return __pd.DataFrame()
        else:
            return df[["Tarih", "Saat", "İlk Versiyon", "Son Versiyon", "İlk Versiyon Tarihi", "Son Versiyon Tarihi"]]


def kisit_maliyeti(baslangic_tarihi=__dt.datetime.today().strftime("%Y-%m-%d"),
                   bitis_tarihi=__dt.datetime.today().strftime("%Y-%m-%d"),
                   talimat_tipi="YAL_YAT"):
    """
    İlgili tarih aralığı ve talimat tipi için şehir bazlı toplam talimat miktar/maliyet bilgisini vermektedir.

    Parametreler
    ------------
    baslangic_tarihi : %YYYY-%AA-%GG formatında başlangıç tarihi (Varsayılan: bugün)
    bitis_tarihi     : %YYYY-%AA-%GG formatında bitiş tarihi (Varsayılan: bugün)
    talimat_tipi     : metin formatında talimat tipi (YAL, YAT, YAL_YAT) (Varsayılan: "YAL_YAT")

    Geri Dönüş Değeri
    -----------------
    Kısıt Miktar/Maliyet
    """
    if __dogrulama.__baslangic_bitis_tarih_dogrulama(baslangic_tarihi, bitis_tarihi):
        try:
            particular_url = __first_part_url + "congestion-rent" + "?startDate=" + baslangic_tarihi + \
                             "&endDate=" + bitis_tarihi + "&orderType=" + talimat_tipi
            json = __make_requests(particular_url)
            df = __pd.DataFrame(json["body"]["congestionRentList"])
            df = df.rename(columns={"cityId": "Şehir Id", "cityName": "Şehir Adı", "mcpCost": "PTF Maliyeti",
                                    "smpCost": "SMF Maliyeti", "orderCount": "Talimat Sayısı",
                                    "upRegulationOrderCount": "YAL Talimat Miktarı",
                                    "downRegulationOrderCount": "YAT Talimat Miktarı",
                                    "totalOrderCount": "Toplam Talimat Miktarı"})
        except (KeyError, TypeError):
            return __pd.DataFrame()
        else:
            return df


def nomine_kapasite(baslangic_tarihi=__dt.datetime.today().strftime("%Y-%m-%d"),
                    bitis_tarihi=__dt.datetime.today().strftime("%Y-%m-%d")):
    """
    İlgili tarih aralığı için Nomine Kapasite değerleri, ithalat (satış miktarı) ve ihracat (alış miktarı) için yapılan
    ikili anlaşmaları vermektedir.

    Parametreler
    ------------
    baslangic_tarihi : %YYYY-%AA-%GG formatında başlangıç tarihi (Varsayılan: bugün)
    bitis_tarihi     : %YYYY-%AA-%GG formatında bitiş tarihi (Varsayılan: bugün)

    Geri Dönüş Değeri
    -----------------
    Nomine Kapasite
    """
    if __dogrulama.__baslangic_bitis_tarih_dogrulama(baslangic_tarihi, bitis_tarihi):
        try:
            particular_url = __first_part_url + "nominal-capacity" + "?startDate=" + baslangic_tarihi + \
                             "&endDate=" + bitis_tarihi
            json = __make_requests(particular_url)
            df = __pd.DataFrame(json["body"]["nominalCapacitiyList"])
            df["Saat"] = df["date"].apply(__pd.to_datetime).dt.hour
            df["Tarih"] = df["date"].apply(__pd.to_datetime).dt.date.apply(str)
            df = df.rename(columns={"offerQuantity": "İthalat (MWh)", "bidQuantity": "İhracat (MWh)"})
        except (KeyError, TypeError):
            return __pd.DataFrame()
        else:
            return df[["Tarih", "Saat", "İthalat (MWh)", "İhracat (MWh)"]]


def kapasite_talepleri(baslangic_tarihi=__dt.datetime.today().strftime("%Y-%m-%d"),
                       bitis_tarihi=__dt.datetime.today().strftime("%Y-%m-%d"),
                       yonler=["TRGR", "GRTR", "TRBG", "BGTR"]):
    """
    İlgili tarih aralığı ve yönler için kapasite ihalelerine ait talepleri ve tahsis edilen kapasiteleri vermektedir.

    Parametreler
    ------------
    baslangic_tarihi : %YYYY-%AA-%GG formatında başlangıç tarihi (Varsayılan: bugün)
    bitis_tarihi     : %YYYY-%AA-%GG formatında bitiş tarihi (Varsayılan: bugün)
    yonler           : list formatında yönler (Varsayılan: ["TRGR", "GRTR", "TRBG", "BGTR"])

    Geri Dönüş Değeri
    -----------------
    Kapasite Talepleri
    """
    if __dogrulama.__baslangic_bitis_tarih_dogrulama(baslangic_tarihi, bitis_tarihi):
        try:
            df_list = []
            for yon in yonler:
                particular_url = __first_part_url + "tcat-participant-capacity" + "?startDate=" + baslangic_tarihi + \
                                 "&endDate=" + bitis_tarihi + "&td=" + yon
                json = __make_requests(particular_url)
                df_list.append(__pd.DataFrame(json["body"]["data"]))
            df = __pd.concat(df_list)
            df = df.rename(columns={"auctionCode": "İhale Numarası", "td": "Yön", "transferDatetime": "Transfer Zamanı",
                                    "requestedCapacity": "Katılımcılar Tarafından Talep Edilen Kapasite(MW)",
                                    "allocatedCapacity": "Tahsis Edilen Kapasite(MW)",
                                    "clearancePrice": "Takas Fiyatı(EUR/MWh)",
                                    "participantCount": "Katılımcı Sayısı",
                                    "winnerCount": "Kazanan Katılımcı Sayısı",
                                    "bidCount": "Teklif Sayısı"})
        except (KeyError, TypeError):
            return __pd.DataFrame()
        else:
            return df[["Yön", "İhale Numarası", "Transfer Zamanı", "Katılımcılar Tarafından Talep Edilen Kapasite(MW)",
                       "Tahsis Edilen Kapasite(MW)", "Takas Fiyatı(EUR/MWh)", "Katılımcı Sayısı",
                       "Kazanan Katılımcı Sayısı", "Teklif Sayısı"]]
