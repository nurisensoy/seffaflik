import pandas as __pd
import datetime as __dt
from dateutil import relativedelta as __rd
from multiprocessing import Pool as __Pool
import multiprocessing as __mp
import requests as __requests

from seffaflik.__ortak.__araclar import make_requests as __make_requests
from seffaflik.__ortak import __dogrulama as __dogrulama

__first_part_url = "production/"


def santraller(tarih=__dt.datetime.now().strftime("%Y-%m-%d")):
    """
    İlgili tarihte EPİAŞ sistemine kayıtlı YEKDEM santral bilgilerini vermektedir.

    Parametre
    ----------
    tarih : %YYYY-%AA-%GG formatında tarih (Varsayılan: bugün)

    Geri Dönüş Değeri
    -----------------
    Santral Bilgileri(Id, Adı, EIC Kodu, Kısa Adı)
    """
    if __dogrulama.__tarih_dogrulama(tarih):
        try:
            particular_url = __first_part_url + "renewable-sm-licensed-power-plant-list?period=" + tarih
            json = __make_requests(particular_url)
            df = __pd.DataFrame(json["body"]["powerPlantList"])
            df.rename(index=str, columns={"id": "Id", "name": "Adı", "eic": "EIC Kodu",
                                          "shortName": "Kısa Adı"}, inplace=True)
            df = df[["Id", "Adı", "EIC Kodu", "Kısa Adı"]]
        except (KeyError, TypeError):
            return __pd.DataFrame()
        else:
            return df


def kurulu_guc(baslangic_tarihi=__dt.datetime.today().strftime("%Y-%m-%d"),
               bitis_tarihi=__dt.datetime.today().strftime("%Y-%m-%d")):
    """
    İlgili tarih aralığına tekabül eden aylar için EPİAŞ sistemine kayıtlı YEKDEM santrallerin kaynak bazlı toplam
    kurulu güç bilgisini vermektedir.

    Parametreler
    ------------
    baslangic_tarihi : %YYYY-%AA-%GG formatında başlangıç tarihi (Varsayılan: bugün)
    bitis_tarihi     : %YYYY-%AA-%GG formatında bitiş tarihi (Varsayılan: bugün)

    Geri Dönüş Değeri
    -----------------
    Kurulu Güç Bilgisi (Tarih, Kurulu Güç)
    """
    if __dogrulama.__baslangic_bitis_tarih_dogrulama(baslangic_tarihi, bitis_tarihi):
        ilk = __dt.datetime.strptime(baslangic_tarihi[:7], '%Y-%m')
        son = __dt.datetime.strptime(bitis_tarihi[:7], '%Y-%m')
        date_list = []
        while ilk <= son and ilk <= __dt.datetime.today():
            date_list.append(ilk.strftime("%Y-%m-%d"))
            ilk = ilk + __rd.relativedelta(months=+1)
        with __Pool(__mp.cpu_count()) as p:
            df_list = p.map(__yekdem_kurulu_guc, date_list)
        return __pd.concat(df_list, sort=False)


def lisansli_uevm(baslangic_tarihi=__dt.datetime.today().strftime("%Y-%m-%d"),
                  bitis_tarihi=__dt.datetime.today().strftime("%Y-%m-%d")):
    """
    İlgili tarih aralığı için saatlik YEKDEM kapsamındaki lisanslı santrallerin kaynak bazında uzlaştırmaya esas veriş
    miktarı (UEVM) bilgisini  vermektedir.

    Parametreler
    ------------
    baslangic_tarihi : %YYYY-%AA-%GG formatında başlangıç tarihi (Varsayılan: bugün)
    bitis_tarihi     : %YYYY-%AA-%GG formatında bitiş tarihi (Varsayılan: bugün)

    Geri Dönüş Değeri
    -----------------
    Saatlik YEKDEM Lisanslı UEVM (MWh)
    """
    if __dogrulama.__baslangic_bitis_tarih_dogrulama(baslangic_tarihi, bitis_tarihi):
        try:
            particular_url = \
                __first_part_url + "renewable-sm-licensed-injection-quantity" + "?startDate=" + baslangic_tarihi + \
                "&endDate=" + bitis_tarihi
            json = __make_requests(particular_url)
            df = __pd.DataFrame(json["body"]["renewableSMProductionList"])
            df["Saat"] = df["date"].apply(lambda h: int(h[11:13]))
            df["Tarih"] = __pd.to_datetime(df["date"].apply(lambda d: d[:10]))
            df.rename(index=str,
                      columns={"canalType": "Kanal Tipi", "riverType": "Nehir Tipi", "biogas": "Biyogaz",
                               "biomass": "Biyokütle", "landfillGas": "Çöp Gazı", "sun": "Güneş",
                               "geothermal": "Jeotermal", "reservoir": "Rezervuarlı", "wind": "Rüzgar",
                               "total": "Toplam", "others": "Diğer"},
                      inplace=True)
            df = df[
                ["Tarih", "Saat", "Rüzgar", "Jeotermal", "Rezervuarlı", "Kanal Tipi", "Nehir Tipi", "Çöp Gazı",
                 "Biyogaz", "Güneş", "Biyokütle", "Diğer", "Toplam"]]
        except (KeyError, TypeError):
            return __pd.DataFrame()
        else:
            return df


def lisanssiz_uevm(baslangic_tarihi=__dt.datetime.today().strftime("%Y-%m-%d"),
                   bitis_tarihi=__dt.datetime.today().strftime("%Y-%m-%d")):
    """
    İlgili tarih aralığı için saatlik YEKDEM kapsamındaki lisanssiz santrallerin kaynak bazında uzlaştırmaya esas veriş
    miktarı (UEVM) bilgisini  vermektedir.

    Parametreler
    ------------
    baslangic_tarihi : %YYYY-%AA-%GG formatında başlangıç tarihi (Varsayılan: bugün)
    bitis_tarihi     : %YYYY-%AA-%GG formatında bitiş tarihi (Varsayılan: bugün)

    Geri Dönüş Değeri
    -----------------
    Saatlik YEKDEM Lisanssiz UEVM (MWh)
    """
    if __dogrulama.__baslangic_bitis_tarih_dogrulama(baslangic_tarihi, bitis_tarihi):
        try:
            particular_url = \
                __first_part_url + "renewable-unlicenced-generation-amount" + "?startDate=" + baslangic_tarihi + \
                "&endDate=" + bitis_tarihi
            json = __make_requests(particular_url)
            df = __pd.DataFrame(json["body"]["renewableUnlicencedGenerationAmountList"])
            df["Saat"] = df["date"].apply(lambda h: int(h[11:13]))
            df["Tarih"] = __pd.to_datetime(df["date"].apply(lambda d: d[:10]))
            df.rename(index=str,
                      columns={"canalType": "Kanal Tipi", "riverType": "Nehir Tipi", "biogas": "Biyogaz",
                               "biomass": "Biyokütle", "lfg": "Çöp Gazı", "sun": "Güneş",
                               "geothermal": "Jeotermal", "reservoir": "Rezervuarlı", "wind": "Rüzgar",
                               "total": "Toplam", "others": "Diğer"},
                      inplace=True)
            df = df[
                ["Tarih", "Saat", "Rüzgar", "Kanal Tipi", "Biyogaz", "Güneş", "Biyokütle", "Diğer", "Toplam"]]
        except (KeyError, TypeError):
            return __pd.DataFrame()
        else:
            return df


def uevm(baslangic_tarihi=__dt.datetime.today().strftime("%Y-%m-%d"),
         bitis_tarihi=__dt.datetime.today().strftime("%Y-%m-%d")):
    """
    İlgili tarih aralığı için YEKDEM kapsamındaki santrallerin kaynak bazında uzlaştırmaya esas veriş miktarı (UEVM)
    bilgisini vermektedir.

    Parametreler
    ------------
    baslangic_tarihi : %YYYY-%AA-%GG formatında başlangıç tarihi (Varsayılan: bugün)
    bitis_tarihi     : %YYYY-%AA-%GG formatında bitiş tarihi (Varsayılan: bugün)

    Geri Dönüş Değeri
    -----------------
    Saatlik YEKDEM UEVM (MWh)
    """
    if __dogrulama.__baslangic_bitis_tarih_dogrulama(baslangic_tarihi, bitis_tarihi):
        try:
            particular_url = \
                __first_part_url + "renewable-sm-production" + "?startDate=" + baslangic_tarihi + \
                "&endDate=" + bitis_tarihi
            json = __make_requests(particular_url)
            df = __pd.DataFrame(json["body"]["renewableSMProductionList"])
            df["Saat"] = df["date"].apply(lambda h: int(h[11:13]))
            df["Tarih"] = __pd.to_datetime(df["date"].apply(lambda d: d[:10]))
            df.rename(index=str,
                      columns={"canalType": "Kanal Tipi", "riverType": "Nehir Tipi", "biogas": "Biyogaz",
                               "biomass": "Biyokütle", "landfillGas": "Çöp Gazı",
                               "geothermal": "Jeotermal", "dammedHydroWithReservoir": "Rezervuarlı", "wind": "Rüzgar",
                               "total": "Toplam", "others": "Diğer"},
                      inplace=True)
            df = df[["Tarih", "Saat", "Rüzgar", "Jeotermal", "Rezervuarlı", "Kanal Tipi", "Nehir Tipi", "Çöp Gazı",
                     "Biyogaz", "Biyokütle", "Diğer", "Toplam"]]
        except (KeyError, TypeError):
            return __pd.DataFrame()
        else:
            return df


def lisansli_gerceklesen(baslangic_tarihi=__dt.datetime.today().strftime("%Y-%m-%d"),
                         bitis_tarihi=__dt.datetime.today().strftime("%Y-%m-%d"), santral_id=""):
    """
    İlgili tarih aralığı için YEKDEM kapsamındaki lisanslı santrallerin toplam gerçek zamanlı üretim bilgisini
    vermektedir.
    Not: "santral_id" değeri girildiği taktirde santrale ait gerçek zamanlı üretim bilgisini vermektedir.
    Girilmediği taktirde toplam gerçek zamanlı üretim bilgisini vermektedir.

    Parametreler
    ------------
    baslangic_tarihi : %YYYY-%AA-%GG formatında başlangıç tarihi (Varsayılan: bugün)
    bitis_tarihi     : %YYYY-%AA-%GG formatında bitiş tarihi (Varsayılan: bugün)
    santral_id       : metin yada tam sayı formatında santral id (Varsayılan: "")

    Geri Dönüş Değeri
    -----------------
    Gerçek Zamanlı Üretim("Tarih", "Saat", "Rüzgar", "Jeotermal", "Rezervuarlı", "Kanal Tipi", "Nehir Tipi", "Çöp Gazı",
                 "Biyogaz", "Güneş", "Biyokütle", "Diğer", "Toplam")
    """
    if __dogrulama.__baslangic_bitis_tarih_id_dogrulama(baslangic_tarihi, bitis_tarihi, santral_id):
        if santral_id == "":
            return __gerceklesen(baslangic_tarihi, bitis_tarihi)
        else:
            return __santral_bazli_gerceklesen(baslangic_tarihi, bitis_tarihi, santral_id)


def birim_maliyet(baslangic_tarihi=__dt.datetime.today().strftime("%Y-%m-%d"),
                  bitis_tarihi=__dt.datetime.today().strftime("%Y-%m-%d")):
    """
    İlgili tarih aralığı için YEKDEM birim maliyet bilgisini vermektedir.

    Parametreler
    ------------
    baslangic_tarihi : %YYYY-%AA-%GG formatında başlangıç tarihi (Varsayılan: bugün)
    bitis_tarihi     : %YYYY-%AA-%GG formatında bitiş tarihi (Varsayılan: bugün)

    Geri Dönüş Değeri
    -----------------
    Dönemlik YEKDEM Birim Maliyet (₺/MWh)
    """
    if __dogrulama.__baslangic_bitis_tarih_dogrulama(baslangic_tarihi, bitis_tarihi):
        try:
            particular_url = \
                __first_part_url + "renewable-sm-unit-cost" + "?startDate=" + baslangic_tarihi + "&endDate=" + \
                bitis_tarihi
            json = __make_requests(particular_url)
            df = __pd.DataFrame(json["body"]["renewableSMUnitCostList"])
            df["Dönem"] = df["id"].apply(
                lambda x: str(__pd.to_datetime(x["donem"][:10]).month_name(locale='tr_TR.UTF-8')) + "-" + str(
                    __pd.to_datetime(x["donem"][:10]).year))
            df["Versiyon"] = df["id"].apply(
                lambda x: str(__pd.to_datetime(x["versiyon"][:10]).month_name(locale='tr_TR.UTF-8')) + "-" + str(
                    __pd.to_datetime(x["versiyon"][:10]).year))
            df.rename(index=str, columns={"unitCost": "Birim Maliyet (TL)"}, inplace=True)
            df = df[["Dönem", "Versiyon", "Birim Maliyet (TL)"]]
        except (KeyError, TypeError):
            return __pd.DataFrame()
        else:
            return df


def donemsel_maliyet(baslangic_tarihi=__dt.datetime.today().strftime("%Y-%m-%d"),
                     bitis_tarihi=__dt.datetime.today().strftime("%Y-%m-%d")):
    """
    İlgili tarih aralığı için dönemlik YEKDEM maliyetleri bilgisini  vermektedir.

    Parametreler
    ------------
    baslangic_tarihi : %YYYY-%AA-%GG formatında başlangıç tarihi (Varsayılan: bugün)
    bitis_tarihi     : %YYYY-%AA-%GG formatında bitiş tarihi (Varsayılan: bugün)

    Geri Dönüş Değeri
    -----------------
    Dönemsel YEKDEM Maliyeti (MWh)
    """
    if __dogrulama.__baslangic_bitis_tarih_dogrulama(baslangic_tarihi, bitis_tarihi):
        try:
            particular_url = \
                __first_part_url + "renewables-support" + "?startDate=" + baslangic_tarihi + \
                "&endDate=" + bitis_tarihi
            json = __make_requests(particular_url)
            df = __pd.DataFrame(json["body"]["renewablesSupports"])
            df["Dönem"] = df["period"].apply(
                lambda x: str(__pd.to_datetime(x[:10]).month_name(locale='tr_TR.UTF-8')) + "-" + str(
                    __pd.to_datetime(x[:10]).year))
            df.rename(index=str,
                      columns={"unitCost": "Birim Maliyet (TL)", "licenseExemptCost": "Lisanssız Toplam Maliyet (TL)",
                               "renewablesTotalCost": "Toplam Maliyet (TL)",
                               "reneablesCost": "Lisanlı Toplam Maliyet (TL)",
                               "portfolioIncome": "Toplam Gelir (TL)"},
                      inplace=True)
            df = df[
                ["Dönem", "Birim Maliyet (TL)", "Lisanssız Toplam Maliyet (TL)", "Lisanlı Toplam Maliyet (TL)",
                 "Toplam Maliyet (TL)", "Toplam Gelir (TL)"]]
        except (KeyError, TypeError):
            return __pd.DataFrame()
        else:
            return df


def res_uretim_tahmini():
    """
    Türkiye geneli izlenebilen RES'lerin ertesi gün için toplam güç üretim tahmini bilgisini vermektedir.
    Not: İlgili veri ritm.gov.tr üzerinden temin edilmektedir.

    Parametreler
    ------------

    Geri Dönüş Değeri
    -----------------
    RES Üretim Tahmini (MWh)
    """
    r = __requests.get("http://www.ritm.gov.tr/amline/data_file_ritm.txt")
    df = __pd.DataFrame(r.text.split("\n")[1:][:-1])
    df = __pd.DataFrame(df[0].str.split(",").tolist(), columns=["Tarih", "Q5", "Q25", "Q75", "Q95", "Tahmin", "Üretim"])
    df["Saat"] = df["Tarih"].apply(lambda x: x.split(" ")[1])
    df["Tarih"] = df["Tarih"].apply(lambda x: __pd.to_datetime(x.split(" ")[0], format="%d.%m.%Y"))
    df = df[["Tarih", "Saat", "Q5", "Q25", "Q75", "Q95", "Tahmin"]]
    tomorrow = (__dt.datetime.today() + __rd.relativedelta(days=+1)).strftime("%Y-%m-%d")
    return df[df["Tarih"] == tomorrow]


def __gerceklesen(baslangic_tarihi=__dt.datetime.today().strftime("%Y-%m-%d"),
                  bitis_tarihi=__dt.datetime.today().strftime("%Y-%m-%d")):
    """
    İlgili tarih aralığı için saatlik YEKDEM kapsamındaki lisanslı santrallerin kaynak bazında gerçek zamanlı üretim
    bilgisini  vermektedir.

    Parametreler
    ------------
    baslangic_tarihi : %YYYY-%AA-%GG formatında başlangıç tarihi (Varsayılan: bugün)
    bitis_tarihi     : %YYYY-%AA-%GG formatında bitiş tarihi (Varsayılan: bugün)

    Geri Dönüş Değeri
    -----------------
    Saatlik YEKDEM Lisanslı UEVM (MWh)
    """
    if __dogrulama.__baslangic_bitis_tarih_dogrulama(baslangic_tarihi, bitis_tarihi):
        try:
            particular_url = \
                __first_part_url + "renewable-sm-licensed-real-time-generation" + "?startDate=" + baslangic_tarihi + \
                "&endDate=" + bitis_tarihi
            json = __make_requests(particular_url)
            df = __pd.DataFrame(json["body"]["renewableLicencedGenerationAmount"])
            df["Saat"] = df["date"].apply(lambda h: int(h[11:13]))
            df["Tarih"] = __pd.to_datetime(df["date"].apply(lambda d: d[:10]))
            df.rename(index=str,
                      columns={"canalType": "Kanal Tipi", "riverType": "Nehir Tipi", "biogas": "Biyogaz",
                               "biomass": "Biyokütle", "lfg": "Çöp Gazı", "sun": "Güneş",
                               "geothermal": "Jeotermal", "reservoir": "Rezervuarlı", "wind": "Rüzgar",
                               "total": "Toplam", "others": "Diğer"},
                      inplace=True)
            df = df[["Tarih", "Saat", "Rüzgar", "Jeotermal", "Rezervuarlı", "Kanal Tipi", "Nehir Tipi", "Çöp Gazı",
                     "Biyogaz", "Güneş", "Biyokütle", "Diğer", "Toplam"]]
        except (KeyError, TypeError):
            return __pd.DataFrame()
        else:
            return df


def __santral_bazli_gerceklesen(baslangic_tarihi, bitis_tarihi, santral_id):
    """
    İlgili tarih aralığı ve YEKDEM kapsamındaki lisanslı santral için gerçek zamanlı üretim bilgisini vermektedir.

    Parametreler
    ------------
    baslangic_tarihi : %YYYY-%AA-%GG formatında başlangıç tarihi
    bitis_tarihi     : %YYYY-%AA-%GG formatında bitiş tarihi
    santral_id       : metin yada tam sayı formatında santral id

    Geri Dönüş Değeri
    -----------------
    Santral Bazlı Gerçek Zamanlı Üretim("Tarih", "Saat", "Doğalgaz", "Barajlı", "Linyit", "Akarsu", "İthal Kömür",
                "Rüzgar", "Güneş", "Fuel Oil", "Jeo Termal", "Asfaltit Kömür", "Taş Kömür", "Biokütle", "Nafta", "LNG",
                "Uluslararası", "Toplam")
    """
    try:
        particular_url = __first_part_url + "renewable-sm-licensed-real-time-generation_with_powerplant" + \
                         "?startDate=" + baslangic_tarihi + "&endDate=" + bitis_tarihi + "&powerPlantId=" + \
                         str(santral_id)
        json = __make_requests(particular_url)
        df = __pd.DataFrame(json["body"]["renewableLicencedGenerationAmount"])
        df["Saat"] = df["date"].apply(lambda h: int(h[11:13]))
        df["Tarih"] = __pd.to_datetime(df["date"].apply(lambda d: d[:10]))
        df.rename(index=str,
                  columns={"canalType": "Kanal Tipi", "riverType": "Nehir Tipi", "biogas": "Biyogaz",
                           "biomass": "Biyokütle", "lfg": "Çöp Gazı", "sun": "Güneş",
                           "geothermal": "Jeotermal", "reservoir": "Rezervuarlı", "wind": "Rüzgar",
                           "total": "Toplam", "others": "Diğer"},
                  inplace=True)
        df = df[
            ["Tarih", "Saat", "Rüzgar", "Jeotermal", "Rezervuarlı", "Kanal Tipi", "Nehir Tipi", "Çöp Gazı", "Biyogaz",
             "Güneş", "Biyokütle", "Diğer", "Toplam"]]
    except (KeyError, TypeError):
        return __pd.DataFrame()
    else:
        return df


def __yekdem_kurulu_guc(tarih):
    """
    İlgili tarih için EPİAŞ sistemine kayıtlı yekdem kapsamındaki santrallerin kaynak bazlı toplam kurulu güç bilgisini
    vermektedir.

    Parametre
    ----------
    tarih : %YYYY-%AA-%GG formatında tarih (Varsayılan: bugün)

    Geri Dönüş Değeri
    -----------------
    Kurulu Güç Bilgisi (Tarih, Kurulu Güç)
    """
    try:
        particular_url = __first_part_url + "installed-capacity-of-renewable?period=" + tarih
        json = __make_requests(particular_url)
        df = __pd.DataFrame(json["body"]["installedCapacityOfRenewableList"])
        columns = df["capacityType"].values
        df = df[["capacity"]].transpose()
        df.set_axis(columns, axis=1, inplace=True)
        df.reset_index(drop=True, inplace=True)
        df.insert(loc=0, column="Tarih", value=__pd.to_datetime(tarih))
    except (KeyError, TypeError):
        return __pd.DataFrame()
    else:
        return df
