import datetime as __datetime
import logging as __logging
from seffaflik.ortak import parametreler as __param


def __baslangic_bitis_tarih_dogrulama(baslangic_tarihi, bitis_tarihi):
    try:
        ilk = __datetime.datetime.strptime(baslangic_tarihi, '%Y-%m-%d')
        son = __datetime.datetime.strptime(bitis_tarihi, '%Y-%m-%d')
        if ilk > son:
            __logging.warning("Bitiş tarihi başlangıç tarihinden küçük olmamalıdır!")
            return False
    except ValueError:
        __logging.warning("Girilen tarihler YYYY-AA-GG formatında olmalıdır! (Ör: 2000-10-25)")
        return False
    else:
        return True


def __baslangic_bitis_tarih_eic_dogrulama(baslangic_tarihi, bitis_tarihi, eic):
    try:
        ilk = __datetime.datetime.strptime(baslangic_tarihi, '%Y-%m-%d')
        son = __datetime.datetime.strptime(bitis_tarihi, '%Y-%m-%d')
        if ilk > son:
            __logging.warning("Bitiş tarihi başlangıç tarihinden küçük olmamalıdır!")
            return False
        if not isinstance(eic, str):
            __logging.warning("Organizasyon EIC değeri metin formatında girilmelidir!")
            return False
    except ValueError:
        __logging.warning("Girilen tarihler YYYY-AA-GG formatında olmalıdır! (Ör: 2000-10-25)")
        return False
    else:
        return True


def __tarih_dogrulama(tarih):
    try:
        __datetime.datetime.strptime(tarih, '%Y-%m-%d')
    except ValueError:
        __logging.warning("Girilen tarihler YYYY-AA-GG formatında olmalıdır! (Ör: 2000-10-25)")
        return False
    else:
        return True


def __check_http_error(error):
    if error == 401:
        __logging.error(__param.__requestsAuthenticationErrorLogging, exc_info=False)


def __kgup_girebilen_organizasyon_dogrulama(eic):
    if not isinstance(eic, str):
        __logging.warning("EIC değeri metin formatında girilmelidir!")
        return False
    else:
        from seffaflik.elektrik import uretim
        df_org = uretim.organizasyonlar()
        if eic in df_org["EIC Kodu"].values:
            return True
        else:
            __logging.warning("İlgili eic değeri KGÜP girebilecek organizasyonlar listesinde bulunmamaktadır!")
            return False
