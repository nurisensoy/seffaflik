import datetime


def baslangic_bitis_tarih_dogrulama(baslangic_tarihi, bitis_tarihi):
    try:
        ilk = datetime.datetime.strptime(baslangic_tarihi, '%Y-%m-%d')
        son = datetime.datetime.strptime(bitis_tarihi, '%Y-%m-%d')
        if ilk > son:
            print("Bitiş tarihi başlangıç tarihinden küçük olmamalıdır!")
            return False
    except ValueError:
        print("Girilen tarihler YYYY-AA-GG formatında olmalıdır! (Ör: 2000-10-25)")
        return False
    else:
        return True


def tarih_dogrulama(tarih):
    try:
        datetime.datetime.strptime(tarih, '%Y-%m-%d')
    except ValueError:
        print("Girilen tarihler YYYY-AA-GG formatında olmalıdır! (Ör: 2000-10-25)")
        return False
    else:
        return True


def kgup_girebilen_organizasyon_dogrulama(eic):
    if type(eic) != str:
        print("EIC değeri metin formatında girilmelidir!")
        return False
    else:
        from seffaflik.uretim import uretim
        df_org = uretim.organizasyonlar()
        if eic in df_org["EIC Kodu"].values:
            return True
        else:
            print("İlgili eic değeri KGÜP girebilecek organizasyonlar listesinde bulunmamaktadır!")
            return False
