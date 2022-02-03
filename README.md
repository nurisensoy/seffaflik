## Seffaflik: 
![PyPI - Python Version](https://img.shields.io/pypi/pyversions/seffaflik)
![PyPI](https://img.shields.io/pypi/v/seffaflik)
![PyPI - License](https://img.shields.io/pypi/l/seffaflik)
![GitHub last commit](https://img.shields.io/github/last-commit/nurisensoy/seffaflik)
[![Downloads](https://pepy.tech/badge/seffaflik)](https://pepy.tech/project/seffaflik)
[![Downloads](https://pepy.tech/badge/seffaflik/month)](https://pepy.tech/project/seffaflik)
# Kurulum
### Gereksinimler
seffaflik kütüphanesinin kullanımı için gerekli paketler:
* pandas
* requests
* python-dateutils

Not: İlgili paketler aşağıda belirtilen yükleme yöntemini izlediğiniz taktirde otomatik olarak yüklenecektir.
### pip aracılığıyla en son sürümü yükleyin
```
$ pip install seffaflik
```

### Kimlik (Client Id) Oluşturulması
Kütüphane yükleme işlemini tamamladıktan sonra kütüphane aracılığıyla veri çekiş işlemini gerçekleştirebilmeniz için 
temin etmiş olduğunuz **istemci taniticisi (client id)** ile aşağıda belirtilen kimlik oluşturma sürecini de tamamlamış 
olmanız gerekmektedir. [İstemci tanitici (client id) nasıl temin edilir?](https://nurisensoy.github.io/nurisensoy/seffaflik_platformu/nedir)

```python
import seffaflik
seffaflik.Kimlik(istemci_taniticisi="abcdefg")
```
Yukarıdaki işlemi gerçekleştirmenizden sonra ana dizininize özel bir
.seffaflik/.kimlik dosyası yerleştirilir. ~/.seffaflik/.kimlik dosyanız aşağıdaki
gibi görünmelidir:
```
{
  "istemci_taniticisi": "abcdefg"
}
```
Not: İstemci Tanıtıcı geçerliliğini koruduğu sürece kimlik oluşturma işleminin tekrar gerçekleştirilmesine 
gerek duyulmamaktadır.


# Kullanım
Kütüphane yükleme ve kimlik oluşturma işlemleri tamamladıktan sonra kütüphane aracılığıyla şeffaflık platformundan 
kolaylıkla veri çekiş işlemine başlanabilmektedir. Kütüphanenin paketleri, alt-paketleri ve modülleri şu şekilde 
tasarlanmıştır:
```
seffaflik/
├── dogalgaz/
│   └── stp.py
└── elektrik/
    ├── piyasalar/
    │   ├── dengesizlik.py
    │   ├── dgp.py
    │   ├── genel.py
    │   ├── gip.py    
    │   ├── gop.py
    │   ├── ia.py  
    │   └── yanhizmetler.py
    ├── iletim.py
    ├── santraller.py
    ├── tuketim.py
    ├── yekdem.py
    └── uretim.py
    
```
Bu çerçevede tüm modüller şu şekilde import edilebilir:
```python
from seffaflik.elektrik import santraller, tuketim, uretim, yekdem
from seffaflik.elektrik.piyasalar import dengesizlik, dgp, genel, gip, gop, ia, yanhizmetler
from seffaflik.dogalgaz import stp
```
### Örnek İstekler
Yukarıda belirtilen modüller import edildikten sonra aşağıda belirtilen örnek istekler yapılabilecektir.
#### 1. Piyasa Takas Fiyatı (PTF)
```python
df = gop.ptf(baslangic_tarihi='2019-01-01', bitis_tarihi='2019-01-01')
```
#### 2. KGÜP Girebilecek Organizasyonlar
```python
df = uretim.organizasyonlar()
```
#### 3. Uzlaştırmaya Esas Veriş Miktarı (UEVM)
```python
df = uretim.uevm(baslangic_tarihi='2019-01-01', bitis_tarihi='2019-01-01')
```
#### 4. Dönemsel Serbest Tüketici Sayıları
```python
df = tuketim.serbest_tuketici_sayisi("2019-01-01","2019-10-01")
```
#### 5. Doğalgaz Sürekli Ticaret Platformu Günlük Fiyat
```python
df = stp.gunluk_fiyat("2019-01-01","2019-10-01")
```
