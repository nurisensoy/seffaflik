# Enerji Piyasaları İşletme A.Ş. (EPİAŞ)
[EPİAŞ](https://www.epias.com.tr/)’ın başlıca amacı ve faaliyet konusu,
“ Piyasa işletim lisansında yer alan enerji piyasalarının etkin, şeffaf,
güvenilir ve enerji piyasasının ihtiyaçlarını karşılayacak şekilde planlanması,
kurulması, geliştirilmesi ve işletilmesidir. Eşit taraflar arasında ayrım
gözetmeden güvenilir referans fiyat oluşumunun temin edilmesi ve artan piyasa
katılımcı sayısı, ürün çeşitliliği ve işlem hacmiyle likiditenin en üst düzeye
ulaştığı, piyasa birleşmeleri yoluyla ticaret yapılmasına imkan tanıyan bir
enerji piyasası işletmecisi olmaktır.”

# EPİAŞ Şeffaflık Platformu
EPİAŞ, işlettiği piyasalarda katılımcıların işlemlerini fırsat eşitliliği
çerçevesinde yürütebilmesi, piyasadaki bilgi asimetrisinin önlenmesi ve
katılımcıların yapacakları işlemlerde doğru karar verebilmeleri için bir merkezi
veri ve analiz platformu olan [“Şeffaflık Platformunu”](https://seffaflik.epias.com.tr) işletmektedir.

EPİAŞ Şeffaflık Platformundaki hizmet kalitesini artırmak maksadıyla ayrıca
kullanıcılarına “Web Servis Hizmeti” de sunmaktadır. Talep eden kullanıcılar
platformda yayımlanan tüm verilere buradan da kolaylıkla ulaşabilmektedir.
Şeffaflık Platformu Web Servis Hizmetini ve dolayısıyla bu kütüphaneyi
kullanacak kişilerin öncelikle belirtmiş oldukları IP numarası üzerinden Şeffaflık Platformu Web Servis
Hizmeti ile tüm verilere ulaşabilme iznini almaları geremektedir. Bunun için
öncelikle kullanıcıların Şeffaflık Platformunda yayımlanan
[Web Servis Şartnamesini](https://www.epias.com.tr/wp-content/uploads/2016/10/Web-Servis-%C5%9Eartnamesi-1.docx)
doldurmaları ve "seffaflik@epias.com.tr" mail adresi üzerinden doldurulmuş
şartnameyi firma ile paylaşmaları gerekmektedir.

## Kurulum
#### Gereksinimler
seffaflik kütüphanesinin kullanımı için gerekli paketler:
* pandas
* requests
* python-dateutils

Not: İlgili paketler aşağıda belirtilen yükleme yöntemini izlediğiniz taktirde otomatik olarak yüklenecektir.
#### pip aracılığıyla en son sürümü yükleyin
```
$ pip install seffaflik
```

#### Kimlik (Client Id) Oluşturulması
Kütüphane yükleme işlemini tamamladıktan sonra kütüphane aracılığıyla veri çekiş işlemini gerçekleştirebilmeniz için 
aşağıda belirtilen kimlik oluşturma sürecini de tamamlamış olmanız gerekmektedir.

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


## Kullanım
Kütüphane yükleme ve kimlik oluşturma işlemleri tamamladıktan sonra kütüphane aracılığıyla şeffaflık platformundan 
kolaylıkla veri çekiş işlemine başlanabilmektedir. Kütüphanenin paketleri, alt-paketleri ve modülleri şu şekilde 
tasarlanmıştır:
```
seffaflik/
└── elektrik/
    ├── piyasalar/
    │   ├── dengesizlik.py
    │   ├── dgp.py
    │   ├── genel.py
    │   ├── gip.py    
    │   ├── gop.py
    │   ├── ia.py  
    │   └── yanhizmetler.py
    ├── santraller.py
    ├── tuketim.py
    └── uretim.py
```
Bu çerçevede modüller şu şekilde import edilebilir:
```python
from seffaflik.elektrik import santraller, tuketim, uretim
from seffaflik.elektrik.piyasalar import dengesizlik, dgp, genel, gip, gop, ia, yanhizmetler
```
#### Örnek İstekler
Yukarıda belirtilen modüller import edildikten sonra aşağıda belirtilen örnek istekler yapılabilecektir.
##### 1. Piyasa Takas Fiyatı (PTF)
```python
df = gop.ptf(baslangic_tarihi='2019-01-01', bitis_tarihi='2019-01-01')
```
##### 2. KGÜP Girebilecek Organizasyonlar
```python
df = uretim.organizasyonlar()
```
##### 3. Uzlaştırmaya Esas Veriş Miktarı (UEVM)
```python
df = uretim.uevm(baslangic_tarihi='2019-01-01', bitis_tarihi='2019-01-01')
```
##### 4. Dönemsel Serbest Tüketici Sayıları
```python
df = tuketim.serbest_tuketici_sayisi("2019-01-01","2019-10-01")
```