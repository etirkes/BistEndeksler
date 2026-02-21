import yfinance as yf
import pandas as pd
from supabase import create_client, Client
import os
import time
from datetime import datetime, timedelta

# --- AYARLAR ---
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    # .env dosyasından okumayı dene (lokal geliştirme için)
    try:
        from dotenv import load_dotenv
        load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), 'bist-takip', '.env'))
        SUPABASE_URL = os.environ.get("VITE_SUPABASE_URL")
        SUPABASE_KEY = os.environ.get("VITE_SUPABASE_ANON_KEY")
    except:
        pass

if not SUPABASE_URL or not SUPABASE_KEY:
    print("Hata: SUPABASE_URL veya SUPABASE_KEY eksik.")

try:
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
except Exception as e:
    print(f"Bağlantı hatası: {e}")
    supabase = None

# Takip Edilecek Endeksler
INDICES = {
    'XU030.IS': {'name': 'BIST 30', 'category': 'Genel'},
    'XU050.IS': {'name': 'BIST 50', 'category': 'Genel'},
    'XU100.IS': {'name': 'BIST 100', 'category': 'Genel'},
    'XU500.IS': {'name': 'BIST 500', 'category': 'Genel'},
    'XUTUM.IS': {'name': 'BIST TÜM', 'category': 'Genel'},
    'X10XB.IS': {'name': 'BIST Banka Dışı Likit 10', 'category': 'Genel'},
    'XAKUR.IS': {'name': 'BIST Aracı Kurum', 'category': 'Endeks'},
    'XBANK.IS': {'name': 'Bankacılık', 'category': 'Sektör'},
    'XBLSM.IS': {'name': 'Bilişim', 'category': 'Sektör'},
    'XELKT.IS': {'name': 'Elektrik', 'category': 'Sektör'},
    'XFINK.IS': {'name': 'Finansal Kir. Faktoring', 'category': 'Sektör'},
    'XGIDA.IS': {'name': 'Gıda İçecek', 'category': 'Sektör'},
    'XGMYO.IS': {'name': 'Gayrimenkul Y.O.', 'category': 'Sektör'},
    'XHARZ.IS': {'name': 'BIST Halka Arz', 'category': 'Endeks'},
    'XHOLD.IS': {'name': 'Holding ve Yatırım', 'category': 'Sektör'},
    'XILTM.IS': {'name': 'İletişim', 'category': 'Sektör'},
    'XINSA.IS': {'name': 'İnşaat', 'category': 'Sektör'},
    'XK030.IS': {'name': 'BIST Katılım 30', 'category': 'Endeks'},
    'XKMYA.IS': {'name': 'Kimya Petrol Plastik', 'category': 'Sektör'},
    'XKTMT.IS': {'name': 'BIST Katılım Tüm', 'category': 'Endeks'},
    'XKTUM.IS': {'name': 'BIST Katılım Tüm (Eski)', 'category': 'Endeks'},
    'XLBNK.IS': {'name': 'BIST Likit Banka', 'category': 'Endeks'},
    'XMADN.IS': {'name': 'Madencilik', 'category': 'Sektör'},
    'XMANA.IS': {'name': 'Metal Ana', 'category': 'Sektör'},
    'XMESY.IS': {'name': 'Metal Eşya Makina', 'category': 'Sektör'},
    'XSD25.IS': {'name': 'BIST Sürdürülebilirlik 25', 'category': 'Endeks'},
    'XSGRT.IS': {'name': 'Sigorta', 'category': 'Sektör'},
    'XSPOR.IS': {'name': 'Spor', 'category': 'Sektör'},
    'XTAST.IS': {'name': 'Taş Toprak', 'category': 'Sektör'},
    'XTCRT.IS': {'name': 'Ticaret', 'category': 'Sektör'},
    'XTEKS.IS': {'name': 'Tekstil Deri', 'category': 'Sektör'},
    'XTM25.IS': {'name': 'BIST Temettü 25', 'category': 'Endeks'},
    'XTMTU.IS': {'name': 'BIST Temettü', 'category': 'Endeks'},
    'XTRZM.IS': {'name': 'Turizm', 'category': 'Sektör'},
    'XTUMY.IS': {'name': 'BIST Tüm-100', 'category': 'Genel'},
    'XUGRA.IS': {'name': 'BIST Ulaştırma (Alternatif)', 'category': 'Sektör'},
    'XUHIZ.IS': {'name': 'Hizmetler', 'category': 'Sektör'},
    'XULAS.IS': {'name': 'Ulaştırma', 'category': 'Sektör'},
    'XUMAL.IS': {'name': 'Mali', 'category': 'Sektör'},
    'XUSIN.IS': {'name': 'Sınai', 'category': 'Sektör'},
    'XUTEK.IS': {'name': 'Teknoloji', 'category': 'Sektör'},
    'XYLDZ.IS': {'name': 'BIST Yıldız', 'category': 'Pazar'},
    'XYORT.IS': {'name': 'Yatırım Ortaklığı', 'category': 'Sektör'},
    'XYUZO.IS': {'name': 'BIST 100-30', 'category': 'Genel'},
}

# Endekslerin içindeki hisseler (Güncel BIST Verileri ile Genişletildi)
CONSTITUENTS = {
    # --- ANA ENDEKSLER ---
    'XU030.IS': [
        'AKBNK.IS', 'ALARK.IS', 'ARCLK.IS', 'ASELS.IS', 'ASTOR.IS', 'BIMAS.IS', 'BRSAN.IS', 'EKGYO.IS', 'ENKAI.IS', 'EREGL.IS',
        'FROTO.IS', 'GARAN.IS', 'GUBRF.IS', 'HEKTS.IS', 'ISCTR.IS', 'KCHOL.IS', 'KONTR.IS', 'TRALT.IS', 'KRDMD.IS', 'ODAS.IS',
        'OYAKC.IS', 'PGSUS.IS', 'SAHOL.IS', 'SASA.IS', 'SISE.IS', 'TCELL.IS', 'THYAO.IS', 'TOASO.IS', 'TUPRS.IS', 'YKBNK.IS'
    ],
    'XU050.IS': [
        'AKBNK.IS', 'ALARK.IS', 'ARCLK.IS', 'ASELS.IS', 'ASTOR.IS', 'BIMAS.IS', 'BRSAN.IS', 'EKGYO.IS', 'ENKAI.IS', 'EREGL.IS',
        'FROTO.IS', 'GARAN.IS', 'GUBRF.IS', 'HEKTS.IS', 'ISCTR.IS', 'KCHOL.IS', 'KONTR.IS', 'TRALT.IS', 'KRDMD.IS', 'ODAS.IS',
        'OYAKC.IS', 'PGSUS.IS', 'SAHOL.IS', 'SASA.IS', 'SISE.IS', 'TCELL.IS', 'THYAO.IS', 'TOASO.IS', 'TUPRS.IS', 'YKBNK.IS',
        'AEFES.IS', 'AGHOL.IS', 'AKSA.IS', 'AKSEN.IS', 'ALFAS.IS', 'BERA.IS', 'CANTE.IS', 'CCOLA.IS', 'CIMSA.IS', 'DOHOL.IS',
        'EGEEN.IS', 'ENJSA.IS', 'EUPWR.IS', 'GESAN.IS', 'HALKB.IS', 'ISGYO.IS', 'TRMET.IS', 'MGROS.IS', 'SMRTG.IS', 'SOKM.IS',
        'TTKOM.IS', 'ULKER.IS', 'VAKBN.IS', 'VESTL.IS'
    ],
    'XU100.IS': [
        'AGHOL.IS', 'AKBNK.IS', 'AKSA.IS', 'AKSEN.IS', 'ALARK.IS', 'ALBRK.IS', 'ALFAS.IS', 'ARCLK.IS', 'ASELS.IS', 'ASTOR.IS',
        'BERA.IS', 'BIMAS.IS', 'BIOEN.IS', 'BRSAN.IS', 'BRYAT.IS', 'BUCIM.IS', 'CANTE.IS', 'CCOLA.IS', 'CEMTS.IS', 'CIMSA.IS',
        'CWENE.IS', 'DOAS.IS', 'DOHOL.IS', 'ECILC.IS', 'ECZYT.IS', 'EGEEN.IS', 'EKGYO.IS', 'ENJSA.IS', 'ENKAI.IS', 'EREGL.IS',
        'EUPWR.IS', 'EUREN.IS', 'FROTO.IS', 'GARAN.IS', 'GENIL.IS', 'GESAN.IS', 'GLYHO.IS', 'GSDHO.IS', 'GUBRF.IS', 'HALKB.IS',
        'HEKTS.IS', 'IMASM.IS', 'TRENH.IS', 'ISCTR.IS', 'ISDMR.IS', 'ISGYO.IS', 'ISMEN.IS', 'IZMDC.IS', 'KARSN.IS', 'KAYSE.IS',
        'KCAER.IS', 'KCHOL.IS', 'KONTR.IS', 'KONYA.IS', 'TRMET.IS', 'TRALT.IS', 'KRDMD.IS', 'KZBGY.IS', 'MAVI.IS', 'MGROS.IS',
        'MIATK.IS', 'ODAS.IS', 'OTKAR.IS', 'OYAKC.IS', 'PENTA.IS', 'PETKM.IS', 'PGSUS.IS', 'PSGYO.IS', 'QUAGR.IS', 'SAHOL.IS',
        'SASA.IS', 'SELEC.IS', 'SISE.IS', 'SKBNK.IS', 'SMRTG.IS', 'SNGYO.IS', 'SOKM.IS', 'TAVHL.IS', 'TCELL.IS', 'THYAO.IS',
        'TKFEN.IS', 'TOASO.IS', 'TSKB.IS', 'TTKOM.IS', 'TTRAK.IS', 'TUKAS.IS', 'TUPRS.IS', 'TURSG.IS', 'ULKER.IS', 'VAKBN.IS',
        'VESBE.IS', 'VESTL.IS', 'YEOTK.IS', 'YKBNK.IS', 'YYLGD.IS', 'ZOREN.IS'
    ],
    
    # --- SEKTÖR ENDEKSLERİ ---
    'XBANK.IS': ['AKBNK.IS', 'ALBRK.IS', 'GARAN.IS', 'HALKB.IS', 'ICBCT.IS', 'ISATR.IS', 'ISBTR.IS', 'ISCTR.IS', 'SKBNK.IS', 'TSKB.IS', 'VAKBN.IS', 'YKBNK.IS'],    
    'XBLSM.IS': ['ALCTL.IS', 'ARDYZ.IS', 'ARENA.IS', 'ATATP.IS', 'AZTEK.IS', 'BINBN.IS', 'DESPC.IS', 'DGATE.IS', 'DOFRB.IS', 'EDATA.IS', 'ESCOM.IS', 'FONET.IS', 'FORTE.IS', 'HTTBT.IS', 'INDES.IS', 'INGRM.IS', 'KAREL.IS', 'KFEIN.IS', 'KRONT.IS', 'LINK.IS', 'LOGO.IS', 'MANAS.IS', 'MIATK.IS', 'MOBTL.IS', 'MTRKS.IS', 'NETAS.IS', 'NETCD.IS', 'OBASE.IS', 'ODINE.IS', 'PAPIL.IS', 'PATEK.IS', 'PENTA.IS', 'PKART.IS', 'REEDR.IS', 'SMART.IS', 'VBTYZ.IS'],    
    'XELKT.IS': ['A1YEN.IS', 'AHGAZ.IS', 'AKENR.IS', 'AKFYE.IS', 'AKSEN.IS', 'AKSUE.IS', 'ALFAS.IS', 'ARASE.IS', 'ARFYE.IS', 'AYDEM.IS', 'AYEN.IS', 'BESTE.IS', 'BIGEN.IS', 'BIOEN.IS', 'CANTE.IS', 'CATES.IS', 'CONSE.IS', 'CWENE.IS', 'ECOGR.IS', 'ENDAE.IS', 'ENERY.IS', 'ENJSA.IS', 'ENTRA.IS', 'ESEN.IS', 'GWIND.IS', 'HUNER.IS', 'IZENR.IS', 'KLYPV.IS', 'LYDYE.IS', 'MAGEN.IS', 'MOGAN.IS', 'NATEN.IS', 'NTGAZ.IS', 'ODAS.IS', 'PAMEL.IS', 'SMRTG.IS', 'TATEN.IS', 'ZEDUR.IS', 'ZOREN.IS'],    
    'XFINK.IS': ['CRDFA.IS', 'DSTKF.IS', 'GARFA.IS', 'ISFIN.IS', 'LIDFA.IS', 'SEKFK.IS', 'ULUFA.IS', 'VAKFA.IS', 'VAKFN.IS'],    
    'XGIDA.IS': ['AEFES.IS', 'AKHAN.IS', 'ALKLC.IS', 'ARMGD.IS', 'ATAKP.IS', 'AVOD.IS', 'BALSU.IS', 'BANVT.IS', 'BESLR.IS', 'BORSK.IS', 'CCOLA.IS', 'CEMZY.IS', 'DARDL.IS', 'DMRGD.IS', 'DURKN.IS', 'EFOR.IS', 'EKSUN.IS', 'ELITE.IS', 'ERSU.IS', 'FADE.IS', 'FRIGO.IS', 'GOKNR.IS', 'GUNDG.IS', 'KAYSE.IS', 'KRSTL.IS', 'KRVGD.IS', 'KTSKR.IS', 'MERKO.IS', 'MEYSU.IS', 'OBAMS.IS', 'OFSYM.IS', 'ORCAY.IS', 'OYLUM.IS', 'PENGD.IS', 'PETUN.IS', 'PINSU.IS', 'PNSUT.IS', 'SEGMN.IS', 'SELVA.IS', 'SOKE.IS', 'TATGD.IS', 'TBORG.IS', 'TUKAS.IS', 'ULKER.IS', 'ULUUN.IS', 'VANGD.IS', 'YYLGD.IS'],    
    'XGMYO.IS': ['ADGYO.IS', 'AGYO.IS', 'AHSGY.IS', 'AKFGY.IS', 'AKMGY.IS', 'AKSGY.IS', 'ALGYO.IS', 'ASGYO.IS', 'ATAGY.IS', 'AVGYO.IS', 'AVPGY.IS', 'BASGZ.IS', 'BEGYO.IS', 'DGGYO.IS', 'DZGYO.IS', 'EGEGY.IS', 'EKGYO.IS', 'EYGYO.IS', 'FZLGY.IS', 'HLGYO.IS', 'IDGYO.IS', 'ISGYO.IS', 'KGYO.IS', 'KLGYO.IS', 'KRGYO.IS', 'KZBGY.IS', 'KZGYO.IS', 'MHRGY.IS', 'MRGYO.IS', 'MSGYO.IS', 'NUGYO.IS', 'OZGYO.IS', 'OZKGY.IS', 'PAGYO.IS', 'PEKGY.IS', 'PSGYO.IS', 'RYGYO.IS', 'SEGYO.IS', 'SNGYO.IS', 'SRVGY.IS', 'SURGY.IS', 'TDGYO.IS', 'TRGYO.IS', 'TSGYO.IS', 'VKGYO.IS', 'VRGYO.IS', 'YGGYO.IS', 'ZERGY.IS', 'ZGYO.IS', 'ZRGYO.IS'],    
    'XHOLD.IS': ['AGHOL.IS', 'AKYHO.IS', 'ALARK.IS', 'ARSAN.IS', 'AVHOL.IS', 'BERA.IS', 'BINHO.IS', 'BRYAT.IS', 'BULGS.IS', 'COSMO.IS', 'DENGE.IS', 'DERHL.IS', 'DOHOL.IS', 'DUNYH.IS', 'ECILC.IS', 'ECZYT.IS', 'GLRYH.IS', 'GLYHO.IS', 'GOZDE.IS', 'GRTHO.IS', 'GSDHO.IS', 'HDFGS.IS', 'HEDEF.IS', 'HUBVC.IS', 'ICUGS.IS', 'IEYHO.IS', 'IHLAS.IS', 'IHYAY.IS', 'INVEO.IS', 'INVES.IS', 'ISGSY.IS', 'KCHOL.IS', 'KLRHO.IS', 'LRSHO.IS', 'LYDHO.IS', 'MARKA.IS', 'METRO.IS', 'MZHLD.IS', 'NTHOL.IS', 'OSTIM.IS', 'OTTO.IS', 'PAHOL.IS', 'POLHO.IS', 'PRDGS.IS', 'RALYH.IS', 'SAHOL.IS', 'SISE.IS', 'TAVHL.IS', 'TEHOL.IS', 'TKFEN.IS', 'TRCAS.IS', 'TRHOL.IS', 'UFUK.IS', 'UNLU.IS', 'VERTU.IS', 'VERUS.IS', 'YESIL.IS'],    
    'XILTM.IS': ['TCELL.IS', 'TTKOM.IS'],    
    'XINSA.IS': ['AKFIS.IS', 'ANELE.IS', 'BRLSM.IS', 'DAPGM.IS', 'EDIP.IS', 'ENKAI.IS', 'GESAN.IS', 'GLRMK.IS', 'KUYAS.IS', 'ORGE.IS', 'SANEL.IS', 'TURGG.IS', 'UCAYM.IS', 'YAYLA.IS', 'YYAPI.IS'],    
    'XKMYA.IS': ['ACSEL.IS', 'AKSA.IS', 'ALKIM.IS', 'ANGEN.IS', 'AYGAZ.IS', 'BAGFS.IS', 'BAHKM.IS', 'BAYRK.IS', 'BRISA.IS', 'BRKSN.IS', 'DEVA.IS', 'DNISI.IS', 'DYOBY.IS', 'EGGUB.IS', 'EGPRO.IS', 'EPLAS.IS', 'EUREN.IS', 'FRMPL.IS', 'GEDZA.IS', 'GOODY.IS', 'GUBRF.IS', 'HEKTS.IS', 'ISKPL.IS', 'IZFAS.IS', 'KBORU.IS', 'KMPUR.IS', 'KOPOL.IS', 'KRPLS.IS', 'MARMR.IS', 'MEDTR.IS', 'MERCN.IS', 'MRSHL.IS', 'ONCSM.IS', 'OZRDN.IS', 'PETKM.IS', 'POLTK.IS', 'RNPOL.IS', 'RTALB.IS', 'SANFM.IS', 'SASA.IS', 'SEKUR.IS', 'SEYKM.IS', 'TARKM.IS', 'TMPOL.IS', 'TRILC.IS', 'TUPRS.IS'],    
    'XMADN.IS': ['CVKMD.IS', 'PRKME.IS', 'RUZYE.IS', 'TRALT.IS', 'TRENJ.IS', 'TRMET.IS', 'VSNMD.IS'],    
    'XMANA.IS': ['BLUME.IS', 'BMSCH.IS', 'BMSTL.IS', 'BRSAN.IS', 'BURCE.IS', 'BURVA.IS', 'CELHA.IS', 'CEMAS.IS', 'CEMTS.IS', 'CUSAN.IS', 'DMSAS.IS', 'DOFER.IS', 'DOKTA.IS', 'ERBOS.IS', 'ERCB.IS', 'EREGL.IS', 'ISDMR.IS', 'IZMDC.IS', 'KCAER.IS', 'KOCMT.IS', 'KRDMA.IS', 'KRDMB.IS', 'KRDMD.IS', 'MEGMT.IS', 'OZYSR.IS', 'PNLSN.IS', 'SARKY.IS', 'TCKRC.IS', 'TUCLK.IS', 'YKSLN.IS'],    
    'XMESY.IS': ['ALCAR.IS', 'ALVES.IS', 'ARCLK.IS', 'ASTOR.IS', 'ASUZU.IS', 'BFREN.IS', 'BNTAS.IS', 'BVSAN.IS', 'DITAS.IS', 'EGEEN.IS', 'EKOS.IS', 'EMKEL.IS', 'EUPWR.IS', 'FMIZP.IS', 'FORMT.IS', 'FROTO.IS', 'GEREL.IS', 'HATSN.IS', 'HKTM.IS', 'IHEVA.IS', 'IMASM.IS', 'JANTS.IS', 'KARSN.IS', 'KATMR.IS', 'KLMSN.IS', 'MAKIM.IS', 'MAKTK.IS', 'MEKAG.IS', 'OTKAR.IS', 'OZATD.IS', 'PARSN.IS', 'PRKAB.IS', 'SAFKR.IS', 'SAYAS.IS', 'SILVR.IS', 'SNICA.IS', 'TMSN.IS', 'TOASO.IS', 'TTRAK.IS', 'ULUSE.IS', 'VESBE.IS', 'VESTL.IS', 'YIGIT.IS'],    
    'XSGRT.IS': ['AGESA.IS', 'AKGRT.IS', 'ANHYT.IS', 'ANSGR.IS', 'RAYSG.IS', 'TURSG.IS'],    
    'XSPOR.IS': ['BJKAS.IS', 'FENER.IS', 'GSRAY.IS', 'TSPOR.IS'],    
    'XTAST.IS': ['AFYON.IS', 'AKCNS.IS', 'BIENY.IS', 'BOBET.IS', 'BSOKE.IS', 'BTCIM.IS', 'BUCIM.IS', 'CGCAM.IS', 'CIMSA.IS', 'CMBTN.IS', 'DOGUB.IS', 'EGSER.IS', 'GOLTS.IS', 'KLKIM.IS', 'KLSER.IS', 'KONYA.IS', 'KUTPO.IS', 'LMKDC.IS', 'MARBL.IS', 'NIBAS.IS', 'NUHCM.IS', 'OYAKC.IS', 'QUAGR.IS', 'SERNT.IS', 'USAK.IS'],    
    'XTCRT.IS': ['ARZUM.IS', 'BIMAS.IS', 'BIZIM.IS', 'CRFSA.IS', 'DCTTR.IS', 'DOAS.IS', 'EBEBK.IS', 'GENIL.IS', 'GMTAS.IS', 'INTEM.IS', 'KIMMR.IS', 'KOTON.IS', 'MAVI.IS', 'MEPET.IS', 'MGROS.IS', 'MOPAS.IS', 'PSDTC.IS', 'SANKO.IS', 'SELEC.IS', 'SOKM.IS', 'SUWEN.IS', 'TGSAS.IS', 'TKNSA.IS', 'VAKKO.IS'],    
    'XTEKS.IS': ['ARTMS.IS', 'BLCYT.IS', 'BOSSA.IS', 'DAGI.IS', 'DERIM.IS', 'DESA.IS', 'ENSRI.IS', 'HATEK.IS', 'ISSEN.IS', 'KORDS.IS'],   
    'XTRZM.IS': ['AVTUR.IS', 'AYCES.IS', 'BIGCH.IS', 'BYDNR.IS', 'DOCO.IS', 'ETILR.IS', 'MAALT.IS', 'MARTI.IS', 'MERIT.IS', 'PKENT.IS', 'TABGD.IS', 'TEKTU.IS', 'ULAS.IS'],    
    'XULAS.IS': ['BEYAZ.IS', 'CLEBI.IS', 'GRSEL.IS', 'GSDDE.IS', 'HOROZ.IS', 'HRKET.IS', 'PASEU.IS', 'PGSUS.IS', 'RYSAS.IS', 'THYAO.IS', 'TLMAN.IS', 'TUREX.IS'],    
    'XUSIN.IS': ['ACSEL.IS', 'ADEL.IS', 'AEFES.IS', 'AFYON.IS', 'AGROT.IS', 'AKCNS.IS', 'AKHAN.IS', 'AKSA.IS', 'ALCAR.IS', 'ALKA.IS', 'ALKIM.IS', 'ALKLC.IS', 'ALVES.IS', 'ANGEN.IS', 'ARCLK.IS', 'ARMGD.IS', 'ARTMS.IS', 'ASTOR.IS', 'ASUZU.IS', 'ATAKP.IS', 'AVOD.IS', 'AYGAZ.IS', 'BAGFS.IS', 'BAHKM.IS', 'BAKAB.IS', 'BALSU.IS', 'BANVT.IS', 'BARMA.IS', 'BAYRK.IS', 'BESLR.IS', 'BFREN.IS', 'BIENY.IS', 'BLCYT.IS', 'BLUME.IS', 'BMSCH.IS', 'BMSTL.IS', 'BNTAS.IS', 'BOBET.IS', 'BORSK.IS', 'BOSSA.IS', 'BRISA.IS', 'BRKSN.IS', 'BRSAN.IS', 'BSOKE.IS', 'BTCIM.IS', 'BUCIM.IS', 'BURCE.IS', 'BURVA.IS', 'BVSAN.IS', 'CCOLA.IS', 'CELHA.IS', 'CEMAS.IS', 'CEMTS.IS', 'CEMZY.IS', 'CGCAM.IS', 'CIMSA.IS', 'CMBTN.IS', 'CUSAN.IS', 'CVKMD.IS', 'DAGI.IS', 'DARDL.IS', 'DERIM.IS', 'DESA.IS', 'DEVA.IS', 'DGNMO.IS', 'DITAS.IS', 'DMRGD.IS', 'DMSAS.IS', 'DNISI.IS', 'DOFER.IS', 'DOGUB.IS', 'DOKTA.IS', 'DURDO.IS', 'DURKN.IS', 'DYOBY.IS', 'EFOR.IS', 'EGEEN.IS', 'EGGUB.IS', 'EGPRO.IS', 'EGSER.IS', 'EKOS.IS', 'EKSUN.IS', 'ELITE.IS', 'EMKEL.IS', 'ENSRI.IS', 'EPLAS.IS', 'ERBOS.IS', 'ERCB.IS', 'EREGL.IS', 'ERSU.IS', 'EUPWR.IS', 'EUREN.IS', 'FADE.IS', 'FMIZP.IS', 'FORMT.IS', 'FRIGO.IS', 'FRMPL.IS', 'FROTO.IS', 'GEDZA.IS', 'GENTS.IS', 'GEREL.IS', 'GIPTA.IS', 'GOKNR.IS', 'GOLTS.IS', 'GOODY.IS', 'GUBRF.IS', 'GUNDG.IS', 'HATEK.IS', 'HATSN.IS', 'HEKTS.IS', 'HKTM.IS', 'IHEVA.IS', 'IMASM.IS', 'ISDMR.IS', 'ISKPL.IS', 'ISSEN.IS', 'IZFAS.IS', 'IZINV.IS', 'IZMDC.IS', 'JANTS.IS', 'KAPLM.IS', 'KARSN.IS', 'KARTN.IS', 'KATMR.IS', 'KAYSE.IS', 'KBORU.IS', 'KCAER.IS', 'KLKIM.IS', 'KLMSN.IS', 'KLSER.IS', 'KLSYN.IS', 'KMPUR.IS', 'KNFRT.IS', 'KOCMT.IS', 'KONKA.IS', 'KONYA.IS', 'KOPOL.IS', 'KORDS.IS', 'KRDMA.IS', 'KRDMB.IS', 'KRDMD.IS', 'KRPLS.IS', 'KRSTL.IS', 'KRTEK.IS', 'KRVGD.IS', 'KTSKR.IS', 'KUTPO.IS', 'LILAK.IS', 'LMKDC.IS', 'LUKSK.IS', 'MAKIM.IS', 'MAKTK.IS', 'MARBL.IS', 'MARMR.IS', 'MEDTR.IS', 'MEGMT.IS', 'MEKAG.IS', 'MERCN.IS', 'MERKO.IS', 'MEYSU.IS', 'MNDRS.IS', 'MNDTR.IS', 'MRSHL.IS', 'NIBAS.IS', 'NUHCM.IS', 'OBAMS.IS', 'OFSYM.IS', 'ONCSM.IS', 'ORCAY.IS', 'OTKAR.IS', 'OYAKC.IS', 'OYLUM.IS', 'OZATD.IS', 'OZRDN.IS', 'OZSUB.IS', 'OZYSR.IS', 'PARSN.IS', 'PENGD.IS', 'PETKM.IS', 'PETUN.IS', 'PINSU.IS', 'PNLSN.IS', 'PNSUT.IS', 'POLTK.IS', 'PRKAB.IS', 'PRKME.IS', 'PRZMA.IS', 'QUAGR.IS', 'RNPOL.IS', 'RODRG.IS', 'RTALB.IS', 'RUBNS.IS', 'RUZYE.IS', 'SAFKR.IS', 'SAMAT.IS', 'SANFM.IS', 'SARKY.IS', 'SASA.IS', 'SAYAS.IS', 'SEGMN.IS', 'SEKUR.IS', 'SELVA.IS', 'SERNT.IS', 'SEYKM.IS', 'SILVR.IS', 'SKTAS.IS', 'SNICA.IS', 'SOKE.IS', 'SUNTK.IS', 'TARKM.IS', 'TATGD.IS', 'TBORG.IS', 'TCKRC.IS', 'TEZOL.IS', 'TMPOL.IS', 'TMSN.IS', 'TOASO.IS', 'TRALT.IS', 'TRENJ.IS', 'TRILC.IS', 'TRMET.IS', 'TTRAK.IS', 'TUCLK.IS', 'TUKAS.IS', 'TUPRS.IS', 'ULKER.IS', 'ULUSE.IS', 'ULUUN.IS', 'USAK.IS', 'VANGD.IS', 'VESBE.IS', 'VESTL.IS', 'VKING.IS', 'VSNMD.IS', 'YAPRK.IS', 'YATAS.IS', 'YIGIT.IS', 'YKSLN.IS', 'YUNSA.IS', 'YYLGD.IS'],    
    'XUTEK.IS': ['ALCTL.IS', 'ALTNY.IS', 'ARDYZ.IS', 'ARENA.IS', 'ASELS.IS', 'ATATP.IS', 'AZTEK.IS', 'BINBN.IS', 'DESPC.IS', 'DGATE.IS', 'DOFRB.IS', 'EDATA.IS', 'ESCOM.IS', 'FONET.IS', 'FORTE.IS', 'HTTBT.IS', 'INDES.IS', 'INGRM.IS', 'KAREL.IS', 'KFEIN.IS', 'KRONT.IS', 'LINK.IS', 'LOGO.IS', 'MANAS.IS', 'MIATK.IS', 'MOBTL.IS', 'MTRKS.IS', 'NETAS.IS', 'NETCD.IS', 'OBASE.IS', 'ODINE.IS', 'ONRYT.IS', 'PAPIL.IS', 'PATEK.IS', 'PENTA.IS', 'PKART.IS', 'REEDR.IS', 'SDTTR.IS', 'SMART.IS', 'VBTYZ.IS'],   
    'XAKUR.IS': ['A1CAP.IS', 'GEDIK.IS', 'GLBMD.IS', 'INFO.IS', 'ISMEN.IS', 'OSMEN.IS', 'OYYAT.IS', 'SKYMD.IS', 'TERA.IS'],
    'XLBNK.IS': ['AKBNK.IS', 'GARAN.IS', 'HALKB.IS', 'ISCTR.IS', 'VAKBN.IS', 'YKBNK.IS'],
    'X10XB.IS': ['ASELS.IS', 'BIMAS.IS', 'EKGYO.IS', 'EREGL.IS', 'KCHOL.IS', 'PGSUS.IS', 'SASA.IS', 'TCELL.IS', 'THYAO.IS', 'TUPRS.IS'],
    'XSD25.IS': ['AEFES.IS', 'AKBNK.IS', 'ARCLK.IS', 'ASELS.IS', 'BIMAS.IS', 'CIMSA.IS', 'DOAS.IS', 'ENKAI.IS', 'FROTO.IS', 'GARAN.IS', 'ISCTR.IS', 'KCHOL.IS', 'MAVI.IS', 'MGROS.IS', 'OYAKC.IS', 'PETKM.IS', 'PGSUS.IS', 'SAHOL.IS', 'SISE.IS', 'TAVHL.IS', 'TCELL.IS', 'THYAO.IS', 'TSKB.IS', 'TUPRS.IS', 'ULKER.IS'],
    'XUHIZ.IS': ['A1YEN.IS', 'ADESE.IS', 'AHGAZ.IS', 'AKENR.IS', 'AKFIS.IS', 'AKFYE.IS', 'AKSEN.IS', 'AKSUE.IS', 'ALFAS.IS', 'ANELE.IS', 'ARASE.IS', 'ARFYE.IS', 'ARZUM.IS', 'AVTUR.IS', 'AYCES.IS', 'AYDEM.IS', 'AYEN.IS', 'BESTE.IS', 'BEYAZ.IS', 'BIGCH.IS', 'BIGEN.IS', 'BIGTK.IS', 'BIMAS.IS', 'BIOEN.IS', 'BIZIM.IS', 'BJKAS.IS', 'BORLS.IS', 'BRLSM.IS', 'BYDNR.IS', 'CANTE.IS', 'CATES.IS', 'CEOEM.IS', 'CLEBI.IS', 'CONSE.IS', 'CRFSA.IS', 'CWENE.IS', 'DAPGM.IS', 'DCTTR.IS', 'DOAS.IS', 'DOCO.IS', 'EBEBK.IS', 'ECOGR.IS', 'EDIP.IS', 'EGEPO.IS', 'ENDAE.IS', 'ENERY.IS', 'ENJSA.IS', 'ENKAI.IS', 'ENTRA.IS', 'ESCAR.IS', 'ESEN.IS', 'ETILR.IS', 'FENER.IS', 'FLAP.IS', 'GENIL.IS', 'GESAN.IS', 'GLRMK.IS', 'GMTAS.IS', 'GRSEL.IS', 'GSDDE.IS', 'GSRAY.IS', 'GWIND.IS', 'GZNMI.IS', 'HOROZ.IS', 'HRKET.IS', 'HUNER.IS', 'HURGZ.IS', 'IHAAS.IS', 'IHGZT.IS', 'IHLGM.IS', 'INTEM.IS', 'IZENR.IS', 'KIMMR.IS', 'KLYPV.IS', 'KONTR.IS', 'KOTON.IS', 'KUYAS.IS', 'LIDER.IS', 'LKMNH.IS', 'LYDYE.IS', 'MAALT.IS', 'MACKO.IS', 'MAGEN.IS', 'MARTI.IS', 'MAVI.IS', 'MEPET.IS', 'MERIT.IS', 'MGROS.IS', 'MOGAN.IS', 'MOPAS.IS', 'MPARK.IS', 'NATEN.IS', 'NTGAZ.IS', 'ODAS.IS', 'ORGE.IS', 'PAMEL.IS', 'PASEU.IS', 'PCILT.IS', 'PGSUS.IS', 'PKENT.IS', 'PLTUR.IS', 'PSDTC.IS', 'RGYAS.IS', 'RYSAS.IS', 'SANEL.IS', 'SANKO.IS', 'SELEC.IS', 'SKYLP.IS', 'SMRTG.IS', 'SOKM.IS', 'SONME.IS', 'SUWEN.IS', 'TABGD.IS', 'TATEN.IS', 'TCELL.IS', 'TEKTU.IS', 'TGSAS.IS', 'THYAO.IS', 'TKNSA.IS', 'TLMAN.IS', 'TNZTP.IS', 'TSPOR.IS', 'TTKOM.IS', 'TUREX.IS', 'TURGG.IS', 'UCAYM.IS', 'ULAS.IS', 'VAKKO.IS', 'YAYLA.IS', 'YEOTK.IS', 'YYAPI.IS', 'ZEDUR.IS', 'ZOREN.IS'],
    'XYORT.IS': ['ATLAS.IS', 'ETYAT.IS', 'EUKYO.IS', 'EUYO.IS', 'GRNYO.IS', 'ISYAT.IS', 'MTRYO.IS', 'OYAYO.IS', 'VKFYO.IS'],
    'XYUZO.IS': ['AGHOL.IS', 'AKSA.IS', 'AKSEN.IS', 'ALARK.IS', 'ALTNY.IS', 'ANSGR.IS', 'ARCLK.IS', 'BALSU.IS', 'BRSAN.IS', 'BRYAT.IS', 'BSOKE.IS', 'BTCIM.IS', 'CANTE.IS', 'CCOLA.IS', 'CIMSA.IS', 'CWENE.IS', 'DAPGM.IS', 'DOAS.IS', 'DOHOL.IS', 'ECILC.IS', 'EFOR.IS', 'EGEEN.IS', 'ENERY.IS', 'ENJSA.IS', 'EUPWR.IS', 'FENER.IS', 'GENIL.IS', 'GESAN.IS', 'GLRMK.IS', 'GRSEL.IS', 'GRTHO.IS', 'GSRAY.IS', 'HALKB.IS', 'HEKTS.IS', 'ISMEN.IS', 'IZENR.IS', 'KCAER.IS', 'KLRHO.IS', 'KONTR.IS', 'KTLEV.IS', 'KUYAS.IS', 'MAGEN.IS', 'MAVI.IS', 'MIATK.IS', 'MPARK.IS', 'OBAMS.IS', 'ODAS.IS', 'OTKAR.IS', 'OYAKC.IS', 'PASEU.IS', 'PATEK.IS', 'QUAGR.IS', 'RALYH.IS', 'REEDR.IS', 'SKBNK.IS', 'SOKM.IS', 'TABGD.IS', 'TKFEN.IS', 'TRENJ.IS', 'TRMET.IS', 'TSKB.IS', 'TSPOR.IS', 'TTRAK.IS', 'TUKAS.IS', 'TUREX.IS', 'TURSG.IS', 'VAKBN.IS', 'VESTL.IS', 'YEOTK.IS', 'ZOREN.IS'],
    
    # --- EKSİK ENDEKSLER (DOLDURULACAK) ---
    'XU500.IS': [],
    'XUTUM.IS': [],
    'XHARZ.IS': [],
    'XK030.IS': [],
    'XKTMT.IS': [],
    'XKTUM.IS': [],
    'XTM25.IS': [],
    'XTMTU.IS': [],
    'XTUMY.IS': [],
    'XUGRA.IS': [],
    'XUMAL.IS': [],
    'XYLDZ.IS': [],
}

def get_db_price(symbol, target_date, table_name='bist_index_history'):
    """
    Veritabanından belirli bir tarihteki fiyatı çeker.
    Eğer o tarihte yoksa, None döner.
    """
    try:
        response = supabase.table(table_name) \
            .select('close') \
            .eq('symbol', symbol) \
            .eq('date', target_date.strftime('%Y-%m-%d')) \
            .execute()
        
        if response.data and len(response.data) > 0:
            return float(response.data[0]['close'])
        return None
    except Exception as e:
        print(f"DB Read Error for {symbol} in {table_name} on {target_date}: {e}")
        return None

def upsert_price(symbol, price, date, table_name='bist_index_history'):
    """
    Günlük kapanış fiyatını veritabanına kaydeder/günceller.
    """
    try:
        data = {
            'symbol': symbol.replace('.IS', ''),
            'date': date.strftime('%Y-%m-%d'),
            'close': price
        }
        supabase.table(table_name).upsert(data).execute()
    except Exception as e:
        print(f"DB Upsert Error for {symbol} in {table_name}: {e}")

def get_last_friday(today):
    """
    Verilen tarihten önceki son Cuma gününü bulur.
    Eğer bugün Cuma ise ve piyasa kapanmadıysa geçen haftanın Cuma'sını mı alır?
    Genellikle 'Haftalık Değişim' için Pazartesi açılışı veya Cuma kapanışı baz alınır.
    İstek: "Haftayı Pzt-Cuma olarak hesapla".
    Bugün Pazartesi ise -> Referans Cuma.
    Bugün Çarşamba ise -> Referans Cuma.
    Bugün Cuma ise -> Referans GEÇEN Cuma (böylece bu haftanın performansı olur).
    """
    idx = (today.weekday() + 1) % 7 # Mon=0 -> ...
    # Pazartesi=0, Salı=1, ... Cuma=4, ... Pazar=6
    
    # Cuma gününü bulmak için geriye gitmemiz gereken gün sayısı
    # Eğer bugün Pazartesi (0) ise, Cuma (4) için 3 gün geri gitmeliyiz. (0-3 = -3 -> mod 7 = 4)
    # Eğer bugün Cuma (4) ise, geçen haftanın Cuması için 7 gün geri gitmeliyiz.
    
    days_back = (today.weekday() - 4) % 7
    if days_back == 0:
        days_back = 7 # Bugün Cuma ise geçen Cuma'yı al
    elif today.weekday() >= 5: # Cumartesi veya Pazar ise
        days_back += 7 # Geçen haftanın Cumasını al (bugünün/dünün Cumasını değil)
    
    return today - timedelta(days=days_back)

def get_previous_trading_day(today):
    """
    Bir önceki iş gününü (basitçe) bulur.
    Hafta sonlarını atlar.
    """
    days_back = 1
    if today.weekday() == 0: # Pazartesi ise
        days_back = 3 # Cuma'ya git
    elif today.weekday() == 6: # Pazar ise
        days_back = 2 # Cuma'ya git
    
    return today - timedelta(days=days_back)

def fetch_and_calculate(symbol, clean_sym, history_table='bist_index_history'):
    try:
        ticker = yf.Ticker(symbol)
        # Sadece son fiyatı anlık alalım
        todays_data = ticker.history(period="1d")
        
        if todays_data.empty:
            return None
            
        current_price = float(todays_data['Close'].iloc[-1])
        current_vol = float(todays_data['Volume'].iloc[-1]) if 'Volume' in todays_data.columns else 0
        
        now = datetime.now()
        
        # 1. Bugünü Veritabanına Kaydet
        upsert_price(symbol, current_price, now, history_table)
        
        # 2. Geçmiş Verileri Veritabanından Çek
        # Günlük Değişim için Dün
        yesterday = get_previous_trading_day(now)
        price_yesterday = get_db_price(clean_sym, yesterday, history_table)
        
        # Haftalık Değişim için Geçen Cuma
        last_friday = get_last_friday(now)
        price_last_friday = get_db_price(clean_sym, last_friday, history_table)

        # --- YALNIZCA HİSSELER İÇİN GEÇMİŞ VERİ DOLDURMA (FALLBACK) ---
        if (price_yesterday is None or price_last_friday is None) and history_table == 'bist_price_history':
            print(f"[{clean_sym}] DB'de eksik veri var, Yahoo Finance'den çekiliyor...")
            hist_extra = ticker.history(period="1mo")
            if not hist_extra.empty:
                # Tarihleri karşılaştırabilmek için index'i date tipine çevirelim
                hist_extra.index = hist_extra.index.date
                
                # Dün verisi eksikse doldur
                y_date = yesterday.date()
                if price_yesterday is None and y_date in hist_extra.index:
                    price_yesterday = float(hist_extra.loc[y_date]['Close'])
                    upsert_price(symbol, price_yesterday, yesterday, history_table)
                    print(f"  - Dün ({y_date}) verisi Yahoo'dan çekildi ve kaydedildi.")
                
                # Geçen Cuma verisi eksikse doldur
                f_date = last_friday.date()
                if price_last_friday is None and f_date in hist_extra.index:
                    price_last_friday = float(hist_extra.loc[f_date]['Close'])
                    upsert_price(symbol, price_last_friday, last_friday, history_table)
                    print(f"  - Geçen Cuma ({f_date}) verisi Yahoo'dan çekildi ve kaydedildi.")

        # Hesaplamalar
        change_1d = 0
        if price_yesterday and price_yesterday != 0:
            change_1d = (current_price - price_yesterday) / price_yesterday * 100
        
        change_1w = 0
        if price_last_friday and price_last_friday != 0:
            change_1w = (current_price - price_last_friday) / price_last_friday * 100
        
        return {
            'last_price': round(current_price, 2),
            'change_1d': round(change_1d, 2),
            'change_1w': round(change_1w, 2),
            'volume': f"{round(current_vol / 1_000_000, 1)}M",
            'price_yesterday': price_yesterday,
            'price_last_friday': price_last_friday
        }

    except Exception as e:
        print(f"Error for {symbol}: {e}")
        return None



def main():
    print(f"BIST Data Fetcher Started: {datetime.now()}")
    
    results_indices = []
    results_stocks = []
    
    # --- ENDEKSLERİ İŞLE ---
    print(f"Processing Indices...") 
    for symbol, info in INDICES.items():
        clean_sym = symbol.replace('.IS', '')
        # Manuel DB Hesaplaması
        stats = fetch_and_calculate(symbol, clean_sym, history_table='bist_index_history')
        
        if stats:
            print(f"{clean_sym}: {stats['last_price']} (1D: {stats['change_1d']}%, 1W: {stats['change_1w']}%) [Ref: {stats['price_yesterday']}, {stats['price_last_friday']}]")
            
            results_indices.append({
                'code': clean_sym,
                'name': info['name'],
                'category': info['category'],
                'last_price': stats['last_price'],
                'change1d': stats['change_1d'],
                'change1w': stats['change_1w'], # DB Hesaplı Manual
                'change2w': 0,
                'change3w': 0,
                'change1m': 0,
                'change3m': 0,
                'volume': stats['volume'],
                'updated_at': datetime.now().isoformat()
            })
        time.sleep(0.5)

    # --- HİSSELERİ İŞLE ---
    unique_stocks = set()
    stock_to_parents = {}
    if 'CONSTITUENTS' in globals():
        for idx_code, stock_list in CONSTITUENTS.items():
            clean_idx = idx_code.replace('.IS', '')
            for s in stock_list:
                unique_stocks.add(s)
                if s not in stock_to_parents: stock_to_parents[s] = []
                if clean_idx not in stock_to_parents[s]: stock_to_parents[s].append(clean_idx)

    print(f"Processing {len(unique_stocks)} stocks...")
    for i, symbol in enumerate(list(unique_stocks)):
        clean_sym = symbol.replace('.IS', '')
        # Hisseler için de artık veritabanı history kullanıyoruz!
        stats = fetch_and_calculate(symbol, clean_sym, history_table='bist_price_history')
        
        if stats:
            results_stocks.append({
                'symbol': clean_sym,
                'parent_index': ",".join(stock_to_parents.get(symbol, [])),
                'price': stats['last_price'],
                'change1d': stats['change_1d'],
                'change1w': stats['change_1w'],
                'change2w': 0, 
                'change3w': 0,
                'change1m': 0, # Şimdilik geçmiş olmadığı için 0
                'change3m': 0, # Şimdilik geçmiş olmadığı için 0
                'updated_at': datetime.now().isoformat()
            })
        if i % 20 == 0: time.sleep(0.3)

    # --- KAYIT (UPSERT) ---
    try:
        if results_indices:
            supabase.table('bist_indices').upsert(results_indices).execute()
        
        if results_stocks:
            for i in range(0, len(results_stocks), 100):
                supabase.table('bist_stocks').upsert(results_stocks[i:i + 100]).execute()
        
        print(f"✅ SUCCESS: Data (Indices & Stocks) updated successfully.")
    except Exception as e:
        print(f"❌ DB ERROR: {e}")

if __name__ == "__main__":
    main()
