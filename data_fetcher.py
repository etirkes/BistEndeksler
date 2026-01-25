import yfinance as yf
import pandas as pd
from supabase import create_client, Client
import os
import time
from datetime import datetime, timedelta

# --- SETTINGS ---
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    print("Error: SUPABASE_URL and SUPABASE_KEY are not defined.")

try:
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
except Exception as e:
    print(f"Supabase connection error: {e}")

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
        'FROTO.IS', 'GARAN.IS', 'GUBRF.IS', 'HEKTS.IS', 'ISCTR.IS', 'KCHOL.IS', 'KONTR.IS', 'KOZAL.IS', 'KRDMD.IS', 'ODAS.IS',
        'OYAKC.IS', 'PGSUS.IS', 'SAHOL.IS', 'SASA.IS', 'SISE.IS', 'TCELL.IS', 'THYAO.IS', 'TOASO.IS', 'TUPRS.IS', 'YKBNK.IS'
    ],
    'XU050.IS': [
        'AKBNK.IS', 'ALARK.IS', 'ARCLK.IS', 'ASELS.IS', 'ASTOR.IS', 'BIMAS.IS', 'BRSAN.IS', 'EKGYO.IS', 'ENKAI.IS', 'EREGL.IS',
        'FROTO.IS', 'GARAN.IS', 'GUBRF.IS', 'HEKTS.IS', 'ISCTR.IS', 'KCHOL.IS', 'KONTR.IS', 'KOZAL.IS', 'KRDMD.IS', 'ODAS.IS',
        'OYAKC.IS', 'PGSUS.IS', 'SAHOL.IS', 'SASA.IS', 'SISE.IS', 'TCELL.IS', 'THYAO.IS', 'TOASO.IS', 'TUPRS.IS', 'YKBNK.IS',
        'AEFES.IS', 'AGHOL.IS', 'AKSA.IS', 'AKSEN.IS', 'ALFAS.IS', 'BERA.IS', 'CANTE.IS', 'CCOLA.IS', 'CIMSA.IS', 'DOHOL.IS',
        'EGEEN.IS', 'ENJSA.IS', 'EUPWR.IS', 'GESAN.IS', 'HALKB.IS', 'ISGYO.IS', 'KOZAA.IS', 'MGROS.IS', 'SMRTG.IS', 'SOKM.IS',
        'TTKOM.IS', 'ULKER.IS', 'VAKBN.IS', 'VESTL.IS'
    ],
    'XU100.IS': [
        'AGHOL.IS', 'AKBNK.IS', 'AKSA.IS', 'AKSEN.IS', 'ALARK.IS', 'ALBRK.IS', 'ALFAS.IS', 'ARCLK.IS', 'ASELS.IS', 'ASTOR.IS',
        'BERA.IS', 'BIMAS.IS', 'BIOEN.IS', 'BRSAN.IS', 'BRYAT.IS', 'BUCIM.IS', 'CANTE.IS', 'CCOLA.IS', 'CEMTS.IS', 'CIMSA.IS',
        'CWENE.IS', 'DOAS.IS', 'DOHOL.IS', 'ECILC.IS', 'ECZYT.IS', 'EGEEN.IS', 'EKGYO.IS', 'ENJSA.IS', 'ENKAI.IS', 'EREGL.IS',
        'EUPWR.IS', 'EUREN.IS', 'FROTO.IS', 'GARAN.IS', 'GENIL.IS', 'GESAN.IS', 'GLYHO.IS', 'GSDHO.IS', 'GUBRF.IS', 'HALKB.IS',
        'HEKTS.IS', 'IMASM.IS', 'IPEKE.IS', 'ISCTR.IS', 'ISDMR.IS', 'ISGYO.IS', 'ISMEN.IS', 'IZMDC.IS', 'KARSN.IS', 'KAYSE.IS',
        'KCAER.IS', 'KCHOL.IS', 'KONTR.IS', 'KONYA.IS', 'KOZAA.IS', 'KOZAL.IS', 'KRDMD.IS', 'KZBGY.IS', 'MAVI.IS', 'MGROS.IS',
        'MIATK.IS', 'ODAS.IS', 'OTKAR.IS', 'OYAKC.IS', 'PENTA.IS', 'PETKM.IS', 'PGSUS.IS', 'PSGYO.IS', 'QUAGR.IS', 'SAHOL.IS',
        'SASA.IS', 'SELEC.IS', 'SISE.IS', 'SKBNK.IS', 'SMRTG.IS', 'SNGYO.IS', 'SOKM.IS', 'TAVHL.IS', 'TCELL.IS', 'THYAO.IS',
        'TKFEN.IS', 'TOASO.IS', 'TSKB.IS', 'TTKOM.IS', 'TTRAK.IS', 'TUKAS.IS', 'TUPRS.IS', 'TURSG.IS', 'ULKER.IS', 'VAKBN.IS',
        'VESBE.IS', 'VESTL.IS', 'YEOTK.IS', 'YKBNK.IS', 'YYLGD.IS', 'ZOREN.IS'
    ],
    
    # --- SEKTÖR ENDEKSLERİ ---
    'XBANK.IS': ['AKBNK.IS', 'GARAN.IS', 'ISCTR.IS', 'YKBNK.IS', 'VAKBN.IS', 'HALKB.IS', 'TSKB.IS', 'ALBRK.IS', 'SKBNK.IS', 'ICBCT.IS', 'QNBFB.IS'],    
    'XBLSM.IS': ['LOGO.IS', 'VBTYZ.IS', 'ARDYZ.IS', 'MIATK.IS', 'KFEIN.IS', 'FONET.IS', 'LINK.IS', 'PATEK.IS', 'OBSGK.IS', 'ATPTP.IS', 'HTTBT.IS', 'MTRKS.IS'],    
    'XELKT.IS': ['ENJSA.IS', 'ZOREN.IS', 'ODAS.IS', 'AKSEN.IS', 'ASTOR.IS', 'GWIND.IS', 'BIOEN.IS', 'AYDEM.IS', 'CWENE.IS', 'EUPWR.IS', 'GESAN.IS', 'SMRTG.IS', 'YEOTK.IS', 'ALFAS.IS', 'MAGEN.IS', 'HUNER.IS'],    
    'XFINK.IS': ['VAKFN.IS', 'GARFA.IS', 'ISFIN.IS', 'QNBFL.IS', 'LIDFA.IS'],    
    'XGIDA.IS': ['CCOLA.IS', 'AEFES.IS', 'BIMAS.IS', 'MGROS.IS', 'ULKER.IS', 'SOKM.IS', 'TUKAS.IS', 'TATGD.IS', 'KERVT.IS', 'PETUN.IS', 'PINSU.IS', 'PNSUT.IS', 'KRVGD.IS', 'KAYSE.IS'],    
    'XGMYO.IS': ['EKGYO.IS', 'ISGYO.IS', 'TRGYO.IS', 'AKFGY.IS', 'SNGYO.IS', 'KZBGY.IS', 'PSGYO.IS', 'HLGYO.IS', 'OZKGY.IS', 'RYGYO.IS', 'ALGYO.IS'],    
    'XHOLD.IS': ['KCHOL.IS', 'SAHOL.IS', 'DOHOL.IS', 'AGHOL.IS', 'TEKTU.IS', 'GSDHO.IS', 'ALARK.IS', 'TKFEN.IS', 'GLYHO.IS', 'NTHOL.IS', 'BERA.IS', 'IEYHO.IS'],    
    'XILTM.IS': ['TCELL.IS', 'TTKOM.IS'],    
    'XINSA.IS': ['ENKAI.IS', 'TEKFEN.IS', 'ORGE.IS', 'YAYLA.IS'],    
    'XKMYA.IS': ['SASA.IS', 'HEKTS.IS', 'PETKM.IS', 'TUPRS.IS', 'ALKIM.IS', 'BAGFS.IS', 'KORDS.IS', 'AKSA.IS', 'DYOBY.IS', 'MRSHL.IS', 'GUBRF.IS', 'EGGUB.IS', 'DEVA.IS'],    
    'XMADN.IS': ['KOZAL.IS', 'KOZAA.IS', 'IPEKE.IS', 'PARSN.IS', 'CVKMD.IS'],    
    'XMANA.IS': ['EREGL.IS', 'KRDMD.IS', 'ISDMR.IS', 'CEMTS.IS', 'KARDM.IS', 'KCAER.IS', 'IZMDC.IS', 'BOSSA.IS', 'BURCE.IS'],    
    'XMESY.IS': ['FROTO.IS', 'TOASO.IS', 'TTRAK.IS', 'ARCLK.IS', 'VESBE.IS', 'VESTL.IS', 'OTKAR.IS', 'TUKAS.IS', 'JANTS.IS', 'KATMR.IS', 'TMSN.IS'],    
    'XSGRT.IS': ['TURSG.IS', 'AKGRT.IS', 'ANSGR.IS', 'AGESA.IS', 'ANHYT.IS', 'RAYSG.IS'],    
    'XSPOR.IS': ['BJKAS.IS', 'FENER.IS', 'GSRAY.IS', 'TSPOR.IS'],    
    'XTAST.IS': ['CIMSA.IS', 'AKCNS.IS', 'OYAKC.IS', 'NUHCM.IS', 'BUCIM.IS', 'AFYON.IS', 'GOLTS.IS', 'KONYA.IS', 'BTCIM.IS', 'BSOKE.IS'],    
    'XTCRT.IS': ['BIMAS.IS', 'MGROS.IS', 'SOKM.IS', 'MAVI.IS', 'VAKKO.IS'],    
    'XTEKS.IS': ['MAVI.IS', 'VAKKO.IS', 'YUNSA.IS', 'KORDS.IS', 'MNDRS.IS', 'SKTAS.IS', 'ARSAN.IS'],   
    'XTRZM.IS': ['MAALT.IS', 'TEKTU.IS', 'AYCES.IS', 'DOCO.IS', 'TLMAN.IS', 'METUR.IS'],    
    'XULAS.IS': ['THYAO.IS', 'PGSUS.IS', 'TAVHL.IS', 'CLEBI.IS', 'DOCO.IS', 'RYSAS.IS', 'TLMAN.IS', 'GSDDE.IS'],    
    'XUSIN.IS': ['EREGL.IS', 'TUPRS.IS', 'SAHOL.IS', 'SISE.IS', 'KCHOL.IS', 'FROTO.IS', 'TOASO.IS', 'ARCLK.IS', 'ASELS.IS', 'PETKM.IS', 'SASA.IS', 'HEKTS.IS', 'ENKAI.IS', 'CCOLA.IS', 'AEFES.IS'],    
    'XUTEK.IS': ['ASELS.IS', 'LOGO.IS', 'MIATK.IS', 'REEDR.IS', 'VBTYZ.IS', 'KFEIN.IS', 'SDTTR.IS', 'PATEK.IS', 'ALVES.IS', 'KAREL.IS', 'NETAS.IS', 'ESCOM.IS', 'ALCTL.IS', 'FONET.IS', 'KRONT.IS', 'LINK.IS', 'PKART.IS'],   
    'XAKUR.IS': ['A1CAP.IS', 'GEDIK.IS', 'GLBMD.IS', 'INFO.IS', 'ISMEN.IS', 'OSMEN.IS', 'OYYAT.IS', 'SKYMD.IS', 'TERA.IS'],
    'XLBNK.IS': ['AKBNK.IS', 'GARAN.IS', 'HALKB.IS', 'ISCTR.IS', 'VAKBN.IS', 'YKBNK.IS'],
    'X10XB.IS': ['ASELS.IS', 'BIMAS.IS', 'EKGYO.IS', 'EREGL.IS', 'KCHOL.IS', 'PGSUS.IS', 'SASA.IS', 'TCELL.IS', 'TUPRS.IS', 'THYAO.IS'],
    'XSD25.IS': ['AKBNK.IS', 'AEFES.IS', 'ARCLK.IS', 'ASELS.IS', 'BIMAS.IS', 'CIMSA.IS', 'DOAS.IS', 'ENKAI.IS', 'FROTO.IS', 'SAHOL.IS', 'KCHOL.IS', 'MAVI.IS', 'MGROS.IS', 'OYAKC.IS', 'PGSUS.IS', 'PETKM.IS', 'TAVHL.IS', 'TCELL.IS', 'TUPRS.IS', 'THYAO.IS', 'GARAN.IS', 'ISCTR.IS', 'TSKB.IS', 'SISE.IS', 'ULKER.IS'],
    'XUHIZ.IS': ['A1YEN.IS', 'ADESE.IS', 'AHGAZ.IS', 'AKENR.IS', 'AKFIS.IS', 'AKFYE.IS', 'AKSEN.IS', 'AKSUE.IS', 'ALFAS.IS', 'AYCES.IS', 'ANELE.IS', 'ARFYE.IS', 'ARZUM.IS', 'AVTUR.IS', 'AYDEM.IS', 'AYEN.IS', 'BYDNR.IS', 'BJKAS.IS', 'BEYAZ.IS', 'BIGTK.IS', 'BIMAS.IS', 'BIOEN.IS', 'BIGEN.IS', 'BRLSM.IS', 'BIZIM.IS', 'BORLS.IS', 'BIGCH.IS', 'CRFSA.IS', 'CEOEM.IS', 'CONSE.IS', 'CWENE.IS', 'CANTE.IS', 'CATES.IS', 'CLEBI.IS', 'DAPGM.IS', 'DCTTR.IS', 'DOCO.IS', 'ARASE.IS', 'DOAS.IS', 'EBEBK.IS', 'ECOGR.IS', 'EDIP.IS', 'ENDAE.IS', 'ENJSA.IS', 'ENERY.IS', 'ENKAI.IS', 'KIMMR.IS', 'ESCAR.IS', 'ESEN.IS', 'ETILR.IS', 'FENER.IS', 'FLAP.IS', 'GSRAY.IS', 'GWIND.IS', 'GENIL.IS', 'GZNMI.IS', 'GMTAS.IS', 'GESAN.IS', 'GSDDE.IS', 'GLRMK.IS', 'GRSEL.IS', 'HRKET.IS', 'HOROZ.IS', 'HUNER.IS', 'HURGZ.IS', 'ENTRA.IS', 'IHLGM.IS', 'IHGZT.IS', 'IHAAS.IS', 'INTEM.IS', 'IZENR.IS', 'KLYPV.IS', 'KONTR.IS', 'KOTON.IS', 'KUYAS.IS', 'LIDER.IS', 'LKMNH.IS', 'LYDYE.IS', 'MACKO.IS', 'MAGEN.IS', 'MAALT.IS', 'MARTI.IS', 'MAVI.IS', 'MEPET.IS', 'MERIT.IS', 'MGROS.IS', 'MPARK.IS', 'MOGAN.IS', 'MOPAS.IS', 'EGEPO.IS', 'NTGAZ.IS', 'NATEN.IS', 'ODAS.IS', 'ORGE.IS', 'PAMEL.IS', 'PASEU.IS', 'PCILT.IS', 'PGSUS.IS', 'PSDTC.IS', 'PKENT.IS', 'PLTUR.IS', 'RYSAS.IS', 'RGYAS.IS', 'SANEL.IS', 'SANKO.IS', 'SELEC.IS', 'SKYLP.IS', 'SMRTG.IS', 'SONME.IS', 'SUWEN.IS', 'SOKM.IS', 'TABGD.IS', 'TNZTP.IS', 'TATEN.IS', 'TEKTU.IS', 'TKNSA.IS', 'TGSAS.IS', 'TLMAN.IS', 'TSPOR.IS', 'TUREX.IS', 'TCELL.IS', 'TURGG.IS', 'THYAO.IS', 'TTKOM.IS', 'ULAS.IS', 'UCAYM.IS', 'VAKKO.IS', 'YAYLA.IS', 'YEOTK.IS', 'YYAPI.IS', 'ZEDUR.IS', 'ZOREN.IS'],
    'XYORT.IS': ['ATLAS.IS', 'EUKYO.IS', 'EUYO.IS', 'ETYAT.IS', 'GRNYO.IS', 'ISYAT.IS', 'MTRYO.IS', 'OYAYO.IS', 'VKFYO.IS'],
    'XYUZO.IS': ['AGHOL.IS', 'AKSA.IS', 'AKSEN.IS', 'ALARK.IS', 'ALTNY.IS', 'ANSGR.IS', 'ARCLK.IS', 'BALSU.IS', 'BTCIM.IS', 'BSOKE.IS', 'BRSAN.IS', 'BRYAT.IS', 'CCOLA.IS', 'CWENE.IS', 'CANTE.IS', 'CIMSA.IS', 'DAPGM.IS', 'DOHOL.IS', 'DOAS.IS', 'EFOR.IS', 'EGEEN.IS', 'ECILC.IS', 'ENJSA.IS', 'ENERY.IS', 'EUPWR.IS', 'FENER.IS', 'GSRAY.IS', 'GENIL.IS', 'GESAN.IS', 'GRTHO.IS', 'GLRMK.IS', 'GRSEL.IS', 'HEKTS.IS', 'ISMEN.IS', 'IZENR.IS', 'KTLEV.IS', 'KLRHO.IS', 'KCAER.IS', 'KONTR.IS', 'KUYAS.IS', 'MAGEN.IS', 'MAVI.IS', 'MIATK.IS', 'MPARK.IS', 'OBAMS.IS', 'ODAS.IS', 'OTKAR.IS', 'OYAKC.IS', 'PASEU.IS', 'PATEK.IS', 'QUAGR.IS', 'RALYH.IS', 'REEDR.IS', 'SKBNK.IS', 'SOKM.IS', 'TABGD.IS', 'TKFEN.IS', 'TSPOR.IS', 'TRMET.IS', 'TRENJ.IS', 'TUKAS.IS', 'TUREX.IS', 'HALKB.IS', 'TSKB.IS', 'TURSG.IS', 'VAKBN.IS', 'TTRAK.IS', 'VESTL.IS', 'YEOTK.IS', 'ZOREN.IS']
}

# --- HELPER FUNCTIONS ---

def fetch_with_retry(symbol):
    """
    Kademeli veri çekme yöntemi:
    1. 1 Yıllık (Haftalık/Aylık hesaplamalar için ideal)
    2. 1 Aylık (Haftalık hesaplama için yeterli)
    3. 5 Günlük (Sadece güncel fiyat için - Test kodunun başarılı olduğu periyot)
    """
    try:
        ticker = yf.Ticker(symbol)
        
        # Try 1 Year first
        hist = ticker.history(period="1y")
        is_fallback = False
        
        if hist.empty or len(hist) < 2:
            print(f"  ! {symbol}: 1-year failed, trying 1-month...")
            hist = ticker.history(period="1mo")
            if hist.empty or len(hist) < 2:
                print(f"  !! {symbol}: 1-month failed, trying 5-day...")
                hist = ticker.history(period="5d")
                is_fallback = True

        if hist.empty:
            return None
            
        # Clean timezone
        if hasattr(hist.index, 'tz_localize'):
            hist.index = hist.index.tz_localize(None)
        
        closes = hist['Close'].dropna()
        if len(closes) < 2:
            return None

        curr_price = float(closes.iloc[-1])
        last_date = closes.index[-1]
        
        def get_p(days_back):
            try:
                # If we are in fallback (5-day), we can't look back 30 or 90 days.
                target = last_date - timedelta(days=days_back)
                idx = closes.index.asof(target)
                if pd.isna(idx):
                    return float(closes.iloc[0]) # Fallback to oldest available
                return float(closes.loc[idx])
            except:
                return float(closes.iloc[0])

        p1d = float(closes.iloc[-2])
        p1w = get_p(7)
        p1m = get_p(30)
        p3m = get_p(90)

        # Percentage calculation helper
        def pct(new, old):
            if not old or old == 0: return 0
            val = round(((new - old) / old) * 100, 2)
            # If we only have 5 days of data, 1m and 3m changes might be 
            # misleadingly based on the 1st day of those 5 days.
            return val

        # Handle weight (yfinance doesn't provide this directly, using placeholder)
        weight = None 

        vol = float(hist['Volume'].iloc[-1]) if 'Volume' in hist.columns else 0

        return {
            'last_price': round(curr_price, 2),
            'change_1d': pct(curr_price, p1d),
            'change_1w': pct(curr_price, p1w),
            'change_1m': pct(curr_price, p1m),
            'change_3m': pct(curr_price, p3m),
            'volume': f"{round(vol / 1_000_000, 1)}M",
            'weight': weight
        }
    except Exception as e:
        print(f"Critical error fetching {symbol}: {e}")
        return None

def chunk_list(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

# --- MAIN LOOP ---

def main():
    print(f"BIST Data Fetcher Started at {datetime.now()} (Multi-Period Retry Mode)")
    
    # 1. Process All Indices
    all_indices = list(INDICES.keys())
    results_indices = []
    
    print(f"Scanning {len(all_indices)} indices...")
    for symbol in all_indices:
        stats = fetch_with_retry(symbol)
        if stats:
            results_indices.append({
                'code': symbol.replace('.IS', ''),
                'name': INDICES[symbol]['name'],
                'category': INDICES[symbol]['category'],
                'last_price': stats['last_price'],
                'change1d': stats['change_1d'],
                'change1w': stats['change_1w'],
                'change1m': stats['change_1m'],
                'change3m': stats['change_3m'],
                'volume': stats['volume'],
                'updated_at': datetime.now().isoformat()
            })
        time.sleep(0.5) # Anti-ban delay

    # Write Indices
    if results_indices:
        try:
            supabase.table('bist_indices').upsert(results_indices).execute()
            print(f"✅ SUCCESS: {len(results_indices)} indices updated in database.")
        except Exception as e:
            print(f"❌ DB Error (Indices): {e}")

    # 2. Process Unique Stocks
    unique_stocks = set()
    stock_to_parents = {}

    for index_code, stock_list in CONSTITUENTS.items():
        clean_idx = index_code.replace('.IS', '')
        for s in stock_list:
            unique_stocks.add(s)
            if s not in stock_to_parents:
                stock_to_parents[s] = []
            if clean_idx not in stock_to_parents[s]:
                stock_to_parents[s].append(clean_idx)

    unique_stocks_list = list(unique_stocks)
    print(f"Scanning {len(unique_stocks_list)} unique stocks...")
    results_stocks = []
    
    for i, symbol in enumerate(unique_stocks_list):
        if i % 20 == 0 and i > 0:
            print(f"  Stock progress: {i}/{len(unique_stocks_list)}...")
            
        stats = fetch_with_retry(symbol)
        if stats:
            results_stocks.append({
                'symbol': symbol.replace('.IS', ''),
                'parent_index': ",".join(stock_to_parents[symbol]),
                'price': stats['last_price'],
                'change1d': stats['change_1d'],
                'change1w': stats['change_1w'],
                'change1m': stats['change_1m'],
                'change3m': stats['change_3m'],
                'weight': stats['weight'],
                'updated_at': datetime.now().isoformat()
            })
        time.sleep(0.3)

    # Write Stocks in chunks
    if results_stocks:
        try:
            for chunk in chunk_list(results_stocks, 100):
                supabase.table('bist_stocks').upsert(chunk).execute()
            print(f"✅ SUCCESS: {len(results_stocks)} stocks updated in database.")
        except Exception as e:
            print(f"❌ DB Error (Stocks): {e}")

if __name__ == "__main__":
    main()
