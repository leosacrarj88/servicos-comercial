# -*- coding: utf-8 -*-
"""
Mapa Comercial por categorias
✅ Um único arquivo .py
✅ Auto-start Streamlit ao rodar pelo Play do VS Code (sem loop)
✅ Resultados persistentes (st.form + st.session_state)
✅ Busca por categorias:
   - Google Places Nearby Search + Details (telefone, website, horários, url) se tiver API key
   - OSM/Overpass fallback (cache + fallback de endpoints)
✅ Cards primeiro, em grade (até 5 por linha, configurável na sidebar)
✅ Mapa interativo (Folium/OSM) com MarkerCluster + links Google Maps/Rotas

Requisitos:
pip install streamlit pandas requests geopy folium streamlit-folium
"""

# ===============================
# EXECUÇÃO
# ===============================
# ===============================
# IMPORTS
# ===============================
import time
import concurrent.futures
import math
import json
import re
import html
import base64
import requests
import pandas as pd
import streamlit as st
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut
import folium
from folium.plugins import MarkerCluster
from streamlit_folium import st_folium
import sys
import os
import subprocess
from pathlib import Path

try:
    from pyngrok import ngrok
except Exception:
    ngrok = None

# ===============================
# APP
# ===============================


# ===============================
# HISTÓRICO DE ENDEREÇOS (JSON)
# - Salva/Carrega um arquivo JSON na mesma pasta do app/exec
# - Mantém uma lista de endereços recentes para reuso
# ===============================

def _app_base_dir() -> str:
    """Pasta onde o app deve salvar arquivos (mesma pasta do .py ou do executável)."""
    try:
        if getattr(sys, "frozen", False):  # PyInstaller / exe
            return os.path.dirname(sys.executable)
        return os.path.dirname(os.path.abspath(__file__))
    except Exception:
        # fallback: pasta atual do processo
        return os.getcwd()


def _addr_history_path() -> str:
    return os.path.join(_app_base_dir(), "historico_enderecos.json")


def load_address_history(limit: int = 25) -> list[str]:
    """Retorna lista de endereços (mais recentes primeiro)."""
    fp = _addr_history_path()
    try:
        if not os.path.exists(fp):
            return []
        data = json.loads(Path(fp).read_text(encoding="utf-8"))
        if isinstance(data, dict):
            data = data.get("enderecos", [])
        if not isinstance(data, list):
            return []
        out = []
        for x in data:
            if isinstance(x, str):
                s = x.strip()
                if s:
                    out.append(s)
        # remove duplicados mantendo ordem
        seen = set()
        uniq = []
        for s in out:
            k = s.lower()
            if k not in seen:
                uniq.append(s)
                seen.add(k)
        return uniq[: max(1, limit)]
    except Exception:
        return []


def save_address_history(addresses: list[str]) -> None:
    fp = _addr_history_path()
    try:
        payload = {"enderecos": addresses}
        Path(fp).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        # não pode quebrar o app por falha de IO
        return


def add_address_to_history(addr: str, limit: int = 25) -> None:
    a = (addr or "").strip()
    if not a:
        return
    hist = load_address_history(limit=limit)
    # coloca no topo, removendo duplicados
    new_hist = [a] + [x for x in hist if x.strip().lower() != a.lower()]
    save_address_history(new_hist[: max(1, limit)])


# ===============================
# Sugestões de endereço (autocomplete leve)
# ===============================
ALLOWED_UF = {
    "RJ": "Rio de Janeiro",  # edite aqui para liberar outros estados
    # "SP": "São Paulo",
    # "MG": "Minas Gerais",
}

@st.cache_data(ttl=60 * 60 * 24, show_spinner=False)
def nominatim_suggest(query: str, limit: int = 6) -> list[dict]:
    """
    Busca sugestões no Nominatim (OSM).
    Retorna uma lista de dicts com display_name + address details.
    """
    q = (query or "").strip()
    if len(q) < 4:
        return []
    url = "https://nominatim.openstreetmap.org/search"
    params = {
        "format": "jsonv2",
        "q": q,
        "addressdetails": 1,
        "limit": int(limit),
        "countrycodes": "br",
    }
    headers = {"User-Agent": "SuplemexApp/1.0 (streamlit)"}
    try:
        r = requests.get(url, params=params, headers=headers, timeout=15)
        r.raise_for_status()
        data = r.json()
        if not isinstance(data, list):
            return []
        return data
    except Exception:
        return []

def _uf_allowed_for_result(res: dict) -> bool:
    if not ALLOWED_UF:
        return True
    try:
        addr = res.get("address") or {}
        state = (addr.get("state") or "").strip()
        # aceita se o estado bater com algum permitido
        allowed_states = set((v or "").strip().lower() for v in ALLOWED_UF.values())
        return (state.lower() in allowed_states) or any(a in state.lower() for a in allowed_states)
    except Exception:
        return False

def get_suggestions_filtered(query: str, limit: int = 6) -> list[str]:
    raw = nominatim_suggest(query, limit=limit)
    out: list[str] = []
    for r in raw:
        if not isinstance(r, dict):
            continue
        if not _uf_allowed_for_result(r):
            continue
        dn = (r.get("display_name") or "").strip()
        if dn:
            out.append(dn)
    # remove duplicados preservando ordem
    seen = set()
    uniq = []
    for s in out:
        k = s.lower()
        if k in seen:
            continue
        seen.add(k)
        uniq.append(s)
    return uniq

def _highlight_match(texto: str, termo: str) -> str:
    """Destaca o termo digitado em negrito (HTML), mantendo segurança."""
    t = (texto or "")
    q = (termo or "").strip()
    if not q:
        return html.escape(t)
    esc = html.escape(t)
    # destaca todas as ocorrências (case-insensitive)
    try:
        pattern = re.compile(re.escape(q), re.IGNORECASE)
        return pattern.sub(lambda m: f"<b>{html.escape(m.group(0))}</b>", esc)
    except Exception:
        return esc


def _split_suggestion(s: str) -> tuple[str, str]:
    """Divide sugestão longa em linha principal + complementar (para UI mais limpa)."""
    s = (s or "").strip()
    if not s:
        return "", ""
    parts = [p.strip() for p in s.split(",") if p.strip()]
    if len(parts) <= 3:
        return s, ""
    main = ", ".join(parts[:3])
    sub = ", ".join(parts[3:])
    return main, sub



def main():

    # ===============================
    # UI / STYLE
    # ===============================
    st.set_page_config(page_title="ProSearch", page_icon="🗺️", layout="wide", initial_sidebar_state="expanded")

    st.markdown("""
        <style>
        /* Força tema dark */
        :root{
        --bg:#0b1220;
        --card:#0f172a;
        --stroke:#1f2a44;
        --text:#e5e7eb;
        --muted:#a1a1aa;
        --brand:#60a5fa;
        }
        .stApp{ background: var(--bg) !important; color: var(--text) !important; }

        /* Sidebar */
        section[data-testid="stSidebar"]{
        background: #0a1020 !important;
        border-right: 1px solid var(--stroke) !important;
        }

        /* Containers e elementos do Streamlit */
        div[data-testid="stVerticalBlock"]{ color: var(--text) !important; }
        div[data-testid="stMarkdownContainer"]{ color: var(--text) !important; }
        div[data-testid="stMetricValue"]{ color: var(--text) !important; }

        /* Inputs */
        div[data-baseweb="input"] input,
        div[data-baseweb="textarea"] textarea{
        background: #0b1220 !important;
        color: var(--text) !important;
        border: 1px solid var(--stroke) !important;
        }
        div[data-baseweb="select"] > div{
        background: #0b1220 !important;
        color: var(--text) !important;
        border: 1px solid var(--stroke) !important;
        }

        /* Botões */
        button[kind="primary"]{
        background: #2563eb !important;
        color: white !important;
        border: 1px solid #2563eb !important;
        }
        button{
        border-radius: 12px !important;
        border: 1px solid var(--stroke) !important;
        }

        /* Dataframe (melhora contraste) */
        div[data-testid="stDataFrame"]{
        border: 1px solid var(--stroke) !important;
        border-radius: 12px !important;
        overflow: hidden !important;
        }
        </style>
        """, unsafe_allow_html=True)


    # ===============================
    # CATEGORIAS (Google type + OSM tags)
    # ===============================
    CATEGORIES_CONFIG = {
        "🛒 Mercados": {"google_type": "supermarket", "osm": ["shop=supermarket", "shop=convenience", "shop=grocery"]},
        "🏫 Escolas": {"google_type": "school", "osm": ["amenity=school", "amenity=kindergarten", "amenity=university"]},
        "🏗️ Construtoras": {"google_type": "general_contractor", "osm": ["office=construction", "craft=builder", "office=architect"]},
        "🏥 Hospitais": {"google_type": "hospital", "osm": ["amenity=hospital", "amenity=clinic", "amenity=doctors"]},
        "💊 Farmácias": {"google_type": "pharmacy", "osm": ["amenity=pharmacy"]},
        "🍽️ Restaurantes": {"google_type": "restaurant", "osm": ["amenity=restaurant", "amenity=cafe", "amenity=fast_food"]},
        "🏦 Bancos": {"google_type": "bank", "osm": ["amenity=bank", "amenity=atm"]},
        "⛽ Postos": {"google_type": "gas_station", "osm": ["amenity=fuel"]},
        "💪 Academias": {"google_type": "gym", "osm": ["leisure=fitness_centre", "leisure=sports_centre"]},
        "🏬 Shoppings": {"google_type": "shopping_mall", "osm": ["shop=mall"]},
    }

    COLOR_MAP = {
        "🛒 Mercados": "blue",
        "🏫 Escolas": "green",
        "🏗️ Construtoras": "orange",
        "🏥 Hospitais": "red",
        "💊 Farmácias": "purple",
        "🍽️ Restaurantes": "darkred",
        "🏦 Bancos": "darkblue",
        "⛽ Postos": "gray",
        "💪 Academias": "darkgreen",
        "🏬 Shoppings": "cadetblue",
    }

    # ===============================
    # STATE
    # ===============================
    if "results_df" not in st.session_state:
        st.session_state.results_df = None
    if "origin" not in st.session_state:
        st.session_state.origin = None
    if "last_error" not in st.session_state:
        st.session_state.last_error = None
    if "debug" not in st.session_state:
        st.session_state.debug = []

    def clear_all():
        st.session_state.results_df = None
        st.session_state.origin = None
        st.session_state.last_error = None
        st.session_state.debug = []

    # ===============================
    # UTILS
    # ===============================
    def haversine_km(lat1, lon1, lat2, lon2) -> float:
        R = 6371.0088
        phi1 = math.radians(float(lat1))
        phi2 = math.radians(float(lat2))
        dphi = math.radians(float(lat2) - float(lat1))
        dl = math.radians(float(lon2) - float(lon1))
        a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dl/2)**2
        return 2 * R * math.asin(math.sqrt(a))

    def _apply_radius_filter(df: pd.DataFrame, radius_km: float, eps_km: float = 0.15):
        """
        Garante que NADA fora do raio apareça na lista final.
        eps_km evita cortar itens na borda por arredondamento/pequenas diferenças.
        Retorna (df_filtrado, removidos).
        """
        if df is None or df.empty:
            return df, 0
        limit = float(radius_km) + float(eps_km)
        before = len(df)
        df2 = df[df["Distância (km)"] <= limit].copy()
        removed = before - len(df2)
        return df2, removed


    @st.cache_data(ttl=3600)
    def _geocode_one(addr: str):
        g = Nominatim(user_agent=f"prosearch_{int(time.time())}")
        try:
            l = g.geocode(addr, timeout=10)
            if l:
                return l.latitude, l.longitude, l.address
        except GeocoderTimedOut:
            return None, None, None
        except Exception:
            return None, None, None
        return None, None, None

    def geocode_robusto(addr: str):
        if not addr or not addr.strip():
            return None, None, None
        base = addr.strip()
        tries = [base]
        if "brasil" not in base.lower() and "brazil" not in base.lower():
            tries += [f"{base}, Brasil", f"{base}, Brazil"]
        seen=set(); final=[]
        for t in tries:
            if t not in seen:
                seen.add(t); final.append(t)
        for t in final:
            lat, lon, full = _geocode_one(t)
            if lat:
                return float(lat), float(lon), full
        return None, None, None

    # ===============================
    # GOOGLE PLACES (HTTP Endpoints)
    # ===============================
    @st.cache_data(ttl=900)
    def _google_nearby(lat, lon, radius_km, place_type, api_key, keyword=None):
        url = "https://maps.googleapis.com/maps/api/place/nearbysearch/json"
        params = {
            "location": f"{lat},{lon}",
            "radius": int(radius_km * 1000),
            "type": place_type,
            "language": "pt-BR",
            "region": "br",
            "key": api_key,
        }
        if keyword and keyword.strip():
            params["keyword"] = keyword.strip()

        r = requests.get(url, params=params, timeout=15)
        try:
            j = r.json()
        except Exception:
            j = {"status": "BAD_JSON", "error_message": r.text[:200]}
        return j, r.status_code

    @st.cache_data(ttl=24*3600)
    def _google_details(place_id, api_key):
        url = "https://maps.googleapis.com/maps/api/place/details/json"
        params = {
            "place_id": place_id,
            "fields": "formatted_phone_number,international_phone_number,website,rating,user_ratings_total,url,opening_hours,business_status,name,formatted_address",
            "language": "pt-BR",
            "region": "br",
            "key": api_key,
        }
        r = requests.get(url, params=params, timeout=15)
        try:
            j = r.json()
        except Exception:
            j = {"status": "BAD_JSON", "error_message": r.text[:200]}
        return j, r.status_code



def _google_details_batch(place_ids, api_key, max_workers=10):
    """Busca details em paralelo. Retorna dict: place_id -> (json, http_status)."""
    place_ids = [pid for pid in (place_ids or []) if pid]
    if not place_ids:
        return {}
    max_workers = max(2, min(int(max_workers), 16, len(place_ids)))
    out = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as ex:
        futs = {ex.submit(_google_details, pid, api_key): pid for pid in place_ids}
        for fut in concurrent.futures.as_completed(futs):
            pid = futs[fut]
            try:
                out[pid] = fut.result()
            except Exception:
                out[pid] = ({"status":"ERROR","error_message":"details failed"}, 0)
    return out

    @st.cache_data(ttl=24*3600)
    def fetch_place_photo_bytes(photo_reference: str, api_key: str, maxwidth: int = 520):
        """
        Baixa uma miniatura retangular do local via Places Photo.
        Retorna bytes da imagem (ou None).
        """
        if not photo_reference or not api_key:
            return None
        url = "https://maps.googleapis.com/maps/api/place/photo"
        params = {"maxwidth": int(maxwidth), "photo_reference": photo_reference, "key": api_key}
        try:
            r = requests.get(url, params=params, timeout=15, allow_redirects=True)
            if r.status_code == 200 and r.content:
                return r.content
        except Exception:
            return None
        return None


    def _photo_pick_from_result(r: dict):
        """Extrai (photo_reference, html_attribution) do primeiro item de photos, se existir."""
        photos = (r or {}).get("photos") or []
        if not photos:
            return "", ""
        p0 = photos[0] or {}
        pref = p0.get("photo_reference") or ""
        attrs = p0.get("html_attributions") or []
        attr = attrs[0] if attrs else ""
        return pref, attr

    def google_search_category(lat, lon, radius_km, category_name, api_key, keyword=None, debug=False):
        place_type = CATEGORIES_CONFIG[category_name]["google_type"]
        j, http = _google_nearby(lat, lon, radius_km, place_type, api_key, keyword=keyword)

        if debug:
            st.session_state.debug.append({
                "endpoint": "nearbysearch",
                "category": category_name,
                "http_status": http,
                "status": j.get("status"),
                "error_message": j.get("error_message"),
                "results_count": len(j.get("results") or []),
                "type": place_type,
                "keyword": keyword or ""
            })

        if j.get("status") != "OK":
            return []

        results = (j.get("results") or [])[:30]
        place_ids = [r.get("place_id") for r in results if r.get("place_id")]
        # details em paralelo (evita N x latência)
        details_map = _google_details_batch(place_ids, api_key, max_workers=10)

        out = []
        for r in results:
            pid = r.get("place_id")
            geom = (r.get("geometry") or {}).get("location") or {}
            plat = geom.get("lat")
            plon = geom.get("lng")
            if plat is None or plon is None:
                continue

            photo_ref, photo_attr = _photo_pick_from_result(r)

            details = {}
            if pid and pid in details_map:
                dj, dh = details_map.get(pid, ({}, 0))
                if debug:
                    res = (dj.get("result") or {}) if isinstance(dj, dict) else {}
                    st.session_state.debug.append({
                        "endpoint": "details",
                        "category": category_name,
                        "http_status": dh,
                        "status": dj.get("status") if isinstance(dj, dict) else "BAD_JSON",
                        "error_message": dj.get("error_message") if isinstance(dj, dict) else "",
                        "place_id": pid,
                        "has_phone": bool(res.get("formatted_phone_number") or res.get("international_phone_number")),
                        "has_website": bool(res.get("website"))
                    })
                if isinstance(dj, dict) and dj.get("status") == "OK":
                    details = dj.get("result") or {}

            tel = details.get("formatted_phone_number") or details.get("international_phone_number") or "-"
            site = details.get("website") or "-"
            rating = details.get("rating", r.get("rating", "-"))
            reviews = details.get("user_ratings_total", r.get("user_ratings_total", "-"))
            maps_url = details.get("url") or (f"https://www.google.com/maps/place/?q=place_id:{pid}" if pid else f"https://www.google.com/maps?q={plat},{plon}")

            status = details.get("business_status") or r.get("business_status") or "Desconhecido"
            horario = "Não disponível"
            oh = details.get("opening_hours") or {}
            if isinstance(oh, dict):
                wt = oh.get("weekday_text") or []
                if wt:
                    wd = time.localtime().tm_wday  # 0=segunda
                    if len(wt) == 7:
                        horario = wt[wd]
                    else:
                        horario = "; ".join(wt[:2])
                if oh.get("open_now") is True:
                    status = "Aberto"
                elif oh.get("open_now") is False:
                    status = "Fechado"

            out.append({
                "Nome": r.get("name") or details.get("name") or "Sem nome",
                "Categoria": category_name,
                "Endereço": details.get("formatted_address") or r.get("vicinity") or "Endereço não disponível",
                "Telefone": tel,
                "Website": site,
                "Avaliação": rating if rating is not None else "-",
                "Total Avaliações": reviews if reviews is not None else "-",
                "Status": status,
                "Horário": horario,
                "Latitude": float(plat),
                "Longitude": float(plon),
                "Fonte": "Google",
                "Maps": maps_url,
                "PhotoRef": photo_ref,
                "PhotoAttr": photo_attr
            })
        return out

    # ===============================
    # OSM/OVERPASS (cache + fallback)
    # ===============================
    OVERPASS_ENDPOINTS = [
        "https://overpass-api.de/api/interpreter",
        "https://overpass.kumi.systems/api/interpreter",
        "https://overpass.nchc.org.tw/api/interpreter",
    ]

    @st.cache_data(ttl=600)
    def _overpass_call(endpoint: str, q: str):
        r = requests.get(endpoint, params={"data": q}, timeout=45, headers={"User-Agent":"ProSearch/4.0 (Streamlit)"})
        return r.status_code, r.text

    def overpass_fallback(q: str):
        last = None
        for ep in OVERPASS_ENDPOINTS:
            try:
                status, text = _overpass_call(ep, q)
                if status != 200:
                    last = f"{ep} status={status}"
                    continue
                try:
                    return json.loads(text), ep, None
                except Exception as e:
                    last = f"{ep} json error: {e}"
                    continue
            except Exception as e:
                last = f"{ep} exception: {e}"
                continue
        return None, None, last

    def osm_search_category(lat, lon, radius_km, category_name, debug=False):
        r_m = int(radius_km * 1000)
        tags = CATEGORIES_CONFIG[category_name]["osm"]

        tag_queries=[]
        for t in tags:
            k,v = t.split("=")
            tag_queries.append(f'node["{k}"="{v}"](around:{r_m},{lat},{lon});')
            tag_queries.append(f'way["{k}"="{v}"](around:{r_m},{lat},{lon});')
            tag_queries.append(f'relation["{k}"="{v}"](around:{r_m},{lat},{lon});')

        q = f"""
        [out:json][timeout:30];
        (
          {' '.join(tag_queries)}
        );
        out center;
        """
        data, used, err = overpass_fallback(q)

        if debug:
            st.session_state.debug.append({
                "endpoint":"overpass",
                "category": category_name,
                "used": used,
                "error": err,
                "elements": (len((data or {}).get("elements") or []) if data else 0)
            })

        if data is None:
            return []

        out=[]
        for el in data.get("elements", []):
            t = el.get("tags", {}) or {}
            name = t.get("name") or t.get("operator")
            if not name:
                continue
            if "lat" in el and "lon" in el:
                plat, plon = el.get("lat"), el.get("lon")
            else:
                c = el.get("center", {}) or {}
                plat, plon = c.get("lat"), c.get("lon")
            if plat is None or plon is None:
                continue

            street = (t.get("addr:street") or "").strip()
            num = (t.get("addr:housenumber") or "").strip()
            addr = (f"{street}, {num}".strip().strip(",")).strip()
            if not addr:
                addr = "Endereço não informado"

            phone = t.get("phone") or t.get("contact:phone") or "-"
            site = t.get("website") or t.get("contact:website") or "-"

            out.append({
                "Nome": name,
                "Categoria": category_name,
                "Endereço": addr,
                "Telefone": phone if phone else "-",
                "Website": site if site else "-",
                "Avaliação": "-",
                "Total Avaliações": "-",
                "Status": "-",
                "Horário": "-",
                "Latitude": float(plat),
                "Longitude": float(plon),
                "Fonte": "OpenStreetMap",
                "Maps": f"https://www.google.com/maps?q={float(plat)},{float(plon)}",
                "PhotoRef": "",
                "PhotoAttr": ""
            })
        return out

    # ===============================
    # RENDER: CARDS (cards primeiro)
    # ===============================
    def render_cards_grid(df: pd.DataFrame, cols_per_row: int, api_key: str):
        cols_per_row = max(1, min(5, int(cols_per_row)))
        st.markdown("### Resultados")
        if df is None or df.empty:
            st.info("Sem resultados para exibir.")
            return

        rows = df.to_dict("records")
        for start in range(0, len(rows), cols_per_row):
            cols = st.columns(cols_per_row, gap="medium")
            chunk = rows[start:start+cols_per_row]
            for col, r in zip(cols, chunk):
                with col:
                    nome = str(r.get("Nome","Sem nome"))
                    cat = str(r.get("Categoria",""))
                    end = str(r.get("Endereço","Endereço não disponível"))
                    tel = str(r.get("Telefone","-"))
                    web = str(r.get("Website","-"))
                    fonte = str(r.get("Fonte","-"))
                    dist = r.get("Distância (km)", None)
                    dist_str = f"{dist:.2f} km" if isinstance(dist, (float,int)) else "-"

                    rating = r.get("Avaliação","-")
                    nrev = r.get("Total Avaliações","-")
                    maps = r.get("Maps") or f"https://www.google.com/maps?q={r.get('Latitude')},{r.get('Longitude')}"
                    directions = f"https://www.google.com/maps/dir/?api=1&destination={r.get('Latitude')},{r.get('Longitude')}"

                    # Miniatura (retangular) via Places Photo, quando disponível
                    photo_ref = str(r.get("PhotoRef","") or "").strip()
                    photo_attr = str(r.get("PhotoAttr","") or "").strip()
                    img_bytes = None
                    if photo_ref and api_key and api_key.strip():
                        img_bytes = fetch_place_photo_bytes(photo_ref, api_key.strip(), maxwidth=720)

                    if img_bytes:
                        # Imagem com altura fixa para padronizar os cards (150px)
                        try:
                            b64 = base64.b64encode(img_bytes).decode("utf-8")
                            st.markdown(
                                f"""<img src="data:image/jpeg;base64,{b64}"
                                         style="width:100%; height:150px; object-fit:cover; border-radius:16px;"
                                         loading="lazy" />""",
                                unsafe_allow_html=True,
                            )
                        except Exception:
                            st.image(img_bytes, use_container_width=True)
                    else:
                        st.markdown(
                            """<div style="width:100%; height:150px; border-radius:16px;
                                     background: rgba(255,255,255,0.06);
                                     border:1px solid rgba(255,255,255,0.08);
                                     display:flex; align-items:center; justify-content:center; opacity:0.85;">
                                   🖼️ Sem foto
                                 </div>""",
                            unsafe_allow_html=True,
                        )

                    if photo_attr:
                            # Atribuição vem em HTML (exigência do Google). Exibe discreto.
                            st.markdown(f"<div class='small'>📷 {photo_attr}</div>", unsafe_allow_html=True)
                    else:
                        # mantém um espaço visual para consistência
                        st.markdown("<div class='small'>🖼️ (sem foto)</div>", unsafe_allow_html=True)

                    st.markdown(f"""
                    <div class="card">
                      <h4>{nome}</h4>
                      <div class="meta">
                        <span class="badge">{cat}</span>
                        <span class="badge">📏 {dist_str}</span>
                        <span class="badge">🗺️ {fonte}</span>
                      </div>
                      <div class="small">📍 {end}</div>
                      <div class="small">📞 {tel if tel and tel != "-" else "(não informado)"}</div>
                      <div class="small">🌐 {web if web and web != "-" else "(não informado)"}</div>
                      <div class="small">{("⭐ " + str(rating) + " • " + str(nrev) + " avaliações") if str(rating) not in ["-","None","nan"] else ""}</div>
                    </div>
                    """, unsafe_allow_html=True)

                    b1, b2 = st.columns(2)
                    with b1:
                        st.link_button("📍 Maps", maps, use_container_width=True)
                    with b2:
                        st.link_button("🧭 Rotas", directions, use_container_width=True)

    # ===============================
    # RENDER: MAPA INTERATIVO (OSM/Folium)
    # ===============================
    def render_map_interativo(origin, df: pd.DataFrame, zoom: int):
        st.markdown("### Mapa interativo")
        if df is None or df.empty:
            st.info("Sem resultados para mapear.")
            return

        m = folium.Map(location=[origin["lat"], origin["lon"]], zoom_start=max(10, min(18, int(zoom))))
        folium.Marker([origin["lat"], origin["lon"]], tooltip="Você", icon=folium.Icon(color="red")).add_to(m)
        cl = MarkerCluster().add_to(m)

        for _, r in df.iterrows():
            latp, lonp = float(r["Latitude"]), float(r["Longitude"])
            maps = r.get("Maps") or f"https://www.google.com/maps?q={latp},{lonp}"
            directions = f"https://www.google.com/maps/dir/?api=1&destination={latp},{lonp}"
            popup = f"<b>{r.get('Nome','')}</b><br>{r.get('Categoria','')}<br>{r.get('Endereço','')}<br><a href='{maps}' target='_blank'>Maps</a> | <a href='{directions}' target='_blank'>Rotas</a>"
            folium.Marker([latp, lonp], popup=popup, tooltip=r.get("Nome",""), icon=folium.Icon(color=COLOR_MAP.get(r.get("Categoria",""), "blue"))).add_to(cl)

        st_folium(m, width="100%", height=580)

    # ===============================
    # HEADER
    # ===============================
    st.markdown("""
    <div class="header-wrap">
      <div>
        <div class="h1">Mapa Comercial por categorias🗺️</div>
        <div class="sub">Desenvolvido por Sacramento • © V3.20260201</div>
      </div>
      <div class="pill">v4 • 1 arquivo</div>
    </div>
    """, unsafe_allow_html=True)

    # ===============================
    # SIDEBAR
    # ===============================
    with st.sidebar:
        st.header("⚙️ Configurações")

                # ===============================
        # CAMPOS DE BUSCA (sem st.form)
        # - permite sugestões em tempo real no Endereço
        # - a busca só roda quando clicar em "Buscar"
        # ===============================

        default_api_key = "AIzaSyByLLGY4KW3u1kDYmh-puyMwmLsLiTq4H0"
        if not default_api_key:
            default_api_key = os.getenv("GOOGLE_API_KEY", "")
        api_key = default_api_key
        _hist = load_address_history(limit=25)
        _hist_opts = ["(digitar novo)"] + _hist
        _sel = st.selectbox("🕘 Histórico de endereços", options=_hist_opts, index=0, key="addr_hist_sel")

        # Se escolher no histórico, auto-preenche o campo
        if _sel != "(digitar novo)":
            st.session_state["addr_input"] = _sel

        # Campo principal (bonito e estável)
        st.text_input(
            "📍 Endereço",
            key="addr_input",
            placeholder="Digite (rua, número, bairro, cidade)...",
        )

                # Sugestões estilo Google (aparecem enquanto digita) — estável (sem HTML/JS)
        _q = (st.session_state.get("addr_input") or "").strip()
        st.session_state["addr_last_query"] = _q
        _sugs = get_suggestions_filtered(_q, limit=6) if len(_q) >= 4 else []
        st.session_state["addr_suggestions"] = _sugs

        def _pick_suggestion_from_radio():
            idx = st.session_state.get("addr_sug_idx")
            try:
                idx = int(idx)
            except Exception:
                idx = None
            if idx is None:
                return
            sugs = st.session_state.get("addr_suggestions") or []
            if 0 <= idx < len(sugs):
                s = sugs[idx]
                st.session_state["addr_input"] = s
                st.session_state["addr_hist_sel"] = "(digitar novo)"

        if _sugs:
            st.markdown("**📌 Sugestões:**")
            labels = []
            for s in _sugs:
                main_txt, sub_txt = _split_suggestion(s)
                labels.append(f"{main_txt}\n{sub_txt}" if sub_txt else main_txt)

            st.radio(
                "Sugestões de endereço",
                options=list(range(len(labels))),
                format_func=lambda i: labels[i],
                key="addr_sug_idx",
                label_visibility="collapsed",
                on_change=_pick_suggestion_from_radio,
            )
            st.caption("Dica: ao clicar em Buscar, se houver sugestões, a 1ª é usada automaticamente.")

        radius = st.slider("📏 Raio (km)", 0.5, 10.0, 3.0, 0.5)


        categories = st.multiselect(
            "🏷️ Categorias",
            options=list(CATEGORIES_CONFIG.keys()),
            default=["🏫 Escolas", "💊 Farmácias", "🍽️ Restaurantes"]
        )

        keyword = st.text_input("🔎 Palavra-chave (opcional)", value="", placeholder="Ex: Zaccaria, 24h, delivery...")
        prefer_google = True

        cols_per_row = st.slider("🧩 Cards por linha", 1, 5, 5, 1)
        top_n = st.slider("📌 Máx. resultados exibidos", 10, 200, 60, 10)
        zoom = st.slider("🗺️ Zoom do mapa", 10, 18, 14, 1)

        debug = st.checkbox("🧪 Debug (ver status/erros)", value=False)

        go = st.button("🚀 Buscar", type="primary", use_container_width=True)

        st.button("🧹 Limpar", use_container_width=True, on_click=clear_all)

    # ===============================
    # BUSCA
    # ===============================
    if go:
        st.session_state.last_error = None
        st.session_state.results_df = None
        st.session_state.origin = None
        st.session_state.debug = []

        # Endereço final:
        # - Clique numa sugestão => já preencheu addr_input
        # - ENTER no form => usa automaticamente a 1ª sugestão (se existir)
        _addr_raw = (st.session_state.get("addr_input") or "").strip()
        _sugs_now = st.session_state.get("addr_suggestions") or []
        _last_q = (st.session_state.get("addr_last_query") or "").strip()

        if _addr_raw and _sugs_now and (_addr_raw == _last_q) and (_addr_raw != _sugs_now[0]):
            addr = _sugs_now[0]
            st.session_state["addr_input"] = addr  # auto-preenche visualmente
        else:
            addr = _addr_raw

        if not addr:
            st.session_state.last_error = "Informe um endereço válido."
            st.error(st.session_state.last_error)
            st.stop()

        # salva no histórico (não interfere na lógi
        add_address_to_history(addr, limit=25)

        lat, lon, full = geocode_robusto(addr)
        if not lat:
            st.session_state.last_error = "❌ Endereço não encontrado. Tente incluir rua, número, cidade e UF."
        elif not categories:
            st.session_state.last_error = "⚠️ Selecione ao menos 1 categoria."
        else:
            all_rows = []

            # Google (se preferir e tiver key)
            if prefer_google and api_key.strip():
                for cat in categories:
                    all_rows.extend(google_search_category(lat, lon, radius, cat, api_key.strip(), keyword=keyword, debug=debug))
                if not all_rows and debug:
                    st.session_state.debug.append({"note":"Google não retornou resultados; fallback OSM."})

            # OSM fallback
            if not all_rows:
                for cat in categories:
                    all_rows.extend(osm_search_category(lat, lon, radius, cat, debug=debug))

            if not all_rows:
                st.session_state.last_error = "⚠️ Nenhum resultado encontrado. (Se for Google e estiver tudo 'REQUEST_DENIED', verifique billing/restrições/Places API)."
            else:
                df = pd.DataFrame(all_rows)

                df["Distância (km)"] = df.apply(lambda r: haversine_km(lat, lon, r["Latitude"], r["Longitude"]), axis=1).astype(float).round(2)

                # GARANTIA: filtra por raio (evita itens fora do limite)
                df, removed_outside = _apply_radius_filter(df, radius_km=radius, eps_km=0.15)

                df["__key"] = df["Nome"].astype(str) + "_" + df["Latitude"].astype(str) + "_" + df["Longitude"].astype(str)
                df = df.drop_duplicates(subset=["__key"]).drop(columns=["__key"]).reset_index(drop=True)

                df = df.sort_values("Distância (km)", ascending=True).reset_index(drop=True)
                df = df.head(int(top_n)).reset_index(drop=True)

                st.session_state.results_df = df
                st.session_state.origin = {"lat": lat, "lon": lon, "full": full, "radius": radius, "categories": categories, "keyword": keyword, "removed_outside": int(removed_outside) if "removed_outside" in locals() else 0}

    # ===============================
    # RENDER (persistente)
    # ===============================
    if st.session_state.last_error:
        st.error(st.session_state.last_error)

    if st.session_state.origin:
        o = st.session_state.origin
        st.success(f"📍 {o['full']}")
        st.caption(f"Categorias: {', '.join(o['categories'])} • Raio: {o['radius']} km" + (f" • Keyword: {o['keyword']}" if o.get("keyword") else ""))

    if st.session_state.results_df is not None:
        df = st.session_state.results_df
        o = st.session_state.origin

        # KPIs
        c1, c2, c3, c4 = st.columns(4)
        with c1:
            st.markdown(f'<div class="kpi"><div class="label">Total</div><div class="value">{len(df)}</div></div>', unsafe_allow_html=True)
        with c2:
            n_phone = int((df["Telefone"].astype(str).str.strip() != "-").sum()) if "Telefone" in df.columns else 0
            st.markdown(f'<div class="kpi"><div class="label">Com telefone</div><div class="value">{n_phone}</div></div>', unsafe_allow_html=True)
        with c3:
            n_site = int((df["Website"].astype(str).str.strip() != "-").sum()) if "Website" in df.columns else 0
            st.markdown(f'<div class="kpi"><div class="label">Com website</div><div class="value">{n_site}</div></div>', unsafe_allow_html=True)
        with c4:
            near = float(df["Distância (km)"].min()) if not df.empty else 0.0
            st.markdown(f'<div class="kpi"><div class="label">Mais perto</div><div class="value">{near:.2f} km</div></div>', unsafe_allow_html=True)

        st.markdown('<div class="hr"></div>', unsafe_allow_html=True)

        # CARDS PRIMEIRO
        render_cards_grid(df, cols_per_row=cols_per_row, api_key=api_key)

        st.markdown('<div class="hr"></div>', unsafe_allow_html=True)

        tab1, tab2 = st.tabs(["🗺️ Mapa", "📊 Tabela"])
        with tab1:
            render_map_interativo(o, df, zoom=zoom)
        with tab2:
            show_cols = ["Nome","Categoria","Distância (km)","Endereço","Telefone","Website","Avaliação","Total Avaliações","Fonte","Maps"]
            show_cols = [c for c in show_cols if c in df.columns]
            st.dataframe(df[show_cols], use_container_width=True, hide_index=True)
            csv = df.to_csv(index=False, encoding="utf-8-sig")
            st.download_button("📥 Baixar CSV", csv, "busca.csv", "text/csv")

        if debug and st.session_state.debug:
            with st.expander("🧪 Debug (status/erros do Google/Overpass)", expanded=False):
                st.json(st.session_state.debug)

    else:
        if not st.session_state.last_error:
            st.info("👈 Configure endereço, raio e categorias e clique em **Buscar**. Os resultados ficam na tela.")

    st.caption("Mapa Comercial por categorias")


    if __name__ == "__main__":
        import sys
    import subprocess
    try:
        from pyngrok import ngrok
    except Exception:
        ngrok = None

    
        # --- CONFIGURAÇÃO NGROK ---
        # Mude para True se quiser abrir o link para a internet automaticamente
        MODO_ONLINE = False 
        NGROK_AUTH_TOKEN = "35RkSd48qM6Ht5KFkZKyoSclZzr_2jn9As8o6P14JCy7hGrzh"
        # --------------------------

        # Verifica se o script está rodando dentro do contexto do Streamlit
        try:
            from streamlit.runtime.scriptrunner import get_script_run_ctx
            if get_script_run_ctx() is not None:
                main()
                sys.exit(0)
        except ImportError:
            pass

        # Se chegou aqui, não está no Streamlit. Vamos configurar o lançamento.
        print("="*50)
        print("🚀 INICIANDO DASHBOARD SUPLEMEX")
        print("="*50)

        public_url = None
        if MODO_ONLINE:
            if ngrok is None:
                print("❌ Erro: Biblioteca 'pyngrok' não instalada.")
                print("👉 Execute: pip install pyngrok")
            elif NGROK_AUTH_TOKEN == "SEU_TOKEN_AQUI":
                print("⚠️ Aviso: Você precisa configurar seu NGROK_AUTH_TOKEN no código.")
                print("👉 Obtenha um em: https://dashboard.ngrok.com/get-started/your-authtoken")
            else:
                try:
                    ngrok.set_auth_token(NGROK_AUTH_TOKEN)
                    # Porta padrão do Streamlit é 8502
                    tunnel = ngrok.connect(8502)
                    public_url = tunnel.public_url
                    print(f"\n✅ DASHBOARD ONLINE!")
                    print(f"🔗 Link para compartilhar: {public_url}\n")
                except Exception as e:
                    print(f"❌ Erro ao iniciar Ngrok: {e}")

        try:
            # Comando para rodar o streamlit
            # Usamos sys.argv[0] para pegar o próprio arquivo
            cmd = [sys.executable, "-m", "streamlit", "run", sys.argv[0], "--server.port", "8502"]
        
            if not MODO_ONLINE:
                print("\n🏠 Rodando apenas em LOCALHOST (Apenas sua rede local).")
                print("🔗 Acesse em: http://localhost:8502\n")
            
            subprocess.run(cmd)
        except KeyboardInterrupt:
            print("\n👋 Encerrando Dashboard...")
        except Exception as e:
            print(f"❌ Erro ao iniciar Streamlit: {e}")
        finally:
            if public_url:
                ngrok.disconnect(public_url)
                ngrok.kill()


# ===============================
# LAUNCHER (Streamlit + Ngrok)
# ===============================

if __name__ == "__main__":
    # Rodar com "python arquivo.py" no VS Code:
    # - se NÃO estiver dentro do runtime do Streamlit, iniciamos "streamlit run" automaticamente
    # - se JÁ estiver no Streamlit, apenas chama main()
    import sys
    import subprocess

    def _is_streamlit_runtime() -> bool:
        try:
            from streamlit.runtime.scriptrunner import get_script_run_ctx  # type: ignore
            return get_script_run_ctx() is not None
        except Exception:
            return False

    if _is_streamlit_runtime():
        main()
    else:
        # Inicia o Streamlit e encerra este processo
        cmd = [sys.executable, "-m", "streamlit", "run", os.path.abspath(__file__)]
        subprocess.run(cmd)
        raise SystemExit
