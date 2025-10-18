# main.py
import time, re, os, sys, yaml, pandas as pd, requests
from urllib.parse import quote
from bs4 import BeautifulSoup

# =========================
# Réglages anti-crawl infini
# =========================
# Limite de sécurité : nb max de pages DÉTAIL (annonces) traitées par SOURCE (CessionPME, etc.)
MAX_PAGES_PER_SOURCE = 50
# (Optionnel) Limite par page de recherche si jamais un site liste des centaines de liens
MAX_LINKS_PER_SEARCH = 200

def log(*a):  # logs flushés (visibles en direct dans GitHub Actions)
    print(*a, file=sys.stdout, flush=True)

# -------------------------
# Réseau / Fetch helpers
# -------------------------
def fetch(url: str, ua: str, timeout: int = 25) -> str | None:
    try:
        r = requests.get(
            url,
            headers={
                "User-Agent": ua,
                "Accept": "text/html,application/xhtml+xml;q=0.9,*/*;q=0.8",
            },
            timeout=timeout,
            allow_redirects=True,
        )
        log("GET", r.status_code, url[:200])
        if r.status_code != 200:
            return None
        # Certaines pages compressées/encodées bizarrement
        r.encoding = r.apparent_encoding or r.encoding
        return r.text
    except Exception as e:
        log("ERR fetch:", e, "URL:", url[:200])
        return None

def discover_links(html: str, base_domain: str):
    """Repère des liens d’annonces plausibles sur une page de résultats."""
    try:
        soup = BeautifulSoup(html, "html.parser")
        links = []
        for a in soup.find_all("a", href=True):
            href = a["href"]
            # Absolutiser les liens relatifs
            if href.startswith("/"):
                href = f"https://{base_domain}{href}"
            # Filtrer le domaine et heuristique de détail (fiche/annonce/id/long nb)
            if base_domain in href and re.search(r"(annonce|fiche|ref|id=|\d{5,})", href, re.IGNORECASE):
                clean = href.split("#")[0]
                links.append(clean)
        # Dédupe en conservant l’ordre
        seen, out = set(), []
        for u in links[:MAX_LINKS_PER_SEARCH]:
            if u not in seen:
                seen.add(u); out.append(u)
        return out
    except Exception as e:
        log("ERR discover_links:", e)
        return []

# -------------------------
# Parsing texte & numéraires
# -------------------------
def money(text: str):
    if not text: 
        return None
    t = text.replace("\xa0", " ")
    m = re.search(r"([\d\s][\d\s\.,]{2,})\s*€", t)
    if not m: 
        return None
    val = (
        m.group(1)
        .replace(" ", "")
        .replace("\u202f", "")
        .replace(".", "")
        .replace(",", ".")
    )
    try:
        return float(val)
    except:
        return None

def parse_generic(html: str) -> dict:
    """Extraction heuristique générique (sans sélecteurs CSS spécifiques)."""
    try:
        soup = BeautifulSoup(html, "html.parser")
        text = re.sub(r"\s+", " ", soup.get_text(" ", strip=True))

        def find_eur(label, span=80):
            m = re.search(rf"{label}\s*[:\-]?\s*.{{0,{span}}}€", text, re.IGNORECASE)
            return money(m.group(0)) if m else None

        prix    = find_eur(r"Prix(?: de vente)?|Prix net vendeur|Price") or money(text)
        loyer   = find_eur(r"Loyer annuel(?: HT)?|Revenu locatif|Loyers? nets?", 90)
        charges = find_eur(r"Charges(?: locatives)?", 60)
        taxe    = find_eur(r"Taxe fonci(?:e|è)re|TF", 60)

        m_rend  = re.search(r"Rendement\s*[:\-]?\s*(\d+(?:[.,]\d+)?)\s*%", text, re.IGNORECASE)
        rendement = float(m_rend.group(1).replace(",", ".")) if m_rend else None

        m_bail = re.search(r"(Bail|Type de bail|Échéance bail)\s*[:\-]?\s*([A-Za-z0-9\/\-\.,\s]{3,80})", text, re.IGNORECASE)
        bail = m_bail.group(0) if m_bail else None

        m_loc = re.search(r"(Locataire|Enseigne|Occupant)\s*[:\-]?\s*([A-Za-z0-9\-\.,\s]{2,80})", text, re.IGNORECASE)
        locataire = m_loc.group(0) if m_loc else None

        m_act = re.search(r"(Restauration|Pharmacie|Boulangerie|Banque|Sant[ée]|Supermarch[ée]|Retail)", text, re.IGNORECASE)
        activite = m_act.group(1) if m_act else None

        return {
            "prix": prix,
            "loyer": loyer,
            "charges": charges,
            "taxe": taxe,
            "rendement_annonce": rendement,
            "bail": bail,
            "locataire": locataire,
            "activite": activite,
            "raw": text,
        }
    except Exception as e:
        log("ERR parse_generic:", e)
        return {
            "prix": None, "loyer": None, "charges": None, "taxe": None,
            "rendement_annonce": None, "bail": None, "locataire": None,
            "activite": None, "raw": ""
        }

# -------------------------
# Scoring emplacement
# -------------------------
AXES_PRIME = {
  "Paris":["Champs-Élysées","Rue de Rivoli","Boulevard Haussmann","Rue Saint-Honoré","Avenue Montaigne"],
  "Lyon":["Rue de la République","Rue Victor Hugo","Rue Mercière"],
  "Marseille":["Rue Saint-Ferréol","La Canebière"],
  "Bordeaux":["Rue Sainte-Catherine","Cours de l'Intendance"],
  "Toulouse":["Rue d'Alsace-Lorraine","Rue Saint-Rome"],
  "Lille":["Rue de Béthune","Rue Neuve"],
  "Nice":["Avenue Jean Médecin","Rue Masséna"],
  "Nantes":["Rue Crébillon","Rue du Calvaire"],
  "Montpellier":["Rue de la Loge","Comédie"],
  "Rennes":["Rue Le Bastard","Rue d'Antrain"],
  "Strasbourg":["Grand'Rue","Rue des Grandes Arcades"],
  "Grenoble":["Rue Félix Poulat","Rue de Bonne"],
  "Dijon":["Rue de la Liberté"],
  "Angers":["Rue Lenepveu"],
  "Reims":["Rue de Vesle"],
  "Tours":["Rue Nationale"],
  "Clermont-Ferrand":["Rue du 11 Novembre"],
  "Saint-Étienne":["Rue des Martyrs de Vingré"],
  "Nîmes":["Rue de l'Aspic"],
  "Avignon":["Rue de la République"],
  "Béziers":["Allées Paul Riquet"],
  "Perpignan":["Rue Maréchal Foch"],
  "Toulon":["Rue d'Alger"],
  "Le Havre":["Rue de Paris"],
  "Rouen":["Rue du Gros-Horloge"],
  "Orléans":["Rue de la République"],
  "Metz":["Rue Serpenoise"],
  "Nancy":["Rue Saint-Jean"],
  "Caen":["Rue Saint-Pierre"],
  "Poitiers":["Rue Magenta"],
  "Limoges":["Rue de la Boucherie"],
  "Annecy":["Rue Carnot"],
  "Aix-en-Provence":["Cours Mirabeau"],
  "Bayonne":["Rue d'Espagne"],
  "Pau":["Rue Joffre"],
  "La Rochelle":["Rue du Palais"],
  "Valence":["Rue Victor Hugo"],
  "Chambéry":["Rue de Boigne"],
  "Mulhouse":["Rue du Sauvage"],
  "Brest":["Rue de Siam"],
  "Quimper":["Rue Kéréon"],
  "Vannes":["Rue Saint-Vincent"],
  "Amiens":["Rue des Trois Cailloux"],
  "Chartres":["Rue du Bois Merrain"]
}

def detect_city(raw: str, cities: list[str], fallback: str) -> str:
    try:
        for c in cities:
            if re.search(rf"\b{re.escape(c)}\b", raw, re.IGNORECASE):
                return c
        return fallback or ""
    except Exception:
        return fallback or ""

def score_emplacement(city: str, raw: str, axes_map: dict) -> str:
    axes = axes_map.get(city, [])
    for ax in axes:
        if ax and ax.lower() in raw.lower():
            return "N°1"
    if re.search(r"(centre[- ]ville|angle|rue piétonne|fort flux|zone prime|coeur de ville)", raw, re.IGNORECASE):
        return "1bis"
    return "2"

# -------------------------
# Run principal
# -------------------------
def run():
    # Charger la config
    with open("config.yaml", "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    ua       = cfg.get("user_agent")
    min_y    = float(cfg.get("min_yield_pct", 8.0))
    throttle = float(cfg.get("throttle_seconds", 1.2))
    timeout  = int(cfg.get("timeout_seconds", 25))
    pmin     = float(cfg.get("price_min_eur", 0))
    pmax     = float(cfg.get("price_max_eur", 1e12))
    cities   = cfg.get("cities", [])
    queries  = cfg.get("queries", [])
    sources  = cfg.get("sources", [])

    rows = []

    # Boucles sources → patterns → villes → mots-clés
    for src in sources:
        name, domain = src["name"], src["domain"]
        total_detail_pages = 0  # compteur par source
        for pattern in src.get("search_urls", []):
            for city in (cities or [""]):
                for q in (queries or [""]):
                    if total_detail_pages >= MAX_PAGES_PER_SOURCE:
                        log(f"⏹ Limite atteinte ({MAX_PAGES_PER_SOURCE}) pour {name}")
                        break

                    url = pattern.format(city=quote(city), query=quote(q))
                    html = fetch(url, ua, timeout); time.sleep(throttle)
                    if not html:
                        continue

                    links = discover_links(html, domain)
                    log(f"[{name}] {city or '-'} / {q or '-'} → {len(links)} liens (max {MAX_LINKS_PER_SEARCH})")

                    for i, link in enumerate(links):
                        if total_detail_pages >= MAX_PAGES_PER_SOURCE:
                            log(f"⏹ Limite atteinte ({MAX_PAGES_PER_SOURCE}) pour {name}")
                            break

                        det = fetch(link, ua, timeout); time.sleep(throttle)
                        if not det:
                            continue

                        f = parse_generic(det)
                        prix, loyer, charges, taxe = f["prix"], f["loyer"], f["charges"], f["taxe"]

                        # Ticket 1–3 M€ (depuis config)
                        if prix is not None and (prix < pmin or prix > pmax):
                            continue

                        # Rendements
                        brut = round((loyer/prix)*100, 2) if prix and loyer and prix > 0 else None
                        net  = round(((loyer - (charges or 0) - (taxe or 0)) / prix)*100, 2) if prix and loyer and prix > 0 else None

                        # Filtre rendement
                        if ((brut or 0) < min_y) and ((net or 0) < min_y):
                            continue

                        detected = detect_city(f["raw"] or "", cities, city)
                        empl = score_emplacement(detected, f["raw"] or "", AXES_PRIME) if detected else ""

                        rows.append({
                            "Source": name, "Domaine": domain, "URL": link,
                            "Ville (détectée)": detected,
                            "Prix de vente (€)": prix, "Loyer annuel HT-HC (€)": loyer,
                            "Charges locatives (€)": charges, "Taxe foncière (€)": taxe,
                            "Rendement brut (%)": brut, "Rendement net (%)": net,
                            "Bail": f["bail"], "Locataire": f["locataire"],
                            "Activité": f["activite"], "Emplacement (score)": empl
                        })

                        total_detail_pages += 1

    # DataFrame + sortie CSV
    if rows:
        df = pd.DataFrame(rows).drop_duplicates(subset=["URL"])
        # Tri par emplacement puis rendement
        order = {"N°1": 0, "1bis": 1, "2": 2, "": 3}
        df["__ord"] = df["Emplacement (score)"].map(order).fillna(3)
        df.sort_values(by=["__ord","Rendement net (%)","Rendement brut (%)"], ascending=[True, False, False], inplace=True)
        df.drop(columns=["__ord"], inplace=True)
    else:
        df = pd.DataFrame(columns=[
            "Source","Domaine","URL","Ville (détectée)","Prix de vente (€)","Loyer annuel HT-HC (€)",
            "Charges locatives (€)","Taxe foncière (€)","Rendement brut (%)","Rendement net (%)",
            "Bail","Locataire","Activité","Emplacement (score)"
        ])

    os.makedirs("output", exist_ok=True)
    out = "output/filter_ge_{:.0f}_1to3M.csv".format(min_y)
    df.to_csv(out, index=False, encoding="utf-8")
    log("Saved", out, "Rows:", len(df))

if __name__ == "__main__":
    try:
        run()
    except Exception as e:
        # On log l'erreur pour debug mais on laisse le job terminer proprement
        print("FATAL:", e, file=sys.stderr)
        # Si tu veux que le job soit marqué en échec, dé-commente la ligne suivante :
        # sys.exit(1)
