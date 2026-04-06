# Streamlit Finance Dashboard

Application Streamlit de suivi de marche avec :

- watchlist persistante
- comparaison d'actions, indices, ETF et cryptos
- graphes de prix et de performance
- actualites marche par actif
- actualites generalistes via flux RSS
- configuration de deploiement Nginx + systemd

## Fonctionnalites

- Recherche par nom d'entreprise, ticker ou indice
- Comparaison multi-actifs sur une periode configurable
- Affichage des prix et des performances en pourcentage
- Watchlist sauvegardee localement
- Prise en charge des cryptos via Yahoo Finance et CoinGecko
- Onglet d'actualites marche
- Onglet d'infos generales avec sources affichees
- Option pour masquer visuellement les trous de fermeture de marche

## Structure

```text
.
├── app.py
├── requirements.txt
├── .streamlit/config.toml
├── deploy/
│   ├── rafikm.duckdns.org.conf
│   └── streamlit-app.service
└── README.md
```

## Installation locale

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
streamlit run app.py
```

Application locale par defaut :

```text
http://localhost:8501
```

## Deploiement

Le projet inclut deux fichiers utilitaires dans `deploy/` :

- `deploy/streamlit-app.service` : service `systemd`
- `deploy/rafikm.duckdns.org.conf` : reverse proxy Nginx

Exemple d'installation :

```bash
sudo install -D -m 644 deploy/streamlit-app.service /etc/systemd/system/streamlit-app.service
sudo install -D -m 644 deploy/rafikm.duckdns.org.conf /etc/nginx/sites-available/rafikm.duckdns.org.conf
sudo ln -sf /etc/nginx/sites-available/rafikm.duckdns.org.conf /etc/nginx/sites-enabled/rafikm.duckdns.org.conf
sudo systemctl daemon-reload
sudo systemctl enable --now streamlit-app
sudo systemctl enable --now nginx
```

## Sources de donnees

- Societes cotees US : Nasdaq Trader
- Cryptos : CoinGecko
- Prix et news marche : Yahoo Finance via `yfinance`
- Infos generales : flux RSS de medias generalistes

## Fichiers locaux non versionnes

Les elements suivants restent hors Git :

- `.env`
- `.venv/`
- logs Streamlit
- uploads
- donnees de cache dans `data/`

Le fichier `.gitignore` est configure pour eviter de pousser des secrets ou des fichiers temporaires.
