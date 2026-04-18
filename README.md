# Streamlit Finance Dashboard

Application Streamlit de suivi de marche avec :

- authentification locale et gestion d'utilisateurs
- comparaison d'actions, indices, ETF et cryptos
- portefeuille simule simple par montant investi, avec suivi PnL, courbe de performance, allocations et benchmarks
- page marche du jour avec indices, VIX, taux, matieres premieres, crypto et news
- selection quantitative de mid-cap a fort potentiel
- fiche entreprise enrichie avec ratios, lecture rapide et comparaison de pairs
- graphes de prix et de performance
- actualites marche par actif
- actualites generalistes via flux RSS
- configuration de deploiement systemd

## Fonctionnalites

- Creation du premier administrateur par email et mot de passe
- Connexion utilisateur avec session par cookie local, refresh automatique et deconnexion
- Roles `admin` et `user`
- Page admin pour creer, bloquer, reactiver, deverrouiller et modifier les utilisateurs
- Reinitialisation de mot de passe avec renouvellement obligatoire
- Hachage de mots de passe PBKDF2-SHA256 avec sel unique
- Blocage temporaire apres echecs de connexion repetes
- Journal local des evenements de securite
- Recherche par nom d'entreprise, ticker ou indice
- Recommandations mid-cap basees sur momentum, liquidite et tendance technique
- Portefeuille par utilisateur : achat virtuel par montant investi, calcul automatique de la quantite, PnL latent, courbe de performance, contribution et allocation
- Benchmark de portefeuille contre CAC 40, S&P 500 et MSCI World
- Page `Marche du jour` pour un briefing rapide des principaux actifs de contexte
- Fiche entreprise avec PER, PS, EV/EBITDA, marges, croissance, dette, cash, FCF, dividende, resultats et objectifs analystes quand disponibles
- Comparaison multi-actifs sur une periode configurable
- Affichage des prix et des performances en pourcentage
- Prise en charge des cryptos via Yahoo Finance et CoinGecko
- Onglet d'actualites marche
- Onglet d'infos generales en premier dans `Actualites`, avec plus de medias et sources affichees
- Generation admin d'un script de podcast briefing et d'un audio MP3 via OpenAI TTS
- Envoi manuel et quotidien d'un recap infos par email
- Option pour masquer visuellement les trous de fermeture de marche

## Structure

```text
.
├── app.py
├── requirements.txt
├── .streamlit/config.toml
├── deploy/
│   ├── daily-news-email.service
│   ├── daily-news-email.timer
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

## Premier lancement et comptes

Au premier lancement, l'application affiche un formulaire pour creer le premier administrateur avec une adresse email, un nom affiche et un mot de passe. Ensuite, l'acces passe par l'ecran de connexion email + mot de passe.

Apres connexion, l'application cree une session renouvelee par cookie `rafik_refresh` valable 30 jours. Le refresh token est stocke sous forme hachee dans la base locale et n'est plus place dans l'URL. Les anciens liens contenant `access`, `refresh` ou `session` sont acceptes uniquement pour migrer/nettoyer l'adresse, puis ces parametres sont retires. Le bouton de deconnexion invalide le refresh token.

Les comptes sont stockes localement dans :

```text
data/users.sqlite3
data/jwt_secret.key
```

Ces fichiers sont ignores par Git. Les mots de passe et refresh tokens ne sont jamais stockes en clair.

## Recap infos par email

L'application peut envoyer un recap des infos du jour par SMTP depuis l'onglet `Actualites`, section `Infos generales`. Le recap commence par les informations bourse/marche et termine par les actualites generales. Le bouton manuel est reserve au role `admin`. L'admin peut utiliser les destinataires par defaut ou taper d'autres adresses pour un envoi ponctuel. Si `.env` n'est pas configure, l'admin peut renseigner le compte SMTP et le mot de passe d'application directement dans le formulaire d'envoi manuel.

Configure les identifiants dans `.env` :

```env
NEWS_SMTP_HOST=smtp.gmail.com
NEWS_SMTP_PORT=465
NEWS_SMTP_USER=ton-adresse@gmail.com
NEWS_SMTP_PASSWORD=mot-de-passe-application
NEWS_EMAIL_FROM=ton-adresse@gmail.com
NEWS_EMAIL_TO=destinataire1@example.com,destinataire2@example.com
```

Avec Gmail, utilise un mot de passe d'application, pas le mot de passe principal du compte.

## Podcast briefing audio

Dans `Actualites` > `Infos generales`, les administrateurs peuvent generer un script de briefing audio generaliste a partir des rubriques d'information selectionnees. Le script commence directement par le vif du sujet, creuse le sujet principal du jour, puis passe aux autres informations importantes. Les marches financiers ne sont pas mis en avant par defaut : ils restent un sujet comme les autres, sauf si l'option de contexte marche dedie est activee. Si `OPENAI_API_KEY` est configuree, l'application peut aussi generer un fichier MP3 avec le modele TTS configure. Sans cle OpenAI, l'application peut utiliser `espeak-ng` local si le paquet est installe, avec une voix moins naturelle mais totalement offline.

```env
OPENAI_API_KEY=sk-...
OPENAI_SCRIPT_MODEL=gpt-4o-mini
```

Fallback local :

```bash
sudo apt-get install -y espeak-ng
```

Les fichiers generes sont sauvegardes localement dans :

```text
data/briefings/
```

## Deploiement

Le projet inclut des fichiers utilitaires dans `deploy/` :

- `deploy/streamlit-app.service` : service `systemd` qui lance Streamlit directement sur le port `80`
- `deploy/daily-news-email.service` : envoi ponctuel du recap infos
- `deploy/daily-news-email.timer` : planification quotidienne a 08:00

Exemple d'installation :

```bash
sudo install -D -m 644 deploy/streamlit-app.service /etc/systemd/system/streamlit-app.service
sudo install -D -m 644 deploy/daily-news-email.service /etc/systemd/system/daily-news-email.service
sudo install -D -m 644 deploy/daily-news-email.timer /etc/systemd/system/daily-news-email.timer
sudo systemctl daemon-reload
sudo systemctl enable --now streamlit-app
sudo systemctl enable --now daily-news-email.timer
```

## Sources de donnees

- Societes cotees US : Nasdaq Trader
- Cryptos : CoinGecko
- Prix et news marche : Yahoo Finance via `yfinance`
- Infos generales : flux RSS publics de medias generalistes et thematiques francophones

## Documentation projet

- `docs/ameliorations.md` : etat actuel de l'application, limites connues et propositions d'amelioration priorisees

## Fichiers locaux non versionnes

Les elements suivants restent hors Git :

- `.env`
- `.venv/`
- logs Streamlit
- uploads
- base utilisateurs locale
- donnees de cache dans `data/`

Le fichier `.gitignore` est configure pour eviter de pousser des secrets ou des fichiers temporaires.
