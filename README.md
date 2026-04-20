# Streamlit Finance Dashboard

Application Streamlit de suivi de marche, de portefeuille simule, d'analyse d'actions et de briefing d'actualites. Elle combine donnees Yahoo Finance, annuaires Nasdaq/CoinGecko, flux RSS francophones, scoring quantitatif en worker et generation de podcast avec OpenAI.

L'application sert surtout a :

- suivre les principaux marches, actions, indices, ETF et cryptos ;
- comparer plusieurs actifs sur des horizons configurables ;
- maintenir un portefeuille virtuel par montant investi ;
- detecter des actions US interessantes via un score technique et momentum ;
- consulter une fiche entreprise enrichie ;
- lire des actualites marche et generalistes ;
- generer un script de podcast et un audio MP3 de briefing quotidien ;
- envoyer automatiquement ce briefing par email.

Les idees marche et les scores affiches ne sont pas des conseils financiers.

## Pages Principales

### Comparateur

La page `Comparateur` permet de selectionner plusieurs actifs et de comparer leurs trajectoires :

- actions US issues des fichiers Nasdaq Trader ;
- grandes valeurs europeennes ajoutees manuellement ;
- indices majeurs comme CAC 40, S&P 500, Nasdaq, DAX, MSCI World proxy ;
- cryptos chargees via CoinGecko ;
- courbes de prix et de performance ;
- periode configurable : 1 jour, 5 jours, 1 mois, 3 mois, 6 mois, 1 an, 2 ans, 5 ans, maximum ;
- donnees intraday quand l'horizon est court ;
- lissage visuel des fermetures de marche pour eviter les ruptures de courbe.

### Portefeuille

La page `Portefeuille` gere un portefeuille virtuel simple.

Fonctionnalites :

- achat virtuel par montant investi ;
- calcul automatique de la quantite achetee a partir du prix de reference ;
- prix d'achat recupere a la date choisie quand disponible ;
- suivi de la valeur actuelle ;
- PnL latent en montant et en pourcentage ;
- tableau detaille des lignes individuelles ;
- graphe d'evolution de la valeur du portefeuille ;
- allocation par type, secteur ou devise ;
- contribution de chaque position au PnL ;
- benchmark contre CAC 40, S&P 500 et MSCI World proxy ;
- suppression de lignes.

La recuperation des cours du portefeuille utilise plusieurs niveaux de fallback :

1. prix intraday recents via Yahoo Finance ;
2. historique journalier si l'intraday est incomplet ;
3. `fast_info` Yahoo Finance si l'historique ne suffit pas.

Cela evite qu'une ligne soit marquee "sans cotation" quand Yahoo ne renvoie qu'une partie des tickers.

### Marche Du Jour

La page `Marche du jour` donne une vision rapide de contexte :

- indices actions ;
- volatilite avec VIX ;
- taux US 10 ans ;
- petrole WTI ;
- or ;
- Bitcoin ;
- EUR/USD ;
- variation 1 jour et 1 mois quand disponible ;
- actualites marche associees.

### Analyse

La page `Analyse` regroupe trois blocs :

- `A la une` : meilleurs gagnants/perdants de grandes capitalisations US ;
- `Valeurs a fort potentiel` : recommandations d'actions issues du worker ;
- `Fiche entreprise` : analyse detaillee d'une societe.

La fiche entreprise affiche notamment :

- prix, variation recente et sentiment marche ;
- ratios de valorisation : PER, Price/Sales, EV/EBITDA ;
- marges, croissance, free cash-flow, dette, cash ;
- dividende, beta, objectifs analystes quand disponibles ;
- lecture qualitative : valorisation, croissance, risque ;
- comparaison de pairs ;
- actualites recentes de l'actif.

### Actualites

La page `Actualites` contient deux onglets :

- `Infos generales` : flux RSS publics francophones par rubrique ;
- `News marche` : actualites Yahoo Finance par actif suivi dans le comparateur.

Rubriques generalistes disponibles :

- A la une ;
- Politique ;
- International ;
- Economie ;
- Tech / Sciences ;
- Culture / Societe ;
- France.

Les flux proviennent de medias comme Franceinfo, Le Monde, Le Figaro, BFMTV, France 24, RFI, Les Echos, Challenges et 20 Minutes selon les rubriques.

### Logs Et Administration

L'application contient aussi :

- une page de logs de connexion et d'evenements d'authentification ;
- une page d'administration utilisateurs ;
- creation, blocage, reactivation et deverrouillage de comptes ;
- reinitialisation de mot de passe ;
- roles `admin` et `user`.

## Algorithme De Recommandation D'Actions

Les recommandations visibles dans `Analyse` > `Valeurs a fort potentiel` sont calculees par `worker.py`, independamment de Streamlit. Les resultats sont ecrits dans `data/cache/stock_ideas.json`, puis l'application les lit pour afficher une liste rapide et stable.

### Univers De Depart

Le worker construit un univers d'actions US en combinant :

- top movers du jour sur plusieurs segments de capitalisation ;
- actions liquides triees par volume ;
- watchlist persistante si elle existe ;
- filtrage des instruments peu pertinents : warrants, rights, units, preferred shares, acquisition corps, etc.

Les segments sont volontairement equilibres :

- small caps a partir d'environ 300 M$ ;
- mid caps autour de 2 a 10 Md$ ;
- large caps au-dessus de 10 Md$.

L'objectif est d'eviter que l'univers soit domine uniquement par les tres grosses capitalisations.

### Donnees Utilisees

Pour chaque ticker candidat, le worker recupere environ trois mois d'historique Yahoo Finance et calcule :

- cours actuel ;
- variation de seance ;
- capitalisation ;
- volume et volume moyen ;
- moyennes mobiles 20, 50 et 200 jours ;
- proximite au plus haut 52 semaines ;
- RSI ;
- MACD ;
- position dans les bandes de Bollinger ;
- qualite de tendance par regression lineaire ;
- force relative vs SPY sur 1 mois et 3 mois ;
- proximite d'earnings quand Yahoo fournit l'information.

### Score Technique

Le score final est une somme de blocs plafonnes.

Bloc `Tendance` jusqu'a 3 points :

- prix au-dessus de la moyenne mobile 50 jours ;
- moyenne mobile 50 jours au-dessus de la 200 jours ;
- tendance lineaire propre avec R2 eleve ;
- pente positive ;
- penalite legere si la pente recente devient negative.

Bloc `Momentum` jusqu'a 2,5 points :

- variation positive du jour ;
- confirmation par volume inhabituel ;
- confirmation par structure de tendance ;
- RSI sain ou fort ;
- signal MACD haussier ;
- penalites si MACD devient baissier.

Bloc `Force` jusqu'a 2 points :

- proximite du plus haut 52 semaines ;
- surperformance vs SPY sur 1 mois ;
- surperformance vs SPY sur 3 mois.

Bloc `Setup` jusqu'a 2 points :

- pullback proche de la moyenne mobile 20 jours ;
- volume qui se tarit pendant la respiration du prix ;
- compression de range sur 5 jours ;
- penalite si le prix est trop etire par rapport a la MA20.

Bloc `Risque` :

- malus si RSI tres eleve ;
- malus si extension Bollinger ;
- malus si le prix est trop loin de la MA20 ;
- malus supplementaire si des earnings sont tres proches.

Le score est borne entre 0 et 10. Les colonnes affichees dans l'application exposent aussi des sous-scores et signaux : tendance, momentum, force, setup, risque, RSI, MACD, Bollinger, RS vs SPY et earnings.

### Analyse IA Optionnelle

Si `OPENAI_API_KEY` est configuree, l'utilisateur peut lancer une analyse IA sur les actions affichees. L'application recupere les titres d'actualites Yahoo Finance des tickers selectionnes, puis demande un court diagnostic en JSON :

- sentiment ;
- catalyseur ;
- risque ;
- commentaire synthetique en francais.

Cette analyse est ponctuelle et declenchee par bouton, contrairement au scoring quantitatif qui est calcule par le worker.

## Worker De Fond

Le fichier `worker.py` est prevu pour tourner en continu via systemd.

Jobs planifies :

- toutes les 5 minutes : refresh des movers marche ;
- toutes les 30 minutes : scoring des actions ;
- toutes les 60 minutes : scoring des secteurs ;
- chaque matin a 7h : snapshot de contexte marche ;
- chaque soir a 18h : snapshot historique du portefeuille/recommandations ;
- le week-end : nettoyage des anciens caches.

Caches produits :

```text
data/cache/movers.json
data/cache/stock_ideas.json
data/cache/sectors.json
data/cache/morning_snapshot.json
data/cache/snapshot_YYYY-MM-DD.json
```

Dans l'interface, la section recommandations indique si le worker semble actif, lent ou arrete selon l'age du cache `stock_ideas`.

## Podcast Et Briefing IA

Dans `Actualites` > `Infos generales`, les administrateurs peuvent generer un briefing audio.

### Construction Du Contexte

L'application collecte :

- articles RSS des rubriques selectionnees ;
- titres, resumes, sources, dates et URLs ;
- contenu complet des articles quand `trafilatura` arrive a l'extraire ;
- clusters de sujets similaires pour reperer ce qui revient dans plusieurs sources ;
- contexte marche optionnel ;
- resume du portefeuille optionnel ;
- actualites des tickers du portefeuille quand le portefeuille est inclus.

Les articles sont dedoublonnes, classes et enrichis avant generation.

### Selection Des Articles

Quand une cle OpenAI est disponible, l'application fait une premiere passe LLM pour selectionner les articles les plus importants parmi les nouvelles collectees. Les criteres demandes au modele sont :

- importance nationale ou internationale ;
- impact societal ou economique ;
- diversite des sujets ;
- pertinence pour un briefing generaliste.

Si l'appel OpenAI echoue, l'application utilise les articles les plus recents apres dedoublonnage.

### Generation Du Script

La deuxieme passe genere un script pret a etre lu, en francais, avec une structure imposee :

- introduction directe avec "Bonjour Rafik." ;
- sujet principal developpe ;
- sujets secondaires ;
- point marche ou portefeuille si le contexte est active ;
- conclusion courte.

Durations disponibles :

- 3 minutes ;
- 5 minutes ;
- 10 minutes.

Le prompt force une longueur cible et demande de ne pas inventer de faits absents du contexte.

### Generation Audio

Si `OPENAI_API_KEY` est configuree, l'application peut generer un MP3 via le modele TTS configure. Les parametres incluent :

- voix ;
- direction vocale ;
- ton du briefing ;
- duree cible.

Sans OpenAI, l'application peut utiliser un moteur local si disponible :

- `espeak-ng` ;
- `espeak`.

Les briefings sont sauvegardes dans :

```text
data/briefings/
```

Chaque dossier peut contenir :

```text
script.md
context.json
briefing.mp3
```

### Email Du Briefing

Un script genere peut etre envoye par email. L'application prend aussi en charge un envoi automatique du briefing configure dans l'UI :

- activation/desactivation ;
- heure d'envoi ;
- liste des destinataires ;
- protection contre le double envoi le meme jour ;
- envoi SMTP via les variables `.env`.

Le timer `briefing-email.timer` verifie regulierement si l'heure configuree est atteinte, puis lance :

```bash
python app.py send-briefing-email
```

## Recap Infos Par Email

L'application sait aussi construire un recap email text/html des actualites du jour avec :

- infos bourse/marche ;
- valeurs a fort potentiel ;
- actualites generales par rubrique ;
- liens vers les articles.

Variables SMTP :

```env
NEWS_SMTP_HOST=smtp.gmail.com
NEWS_SMTP_PORT=465
NEWS_SMTP_USER=ton-adresse@gmail.com
NEWS_SMTP_PASSWORD=mot-de-passe-application
NEWS_EMAIL_FROM=ton-adresse@gmail.com
NEWS_EMAIL_TO=destinataire1@example.com,destinataire2@example.com
```

Avec Gmail, utiliser un mot de passe d'application, pas le mot de passe principal.

## Authentification Et Donnees Locales

Le projet contient une authentification locale :

- utilisateurs stockes dans SQLite ;
- mots de passe haches en PBKDF2-SHA256 avec sel unique ;
- blocage temporaire apres echecs de connexion ;
- sessions par cookie refresh token ;
- journal local des evenements de securite ;
- roles `admin` et `user`.

Dans l'etat actuel du fichier `app.py`, l'application utilise aussi un ecran d'acces simplifie par mot de passe avant l'interface principale. Le compte courant est force sur un utilisateur admin local `rafik`, ce qui simplifie l'usage personnel mais ne remplace pas un modele multi-utilisateur complet en production publique.

Fichiers de donnees principaux :

```text
data/users.sqlite3
data/jwt_secret.key
data/cache/
data/briefings/
data/worker.log
```

## Sources De Donnees

- Actions US : Nasdaq Trader pour l'annuaire, Yahoo Finance pour les prix et metadonnees.
- Grandes valeurs europeennes : liste locale dans `app.py`.
- Cryptos : CoinGecko pour l'annuaire, Yahoo Finance pour certains prix.
- Indices, matieres premieres, devises : Yahoo Finance.
- News marche par actif : Yahoo Finance via `yfinance`.
- Infos generales : flux RSS publics de medias francophones.
- Contenu d'articles : extraction HTML via `trafilatura` quand possible.
- Generation IA : OpenAI Responses API et OpenAI TTS si `OPENAI_API_KEY` est configuree.

## Installation Locale

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

## Variables D'Environnement

Exemple `.env` :

```env
NEWS_SMTP_HOST=smtp.gmail.com
NEWS_SMTP_PORT=465
NEWS_SMTP_USER=ton-adresse@gmail.com
NEWS_SMTP_PASSWORD=mot-de-passe-application
NEWS_EMAIL_FROM=ton-adresse@gmail.com
NEWS_EMAIL_TO=destinataire@example.com

OPENAI_API_KEY=sk-...
OPENAI_SCRIPT_MODEL=gpt-4o-mini
```

`.env` doit rester local et ne doit pas etre versionne.

## Structure Du Projet

```text
.
├── app.py                         # Application Streamlit et commandes email
├── worker.py                      # Worker de scoring et caches marche
├── cache.py                       # Lecture/ecriture JSON dans data/cache
├── requirements.txt
├── docs/
│   └── ameliorations.md
├── deploy/
│   ├── streamlit-app.service
│   ├── worker.service
│   ├── daily-news-email.service
│   ├── daily-news-email.timer
│   ├── briefing-email.service
│   └── briefing-email.timer
└── data/
    ├── cache/
    ├── briefings/
    ├── users.sqlite3
    └── worker.log
```

## Deploiement Systemd

Le serveur peut utiliser :

- un service Streamlit sur `localhost:8501`, souvent derriere nginx ;
- `worker.service` pour les calculs en fond ;
- `briefing-email.timer` pour l'envoi automatique du briefing IA ;
- optionnellement `daily-news-email.timer` pour le recap infos quotidien.

Exemple d'installation des units fournies :

```bash
sudo install -D -m 644 deploy/streamlit-app.service /etc/systemd/system/streamlit-app.service
sudo install -D -m 644 deploy/worker.service /etc/systemd/system/worker.service
sudo install -D -m 644 deploy/briefing-email.service /etc/systemd/system/briefing-email.service
sudo install -D -m 644 deploy/briefing-email.timer /etc/systemd/system/briefing-email.timer
sudo systemctl daemon-reload
sudo systemctl enable --now streamlit-app
sudo systemctl enable --now worker
sudo systemctl enable --now briefing-email.timer
```

Sur le serveur actuel, le service actif peut s'appeler `streamlit.service` au lieu de `streamlit-app.service`. Eviter d'activer deux services Streamlit sur le meme port `8501`.

## Commandes Utiles

Lancer Streamlit :

```bash
streamlit run app.py
```

Envoyer le recap infos :

```bash
python app.py send-daily-news-email
```

Verifier/envoyer le briefing automatique selon l'heure configuree :

```bash
python app.py send-briefing-email
```

Lancer le worker :

```bash
python worker.py
```

Verifier les services :

```bash
systemctl status streamlit.service
systemctl status worker.service
systemctl status briefing-email.timer
```

## Limites Connues

- Les donnees Yahoo Finance peuvent etre incompletes ou temporairement indisponibles.
- Les scores d'actions dependent de la qualite des donnees historiques, volumes et metadonnees.
- Le scoring ne tient pas compte de toute l'analyse fondamentale d'une entreprise.
- Les flux RSS peuvent changer ou devenir indisponibles.
- L'extraction du contenu complet des articles peut echouer sur certains sites.
- La generation IA depend de la cle OpenAI, du reseau et des limites API.
- L'application est orientee usage personnel et necessite durcissement avant exposition publique large.

## Fichiers Locaux Non Versionnes

Les elements suivants doivent rester hors Git :

- `.env`
- `.venv/`
- `data/users.sqlite3`
- `data/jwt_secret.key`
- `data/cache/`
- `data/briefings/`
- logs locaux

Le fichier `.gitignore` est configure pour eviter de pousser secrets et fichiers temporaires.
