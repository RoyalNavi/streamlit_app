# Documentation et propositions d'amelioration

## Objectif de l'application

L'application est un tableau de bord personnel de suivi financier et d'actualites. Elle permet de consulter des actifs de marche, comparer leurs performances, lire des actualites, recevoir un recap par email et gerer des utilisateurs avec une authentification locale.

Elle est aujourd'hui pensee pour un usage prive : acces par email/mot de passe, compte administrateur, deploiement systemd sur un serveur local expose via Freebox, et envoi de recap quotidien par Gmail SMTP.

## Fonctionnement actuel

### Authentification et utilisateurs

- Creation du premier administrateur au premier lancement.
- Connexion par email et mot de passe.
- Hachage des mots de passe avec PBKDF2-SHA256 et sel unique.
- JWT d'acces valable 5 minutes.
- Refresh token valable 30 jours, stocke sous forme hachee dans SQLite.
- Deconnexion avec revocation du refresh token.
- Blocage temporaire apres plusieurs echecs de connexion.
- Page admin pour creer, bloquer, reactiver, deverrouiller et modifier les utilisateurs.
- Journal d'audit local des actions de securite.

### Tableau de bord finance

- Recherche d'actions, indices, ETF et cryptos.
- Comparaison multi-actifs sur plusieurs periodes.
- Graphiques de prix et de performance.
- Option de lissage visuel des fermetures de marche.
- Fiche entreprise avec activite, chiffre d'affaires, capitalisation et sentiment indicatif.
- Selection quantitative de mid-cap a fort potentiel.
- Mouvements de marche du jour.

### Actualites et recap email

- Actualites marche via Yahoo Finance.
- Actualites generales via flux RSS publics.
- Envoi manuel d'un recap par email depuis l'interface admin.
- Envoi automatique quotidien via `daily-news-email.timer` a 08:00.
- Recap ordonne avec les infos bourse/marche d'abord, les idees mid-cap ensuite, puis les actualites generales en dernier.
- Possibilite d'ajouter des destinataires ponctuels au moment de l'envoi.

### Deploiement

- Service `streamlit-app.service` lance directement Streamlit sur le port 80.
- Timer `daily-news-email.timer` pour l'envoi quotidien.
- Configuration sensible dans `.env`, ignore par Git.
- Donnees locales dans `data/`, ignorees par Git.

## Points forts actuels

- Application deja utilisable de bout en bout.
- Authentification simple pour l'utilisateur final.
- Gestion admin integree.
- Donnees sensibles principales hors Git.
- Recap email manuel et automatique operationnel.
- Architecture simple : un fichier principal, SQLite, systemd.
- Peu de dependances, donc maintenance raisonnable.

## Limites et risques

### Securite

- Les tokens d'authentification passent dans l'URL via query params. C'est pratique avec Streamlit, mais moins robuste que des cookies `HttpOnly` et `Secure`.
- L'application est exposee en HTTP, pas HTTPS.
- Le compte Gmail SMTP est configure localement et depend d'un mot de passe d'application.
- Il n'y a pas encore de sauvegarde automatique de la base utilisateurs.
- Il n'y a pas de rotation automatique du secret JWT.

### Fiabilite

- Les donnees Yahoo Finance, CoinGecko et RSS peuvent echouer ou changer de format.
- L'envoi email depend de Gmail SMTP et de la validite du mot de passe d'application.
- Le service est simple, mais sans supervision applicative fine.
- Il n'y a pas encore de page de statut technique.

### UX

- L'app est fonctionnelle, mais certaines zones admin et email pourraient etre plus guidees.
- Le recap email est utile, mais pourrait etre personnalise par utilisateur.
- Les recommandations mid-cap sont quantitatives, mais pas encore expliquees avec assez de contexte fondamental.

### Qualite de code

- `app.py` concentre beaucoup de responsabilites : authentification, UI, finance, emails, systemd CLI.
- Il n'y a pas encore de suite de tests automatisee structuree.
- Les operations metier gagneraient a etre separees en modules.

## Propositions d'amelioration prioritaires

### Priorite 1 - Securiser l'acces public

Mettre l'application derriere HTTPS.

Options possibles :

- Cloudflare Tunnel : evite d'ouvrir directement le port de la box et fournit HTTPS.
- Caddy : reverse proxy simple avec certificats automatiques.
- Nginx + Let's Encrypt : plus classique, mais plus de configuration.

Impact attendu :

- Connexion plus sure depuis mobile.
- Moins de risques d'interception des tokens.
- Meilleure compatibilite navigateur.

### Priorite 2 - Remplacer les tokens d'URL par des cookies securises

Objectif : stocker le refresh token dans un cookie `HttpOnly`, `Secure`, `SameSite=Lax`.

Impact attendu :

- Les tokens ne sont plus visibles dans l'URL.
- Moins de risques de fuite via historique, capture d'ecran ou copier-coller.
- Experience utilisateur identique : pas de reconnexion au rafraichissement.

Point technique :

- Streamlit ne facilite pas toujours l'ecriture de cookies serveur.
- Une solution propre peut passer par un petit reverse proxy ou une couche backend dediee.

### Priorite 3 - Ajouter une page "Parametres email"

Actuellement, la configuration SMTP se fait via `.env` et le formulaire d'envoi manuel.

Amelioration proposee :

- Page admin dediee `Parametres`.
- Test d'envoi email depuis l'interface.
- Destinataires par defaut modifiables depuis l'UI.
- Heure d'envoi quotidien configurable.
- Activation/desactivation du recap automatique.

Impact attendu :

- Moins besoin d'intervenir en SSH.
- Configuration plus claire.
- Meilleure autonomie pour l'administrateur.

### Priorite 4 - Personnaliser le recap quotidien

Ajouter des preferences :

- Choix des rubriques d'actualites.
- Nombre d'articles par rubrique.
- Inclusion ou non des mid-cap.
- Inclusion ou non des mouvements de marche.
- Liste de destinataires par profil.
- Envoi test avant sauvegarde.

Impact attendu :

- Recap plus pertinent.
- Possibilite d'avoir plusieurs listes : personnel, famille, equipe.

### Priorite 5 - Sauvegardes automatiques

Mettre en place une sauvegarde quotidienne de :

- `data/users.sqlite3`
- `.env`
- `data/jwt_secret.key`

Avec retention locale de 7 a 30 jours.

Impact attendu :

- Recuperation rapide en cas d'erreur ou corruption.
- Protection contre une mauvaise manipulation admin.

## Ameliorations finance

### Score mid-cap plus robuste

Le score actuel utilise surtout momentum, liquidite et tendances techniques.

Ameliorations possibles :

- Croissance du chiffre d'affaires.
- Marge operationnelle.
- Dette nette / EBITDA quand disponible.
- Free cash flow.
- PER ou EV/Sales relatif au secteur.
- Revision des analystes.
- Volatilite et drawdown.

Livrable propose :

- Un score separe en 4 blocs : momentum, qualite, valorisation, risque.
- Une explication lisible pour chaque action.

### Watchlist personnelle

Ajouter une vraie watchlist utilisateur :

- Ajouter/retirer des tickers.
- Notes personnelles.
- Prix d'achat indicatif.
- Alertes de variation.
- Inclusion automatique dans le recap email.

### Alertes marche

Ajouter des alertes :

- Variation journaliere superieure a un seuil.
- Franchissement de moyenne mobile.
- Nouveau plus haut / plus bas.
- Actualite importante sur un ticker suivi.

Canaux possibles :

- Email.
- Notification mobile via service externe.
- Message Telegram ou WhatsApp via API.

## Ameliorations actualites

### Meilleure hierarchisation du recap

Ajouter un resume en haut du mail :

- 3 points marche du jour.
- 3 actualites generales importantes.
- 3 actions a surveiller.

Puis garder les sections detaillees en dessous.

### Filtrage anti-doublons plus fort

Les flux RSS peuvent reprendre les memes sujets.

Ameliorations :

- Detection de titres similaires.
- Regroupement par sujet.
- Limite par source.
- Priorite aux sources fiables ou preferes.

### Resume automatique

Ajouter un court resume en francais pour chaque article, au lieu de reprendre uniquement le descriptif RSS.

Attention :

- Necessite un modele de resume ou une API externe.
- Il faut limiter les couts et eviter de stocker trop de contenu d'articles.

## Ameliorations UX

### Page d'accueil apres connexion

Ajouter une vue plus synthetique :

- Marche aujourd'hui.
- Recap rapide des actualites.
- Top mid-cap.
- Bouton direct "envoyer le recap".

### Meilleure experience mobile

Verifier et optimiser :

- Largeur des tableaux.
- Boutons admin.
- Formulaires email.
- Graphiques Plotly.
- Lisibilite du menu lateral.

### Etats de chargement plus explicites

Ajouter des messages plus utiles quand une source externe echoue :

- Source indisponible.
- Dernieres donnees cachees utilisees.
- Date de derniere mise a jour.

## Ameliorations techniques

### Decouper `app.py`

Structure proposee :

```text
streamlit_app/
├── app.py
├── auth.py
├── email_recap.py
├── finance_data.py
├── news_data.py
├── user_admin.py
├── ui/
│   ├── dashboard.py
│   ├── news.py
│   └── users.py
└── deploy/
```

Impact attendu :

- Code plus lisible.
- Tests plus simples.
- Modifications moins risquees.

### Ajouter des tests automatises

Tests prioritaires :

- Hash et verification de mot de passe.
- Creation utilisateur.
- Blocage/deblocage.
- JWT valide/expire/invalide.
- Refresh token valide/revoque.
- Fusion des destinataires email.
- Construction du recap.

### Logging applicatif

Ajouter des logs structures pour :

- Echecs de recuperation RSS.
- Echecs Yahoo/CoinGecko.
- Envois email reussis/echoues.
- Erreurs auth.

### Page de statut admin

Afficher :

- Etat SMTP.
- Prochain envoi planifie.
- Dernier envoi reussi.
- Etat des sources de donnees.
- Taille de la base SQLite.
- Version de l'application.

## Feuille de route proposee

### Semaine 1

- Page admin `Parametres email`.
- Test SMTP depuis l'UI.
- Sauvegarde quotidienne SQLite + `.env` + secret JWT.
- Page de statut admin simple.

### Semaine 2

- HTTPS via Cloudflare Tunnel ou Caddy.
- Amelioration mobile des formulaires et tableaux.
- Logs d'envoi email et affichage du dernier envoi.

### Semaine 3

- Refactorisation en modules.
- Tests automatises de l'auth et du recap email.
- Preferences de recap par utilisateur.

### Semaine 4

- Score mid-cap enrichi.
- Watchlist utilisateur.
- Alertes email sur tickers suivis.

## Prochaines actions recommandees

1. Mettre en place HTTPS.
2. Ajouter une page admin de configuration email.
3. Ajouter une sauvegarde quotidienne.
4. Ajouter un historique des envois de recap.
5. Decouper progressivement `app.py` en modules.

Ces cinq actions apportent le meilleur rapport utilite/securite/maintenabilite pour l'etat actuel du projet.
