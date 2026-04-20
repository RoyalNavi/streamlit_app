# Refonte du moteur de recommandations

## Etat actuel

Le worker reconstruit actuellement son univers a chaque passage du job `job_score_stocks`.
Cet univers depend des screens Yahoo du moment : movers, volume, momentum 1 mois et watchlist.
Le resultat est trie par score instantane puis ecrit dans `data/cache/stock_ideas.json`.

Ce fonctionnement donne des idees reactives, mais trop sensibles au bruit :

- l'univers change toutes les 30 minutes ;
- une action peut entrer dans le top uniquement a cause d'un mouvement court terme ;
- il n'y a pas d'historique exploitable des scores ;
- la confirmation d'un signal n'est pas mesuree ;
- les setups `breakout`, `trend` et `pullback` sont melanges dans un seul classement.

## Probleme a resoudre

L'objectif est de rendre les recommandations plus utiles pour une aide a la decision personnelle :

- univers de tickers stable pendant la journee ;
- scoring regulier sur ce meme univers ;
- confirmation sur plusieurs cycles ;
- historique de scores consultable et reutilisable pour backtest ;
- explication visible du signal et des risques ;
- classements separes par setup.

## Architecture cible

### Univers journalier

Le worker construit un univers du jour une fois par date de marche et le stocke dans :

```text
data/cache/daily_universe.json
```

Le cache contient :

- `date` ;
- `created_at` ;
- liste de tickers ;
- source de chaque ticker ;
- metadonnees disponibles au moment de la construction.

Pendant la journee, `job_score_stocks` relit cet univers et rescore les memes tickers.
Si le cache manque ou ne correspond pas a la date du jour, il est reconstruit.

### Persistance SQLite

Les snapshots de scores sont stockes dans :

```text
data/signals.sqlite3
```

Table principale :

```text
stock_signal_snapshots
```

Elle stocke au minimum :

- timestamp du calcul ;
- run_id ;
- ticker ;
- rang global ;
- rang dans la categorie ;
- score total ;
- sous-scores ;
- prix ;
- setup dominant ;
- RSI, MACD, RS vs SPY, distance MA20 ;
- raisons de selection ;
- drapeaux de risque ;
- etat de confirmation ;
- stabilite ;
- age du signal.

Une table `stock_signal_state` garde l'etat courant par ticker :

- nombre de cycles consecutifs au-dessus du seuil ;
- premiere et derniere detection ;
- dernier score ;
- nombre d'apparitions recentes dans le top ;
- setup courant ;
- confirmation.

### Confirmation

Un signal est considere confirme si :

- son score est superieur au seuil de confirmation ;
- il est reste au-dessus de ce seuil pendant plusieurs cycles consecutifs ;
- ou il apparait de facon repetee dans le top recent.

Parametres initiaux :

- seuil de confirmation : `4.2` ;
- cycles requis : `2` ;
- fenetre d'apparitions recentes : derniers `5` calculs ;
- top recent observe : top `15`.

### Categories de setup

Chaque ticker recoit un setup dominant :

- `breakout` : force relative, proximite du plus haut 52 semaines, volume et MACD positifs ;
- `trend` : tendance propre, prix au-dessus des moyennes, RS positive ;
- `pullback` : tendance saine mais prix proche de la MA20, respiration constructive.

Le cache `stock_ideas.json` reste compatible avec l'UI existante, mais ajoute :

- `Setup_Type` ;
- `Confirmed` ;
- `Consecutive_Hits` ;
- `Signal_Age_Minutes` ;
- `Stability_Score` ;
- `why_selected` ;
- `risk_flags` ;
- `Rank_Category`.

Le cache ajoute aussi un champ meta :

```json
{
  "data": [...],
  "meta": {
    "run_id": "...",
    "universe_date": "YYYY-MM-DD",
    "universe_size": 80,
    "confirmed_count": 12,
    "setup_counts": {"breakout": 3, "trend": 5, "pullback": 4}
  }
}
```

## Modifications prevues

1. Ajouter la construction et la lecture d'un univers journalier stable.
2. Ajouter une base SQLite locale pour les snapshots de signaux.
3. Enrichir `_score_stock` avec :
   - filtres qualite plus stricts ;
   - distance MA20 ;
   - setup dominant ;
   - raisons lisibles ;
   - risques lisibles.
4. Ajouter une passe de confirmation et stabilite apres scoring.
5. Ecrire les snapshots dans SQLite a chaque run.
6. Ecrire un cache `stock_ideas.json` enrichi mais encore lisible par l'UI.
7. Mettre a jour l'interface Streamlit pour afficher :
   - setup ;
   - confirmation ;
   - stabilite ;
   - age ;
   - raisons ;
   - risques.
8. Ajouter une documentation utilisateur/technique sur la nouvelle logique.

## Compromis

- Le systeme reste rule-based et explicable, sans modele lourd.
- SQLite est utilise pour l'historique car il est robuste et deja disponible dans Python.
- Les backtests complets ne sont pas calcules automatiquement a chaque run pour economiser CPU/reseau.
- L'infrastructure stocke les donnees necessaires pour ajouter un script de backtest incremental ensuite.
