# Guide des recommandations d'actions

Ce document explique comment lire les recommandations affichees dans `Analyse` > `Valeurs a fort potentiel`.

## Principe General

Le worker ne cherche plus a produire un top instantane purement reactif. Il utilise maintenant :

1. un univers journalier fixe ;
2. un rescoring regulier sur ce meme univers ;
3. une confirmation progressive des signaux ;
4. un historique SQLite de chaque calcul ;
5. des categories de setup separees.

L'objectif est d'avoir moins d'idees opportunistes et davantage de signaux suivables.

## Univers Journalier

Le cache suivant contient l'univers du jour :

```text
data/cache/daily_universe.json
```

Il est reconstruit une fois par jour, puis reutilise pendant la seance.
Le job de scoring toutes les 30 minutes ne change donc plus completement de population a chaque passage.

L'univers combine :

- movers du jour ;
- tickers avec momentum relatif ;
- actions liquides ;
- watchlist si disponible ;
- filtres contre les instruments speciaux.

## Filtres Qualite

Un ticker est ignore si les donnees ne passent pas les filtres minimum :

- prix minimum ;
- capitalisation minimum ;
- volume moyen minimum ;
- historique minimum disponible ;
- instrument special ou ticker douteux.

Les valeurs par defaut sont definies dans `worker.py` :

```text
MIN_PRICE
MIN_MARKET_CAP
MIN_AVG_VOLUME
MIN_HISTORY_DAYS
```

## Score

Le score total reste borne entre 0 et 10.

Il combine :

- tendance ;
- momentum ;
- force relative ;
- qualite du setup ;
- risques immediats.

Les sous-scores affiches ou stockes permettent de comprendre d'ou vient le score.

## Categories De Setup

Chaque ticker recoit un `Setup_Type`.

### breakout

Signal d'acceleration ou de sortie par le haut.

Indices typiques :

- proximite du plus haut 52 semaines ;
- volume superieur a la moyenne ;
- MACD haussier ;
- force relative positive vs SPY.

### trend

Signal de tendance installee.

Indices typiques :

- prix au-dessus des moyennes mobiles ;
- MA50 au-dessus de MA200 ;
- pente positive ;
- tendance statistiquement propre ;
- surperformance relative.

### pullback

Signal de respiration dans une tendance.

Indices typiques :

- prix proche de la MA20 ;
- volume qui se tarit ;
- range court terme comprime ;
- tendance encore saine.

## Confirmation

Un signal n'est pas automatiquement considere comme valide apres un seul bon score.

Champs importants :

- `Confirmed` : oui/non ;
- `Consecutive_Hits` : nombre de cycles consecutifs au-dessus du seuil ;
- `Recent_Top_Hits` : presence recente dans le top ;
- `Signal_Age_Minutes` : age du signal ;
- `Stability_Score` : score synthetique de stabilite sur 100.

Par defaut :

- seuil de confirmation : 4,2/10 ;
- confirmation apres 2 cycles consecutifs ;
- ou presence repetee dans les tops recents.

## Explicabilite

Deux champs sont faits pour etre lus directement :

### why_selected

Explique pourquoi l'action remonte.

Exemples :

- prix au-dessus de la MA50 ;
- tendance MA50 > MA200 ;
- MACD haussier ;
- surperformance vs SPY ;
- pullback proche de la MA20 ;
- range court terme comprime.

### risk_flags

Liste les risques proches.

Exemples :

- earnings proches ;
- RSI eleve ;
- extension Bollinger ;
- prix trop etire vs MA20 ;
- prix sous MA20.

Une action peut avoir un bon score et des risques. Le but est de rendre le compromis visible.

## Historique SQLite

Les signaux sont stockes dans :

```text
data/signals.sqlite3
```

Tables :

- `stock_signal_snapshots` : chaque ticker score a chaque run ;
- `stock_signal_state` : etat courant par ticker ;
- `stock_signal_outcomes` : table preparee pour les mesures de performance futures.

La table `stock_signal_snapshots` permet d'analyser :

- evolution du score ;
- evolution du rang ;
- stabilite du signal ;
- taux de confirmation ;
- repartition par setup.

## Base Pour Backtest

La table `stock_signal_outcomes` est prevue pour calculer ensuite :

- performance J+1 ;
- performance J+3 ;
- performance J+5 ;
- performance J+10 ;
- gain max ;
- drawdown max ;
- taux de reussite par setup ;
- performance par tranche de score.

Le calcul automatique des outcomes n'est pas lance a chaque scoring afin de ne pas ajouter de charge reseau inutile au worker.

## Interpretation Recommandee

Pour un usage personnel, privilegier :

1. signaux confirmes ;
2. stabilite elevee ;
3. risques immediats limites ;
4. setup compatible avec le style recherche ;
5. liquidite suffisante ;
6. contexte news/fondamental verifie dans la fiche entreprise.

Le score doit etre un point de depart, pas une decision automatique.
