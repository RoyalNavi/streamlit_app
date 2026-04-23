# Recommandations d'actions

Ce document decrit le fonctionnement des recommandations affichees dans `Analyse` > `Valeurs a fort potentiel`.

Il existe maintenant deux moteurs distincts :

- le moteur standard, dans `worker.py`, pour les signaux stables, propres et suivables ;
- le scanner small caps explosives, dans `smallcap_scanner.py`, pour les signaux plus agressifs bases sur momentum court terme, breakout et volume relatif.

L'interface Streamlit lit ensuite les caches produits par le worker.

## Vue D'Ensemble

Le moteur standard ne cherche pas seulement les plus fortes hausses du moment. Il suit un pipeline en plusieurs etapes :

1. construire un univers journalier de tickers ;
2. telecharger les historiques Yahoo Finance ;
3. enrichir chaque quote avec prix, volume, moyennes et metadonnees ;
4. appliquer des filtres de qualite ;
5. calculer un score technique borne entre 0 et 10 ;
6. classer les actions par score ;
7. appliquer une confirmation basee sur de vraies nouvelles observations de marche ;
8. retrier l'affichage final en privilegiant les signaux confirmes et stables ;
9. ecrire les resultats dans `data/cache/stock_ideas.json` et l'historique dans SQLite.

Le but est d'obtenir des idees suivables, pas un top instantane qui change completement a chaque run.

Le scanner small caps explosives suit une autre philosophie : il ne cherche pas un signal propre et stable, mais un mouvement speculatif actif avec variation forte, participation en volume et potentiel de continuation.

## Reset Des Recommandations

Apres une refonte du moteur, les anciens caches et l'ancien etat SQLite peuvent contenir des signaux incoherents avec les nouvelles regles.

Le script de reset archive l'etat courant au lieu de le supprimer directement :

```bash
.venv/bin/python scripts/reset_recommendations.py
```

Fichiers archives dans `data/archive/recommendations_reset_YYYYMMDD_HHMMSS/` :

- `data/cache/stock_ideas.json` ;
- `data/cache/stock_ideas_meta.json` ;
- `data/cache/smallcap_ideas.json` ;
- `data/cache/smallcap_ideas_meta.json` ;
- `data/cache/daily_universe.json` ;
- `data/signals.sqlite3`.

Fichiers conserves :

- `data/users.sqlite3` ;
- watchlists et preferences utilisateur ;
- annuaires `market_directory.csv` et `crypto_directory.csv` ;
- caches d'actualites ;
- snapshots portefeuille.

Apres l'archive, le script recree une base `data/signals.sqlite3` vide avec les tables attendues.

Pour reconstruire les recommandations :

```bash
systemctl restart worker
```

ou, sans systemd :

```bash
.venv/bin/python -c "import worker; worker.job_score_stocks()"
```

Pour reconstruire uniquement le cache small caps explosives :

```bash
.venv/bin/python -c "import worker; worker.job_score_small_caps()"
```

## Frequence Des Jobs

Le worker est prevu pour tourner en continu.

Dans `worker.py`, le job principal de scoring standard est `job_score_stocks`.

Il est planifie environ toutes les 30 minutes. Chaque run recoit :

- un `run_id` unique ;
- un timestamp `calculated_at` ;
- le meme univers journalier si le cache du jour existe deja.

Un run peut recalculer les scores sans pour autant compter comme nouveau cycle de confirmation si Yahoo Finance n'a pas fourni de nouvelle bougie utile.

Le scanner small caps explosives est `job_score_small_caps`.

Il est planifie toutes les 7 minutes. Chaque run :

- lance `scan_small_cap_opportunities()` depuis `smallcap_scanner.py` ;
- ecrit `data/cache/smallcap_ideas.json` ;
- ecrit `data/cache/smallcap_ideas_meta.json` ;
- ne modifie pas `stock_ideas.json` ;
- ne modifie pas l'etat SQLite de confirmation du moteur standard.

Le job small caps prend en general autour de 25 a 35 secondes, selon la latence Yahoo Finance.

## Regime De Marche Global

Les deux moteurs utilisent un filtre de regime simple calcule dans :

```text
market_regime.py
```

Ce module ne remplace pas le scoring technique. Il ajoute seulement un contexte global pour eviter de traiter un breakout small cap de la meme facon dans un marche porteur et dans un marche fragile.

Le regime peut prendre trois valeurs :

- `RISK_ON` : environnement favorable au risque ;
- `NEUTRAL` : environnement mitigé ;
- `RISK_OFF` : environnement defensif.

Le calcul utilise principalement :

- SPY et QQQ pour le coeur du marche US ;
- IWM pour mesurer l'appetit small caps ;
- prix vs MA50 et MA200 ;
- pente de la MA50 ;
- drawdown depuis le plus haut 20 jours ;
- force relative IWM vs SPY sur environ 20 seances.

Le resultat est ecrit dans les metadonnees des caches :

```text
data/cache/stock_ideas_meta.json
data/cache/smallcap_ideas_meta.json
```

Champs principaux :

- `market_regime.regime` ;
- `market_regime.score` ;
- `market_regime.reasons` ;
- `market_regime.metrics` ;
- `market_regime_adjustment`.

Impact sur le classement :

| Regime | Moteur standard | Small caps explosives |
| --- | --- | --- |
| `RISK_ON` | pas de malus | pas de malus |
| `NEUTRAL` | leger malus sur breakout | leger malus general |
| `RISK_OFF` | malus sur breakout | malus plus fort, surtout breakout |

Le filtre reste volontairement simple et parametrable. Il ne reintroduit pas de confirmation multi-cycles dans le scanner small caps et ne change pas la philosophie du moteur standard.

Desactivation possible :

```bash
MARKET_REGIME_ENABLED=0
```

## Construction De L'Univers

La fonction principale est `build_candidate_universe(spy_hist)`.

Le moteur utilise maintenant deux niveaux :

1. un univers surveille large ;
2. une shortlist plus serree pour le scoring complet.

Objectifs actuels :

- univers surveille : jusqu'a environ 450 tickers ;
- shortlist de scoring complet : jusqu'a environ 170 tickers.

Cela permet d'augmenter la couverture sans lancer le score technique complet sur toute la masse de candidats a chaque reconstruction.

L'univers surveille combine plusieurs sources.

### 1. Screens US Yahoo

Le helper `_screen()` utilise `yf.screen()` avec :

- `region = us` ;
- exchanges `NMS`, `NYQ`, `ASE` ;
- capitalisation minimum ;
- prix minimum ;
- volume journalier minimum ;
- exclusion des instruments speciaux via `EXCLUDED_PATTERN`.

Plusieurs screens sont lances avec des seuils minimums differents :

- `us_large_liquid` : grandes capitalisations liquides ;
- `us_mid_liquid` : mid caps liquides ;
- `us_small_quality` : small caps filtrees ;
- `us_top_volume` : titres a fort volume ;
- `us_movers` : mouvements courts termes.

Cette separation donne plusieurs portes d'entree au screen Yahoo. Les seuils ne sont pas des buckets stricts avec plafond : une grande capitalisation peut apparaitre dans plusieurs screens, puis les tickers sont dedupliques.

Les meilleurs movers sont gardes dans `movers_tickers`.

### 2. Momentum Relatif Et Preselection

Apres la collecte, le worker telecharge les historiques de l'univers surveille et calcule un score leger de preselection.

Ce pre-score utilise :

- liquidite ;
- RS 1 mois contre `SPY` ;
- RS 3 mois contre `SPY` ;
- proximite du plus haut recent ;
- distance a MA20 ;
- RSI ;
- bonus watchlist ;
- bonus source liquide.

Il penalise deja les titres trop etires, en RSI trop tendu ou avec force relative negative.

Seuls les meilleurs candidats de ce pre-score entrent dans la shortlist du scoring complet.

### 3. Europe Liquide Locale

Les actions europeennes viennent de `market_universe.py`.

Le comparateur et les recommandations utilisent une liste locale plus ambitieuse de grandes et mid caps europeennes liquides.

Ce choix est volontaire :

- Yahoo Finance n'a pas d'annuaire Europe aussi simple que Nasdaq Trader ;
- les screens Europe Yahoo peuvent etre incomplets ;
- l'ajout est progressif pour eviter des valeurs obscures ou peu liquides.

Suffixes Yahoo geres :

- `.PA`, `.DE`, `.AS`, `.SW`, `.MC`, `.MI`, `.L`, `.BR`, `.CO`, `.ST`, `.HE`, `.OL`.

Chaque ticker Europe recoit :

- `market_region = Europe` ;
- `exchange` ;
- `country` ;
- `currency` ;
- source `europe_liquid_local`.

### 4. Watchlist

Si une watchlist existe dans le cache applicatif, jusqu'a 20 tickers sont ajoutes.

La watchlist permet de forcer le suivi de valeurs qui ne sortent pas forcement dans les screens Yahoo du jour.

## Metadonnees D'Univers

`stock_ideas_meta.json` expose les tailles importantes :

- `monitored_universe_size` : nombre de tickers surveilles avant preselection ;
- `scoring_shortlist_size` : taille de la shortlist envoyee au score complet ;
- `scored_count` : nombre final d'actions effectivement scorees ;
- `monitored_region_counts` : repartition US / Europe dans l'univers surveille ;
- `shortlist_region_counts` : repartition US / Europe dans la shortlist ;
- `region_counts` : repartition des actions scorees ;
- `source_counts` : contribution des sources de collecte.

L'UI affiche ces informations en resume dans la section recommandations.

## Cache D'Univers Journalier

L'univers final est stocke dans :

```text
data/cache/daily_universe.json
```

La fonction `get_daily_universe()` :

- relit ce cache si la date correspond au jour courant ;
- reconstruit l'univers si le cache manque ou est date d'un autre jour ;
- recharge les historiques Yahoo pour les tickers de l'univers ;
- enrichit les quotes depuis l'historique.

Cela stabilise la population analysee pendant la journee.

## Donnees De Prix Utilisees

Les historiques sont recuperes par `_fetch_histories()`.

Le worker utilise des historiques journaliers Yahoo Finance, typiquement :

- `3mo` pour construire et classer l'univers initial ;
- `6mo` quand l'univers journalier est relu depuis le cache.

La granularite utile pour le scoring et la confirmation est donc la bougie journaliere Yahoo.

Pour chaque ticker, `latest_market_observation(hist)` extrait :

- `last_market_timestamp` : timestamp de la derniere bougie `Close` ;
- `last_observed_price` : dernier prix de cloture disponible.

Ces deux champs viennent du meme historique que celui utilise pour scorer l'action. C'est important : le score et la confirmation parlent de la meme observation de marche.

## Filtres De Qualite

La fonction `_score_stock()` ignore un ticker si les donnees minimales ne sont pas disponibles.

Filtres principaux :

- prix minimum : `MIN_PRICE` ;
- capitalisation minimum : `MIN_MARKET_CAP` ;
- volume moyen minimum : `MIN_AVG_VOLUME` ;
- historique minimum : `MIN_HISTORY_DAYS` ;
- exclusion des warrants, rights, units, preferred shares et acquisition corps.

Pour l'Europe, le seuil de volume moyen est adapte avec `EUROPE_MIN_AVG_VOLUME`. Il est plus bas que le seuil US, car la liquidite est fragmentee entre plusieurs places, mais il reste volontairement conservateur.

Si un ticker n'a pas assez d'historique, pas de capitalisation exploitable ou un volume trop faible, il ne remonte pas dans les recommandations.

## Calcul Du Score

Le score final est borne entre 0 et 10.

Il est calcule par blocs. Chaque bloc mesure une dimension differente.

### Bloc Tendance

Objectif : mesurer si l'action est dans une tendance propre.

Signaux utilises :

- prix au-dessus de la moyenne mobile 50 jours ;
- MA50 au-dessus de MA200 ;
- qualite de tendance lineaire sur 30 jours ;
- pente positive ;
- coefficient `R2` eleve.

Le bloc tendance est plafonne a 3 points.

Ponderation actuelle :

| Signal | Impact |
| --- | ---: |
| Prix > MA50 | `+1.2` |
| MA50 > MA200 | `+1.2` |
| Tendance 30j tres propre (`R2 >= 0.85` et pente positive) | `+1.0` |
| Tendance 30j correcte (`R2 >= 0.65` et pente positive) | `+0.5` |
| Pente negative | `-0.4` |

### Bloc Momentum

Objectif : mesurer si le mouvement recent est constructif.

Signaux utilises :

- variation du jour ;
- confirmation par volume ;
- prix au-dessus de MA50 ;
- RSI ;
- MACD.

La variation du jour n'est pas recompensee brutalement toute seule. Elle pese surtout si le mouvement est confirme par le volume et/ou la structure de tendance.

Le bloc momentum est plafonne a 2,5 points.

Ponderation actuelle :

| Signal | Impact |
| --- | ---: |
| Variation positive confirmee par volume et tendance | `min(variation, 5) * 0.20` |
| Variation positive confirmee par volume ou tendance | `min(variation, 5) * 0.10` |
| Variation positive non confirmee | `min(variation, 3) * 0.03` |
| RSI entre 45 et 65 | `+1.0` |
| RSI entre 65 et 70 | `+0.2` |
| RSI entre 70 et 75 | `0`, puis malus dans le bloc risque |
| MACD croisement haussier | `+1.5` |
| MACD haussier | `+0.8` |
| MACD croisement baissier | `-0.6` |
| MACD baissier | `-0.3` |

### Bloc Force Relative

Objectif : privilegier les actions qui font mieux que le marche.

Signaux utilises :

- proximite du plus haut 52 semaines ;
- force relative 1 mois contre `SPY` ;
- force relative 3 mois contre `SPY`.

Le bloc force relative est plafonne a 2 points.

Ponderation actuelle :

| Signal | Impact |
| --- | ---: |
| Prix >= 95% du plus haut 52 semaines | `+1.2` |
| Prix >= 90% du plus haut 52 semaines | `+0.8` |
| RS 1m et RS 3m positives contre SPY | `+0.8` |
| RS 1m > 5 points | `+0.7` |
| RS 3m > 10 points | `+0.3` |
| RS 1m <= 0 | `-1.8` |
| RS 3m < 0 | `-0.8` |

La force relative est volontairement devenue plus discriminante. Une action qui monte moins bien que `SPY` n'est plus traitee comme une opportunite prioritaire, meme si sa tendance absolue reste positive.

### Bloc Setup

Objectif : distinguer une bonne tendance d'un bon point d'entree.

Signaux utilises :

- distance du prix a la MA20 ;
- pullback proche de la MA20 ;
- volume qui se tarit pendant une respiration ;
- range court terme comprime.

Une action tres solide mais deja trop etiree peut donc avoir un score reduit.

Le bloc setup est plafonne a 2 points.

Ponderation actuelle :

| Signal | Impact |
| --- | ---: |
| Prix entre -1% et 3% de MA20 avec tendance constructive | `+1.7` |
| Prix entre 0% et 3% au-dessus de MA20 sans tendance assez forte | `+1.1` |
| Prix entre 3% et 7% au-dessus de MA20 avec tendance constructive | `+0.8` |
| Prix entre 3% et 7% au-dessus de MA20 sinon | `+0.4` |
| Prix entre 10% et 15% au-dessus de MA20 | `-0.3` |
| Prix >= 15% au-dessus de MA20 | `-0.9` |
| Prix sous MA20 | `-0.3` |
| Volume moyen 3j < 60% du volume moyen 20j | `+0.8` |
| Volume moyen 3j < 80% du volume moyen 20j | `+0.4` |
| Volume moyen 3j > 180% du volume moyen 20j | `-0.3` |
| Range 5j tres comprime | `+0.5` |
| Range 5j moderement comprime | `+0.2` |

### Bloc Risque

Objectif : penaliser les configurations trop fragiles.

Risques pris en compte :

- RSI tres eleve ;
- extension Bollinger ;
- prix trop loin de la MA20 ;
- prix sous la MA20 ;
- earnings proches quand Yahoo fournit l'information.

Ce bloc peut retirer des points au score final.

Ponderation actuelle :

| Risque | Impact |
| --- | ---: |
| RSI > 70 | `-0.45` |
| RSI > 75 | `-1.0` |
| RSI > 75 et prix > 15% au-dessus de MA20 | `-1.6` |
| Prix > 10% au-dessus de MA20 | `-0.45` |
| Prix > 15% au-dessus de MA20 | `-1.0` |
| Prix > 20% au-dessus de MA20 | `-1.5` |
| Extension Bollinger avec RSI > 70 | `-0.5` |
| Extension Bollinger seule | `-0.2` |
| Prix sous bande Bollinger | `-0.3` |
| Earnings entre J+0 et J+3 | `-0.8` apres le score initial |

Ces malus reduisent le biais "tout ce qui monte". Une action tres forte mais deja trop loin de MA20 ou en RSI tendu peut rester surveillee, mais elle descend dans le classement.

Le score final est calcule ainsi :

```text
Score = clamp(
    B_Tendance + B_Momentum + B_Force + B_Setup + B_Risque,
    minimum = 0,
    maximum = 10
)
```

Le champ `Opportunity_Adjustment` expose une synthese lisible du timing :

```text
Opportunity_Adjustment = B_Setup + B_Risque + min(B_Force, 0)
```

Un chiffre positif indique un point d'entree plutot propre. Un chiffre negatif signale une action forte mais moins achetable maintenant.

## Categories De Setup

Apres le score, le worker attribue un `Setup_Type`.

### breakout

Action proche d'une sortie par le haut.

Criteres typiques :

- proche du plus haut 52 semaines ;
- volume superieur a la moyenne ;
- MACD haussier ;
- force relative positive.

Un breakout est reduit s'il n'est pas confirme par au moins un de ces elements :

- volume significatif ;
- consolidation recente / range comprime ;
- retour recent proche de MA20 avant la cassure.

Sans confirmation, le worker ajoute un drapeau `breakout non confirme par volume/consolidation` et reduit le score.

### trend

Action en tendance installee.

Criteres typiques :

- MA50 positive ;
- MA50 > MA200 ;
- tendance statistiquement propre ;
- surperformance contre `SPY`.

### pullback

Action en respiration dans une tendance.

Criteres typiques :

- prix proche de MA20 ;
- tendance encore saine ;
- volume en baisse ;
- range comprime.

`Setup_Type` sert ensuite de filtre dans l'UI.

## Premier Tri

Une fois les tickers scores, le worker fait un premier tri :

```text
Score decroissant
```

Ce tri sert a donner les rangs initiaux :

- `Rank_Global` ;
- `Rank_Category`.

`Rank_Category` est calcule par type de setup.

## Earnings

Apres le premier score, le worker appelle `_fetch_earnings_flag()` en parallele.

Si une publication de resultats est proche :

- `J+0` a `J+3` : malus de score et drapeau de risque fort ;
- `J+4` a `J+7` : drapeau de risque informatif.

Les earnings sont donc visibles dans l'UI et peuvent reduire le classement.

## Confirmation Et Cycles

La confirmation est geree par `apply_signal_confirmation()`.

Un signal est au-dessus du seuil si :

```text
Score >= SIGNAL_CONFIRM_THRESHOLD
```

La valeur actuelle est :

```text
SIGNAL_CONFIRM_THRESHOLD = 4.2
```

Le nombre minimal de cycles consecutifs est :

```text
SIGNAL_CONFIRM_CYCLES = 2
```

Mais un cycle ne compte que s'il y a une nouvelle observation de marche utile.

### Nouvelle Observation

La fonction `_is_new_market_observation(state, row)` compare l'etat precedent du ticker avec le run courant.

Elle considere qu'il y a une nouvelle observation si :

1. `last_market_timestamp` existe et differe du precedent ;
2. ou, si le timestamp est absent ou peu fiable, `last_observed_price` differe significativement du precedent.

Si le worker tourne plusieurs fois pendant que le marche est ferme et que Yahoo renvoie la meme derniere bougie, alors :

```text
new_observation = False
Consecutive_Hits ne monte pas
```

Cela evite de confirmer artificiellement une action seulement parce que le worker a rerun.

### Consecutive_Hits

Regle simplifiee :

```text
si Score >= seuil ET new_observation = True :
    Consecutive_Hits += 1
sinon si Score < seuil :
    Consecutive_Hits = 0
sinon :
    Consecutive_Hits reste stable
```

Donc un signal peut rester bon visuellement sans gagner de cycle si aucune nouvelle bougie utile n'est apparue.

### Recent_Top_Hits

`Recent_Top_Hits` mesure la presence recente du ticker dans le top.

Le worker regarde les derniers runs utiles dans SQLite :

- fenetre : `RECENT_RUN_WINDOW = 5` ;
- top observe : `RECENT_TOP_N = 15`.

Les snapshots sans nouvelle observation sont ignorees pour eviter de compter plusieurs fois la meme bougie.

### Confirmed

Un signal est marque `Confirmed = True` si :

```text
Score >= seuil
ET (
    Consecutive_Hits >= SIGNAL_CONFIRM_CYCLES
    OU Recent_Top_Hits >= 3
)
```

La confirmation veut donc dire :

- le score est encore bon ;
- et le signal a ete observe sur plusieurs vraies observations ;
- ou il revient regulierement dans le haut du classement utile.

### Fraicheur Du Signal

Le score affiche peut etre legerement inferieur au score technique brut.

`apply_signal_confirmation()` ajoute un malus de fraicheur :

| Situation | Malus |
| --- | ---: |
| Signal age de plus de 6 heures | `-0.35` |
| Signal age de plus de 24 heures | `-0.8` |
| Pas de nouvelle observation sur ce run | `-0.25` |

Les champs correspondants sont :

- `Raw_Score` : score technique avant malus de fraicheur ;
- `Age_Penalty` : malus applique ;
- `Score` : score final utilise pour confirmation et classement.

Cette regle evite qu'un ancien bon signal reste indefiniment devant une opportunite plus fraiche.

## Stabilite

Le champ `Stability_Score` est un score interne sur 100.

Il combine :

- nombre de cycles consecutifs ;
- presence recente dans le top ;
- faible variation du score depuis le run precedent.

La stabilite ne remplace pas le score technique. Elle sert a differencier une action qui vient d'apparaitre d'une action qui reste interessante plusieurs observations de suite.

## Tri Final Affiche

Apres confirmation, le worker retrie les resultats pour l'UI.

L'ordre final privilegie :

1. `Confirmed = True` ;
2. `Stability_Score` eleve ;
3. `Score` eleve.

Puis `Display_Rank` est attribue dans cet ordre.

Cela signifie qu'une action avec un score brut legerement plus faible peut apparaitre devant une autre si son signal est confirme et plus stable.

## Donnees Stockees

Le cache principal est :

```text
data/cache/stock_ideas.json
```

Les metadonnees sont dans :

```text
data/cache/stock_ideas_meta.json
```

L'historique est stocke dans :

```text
data/signals.sqlite3
```

Tables :

- `stock_signal_snapshots` : une ligne par ticker score a chaque run ;
- `stock_signal_state` : etat courant par ticker ;
- `stock_signal_outcomes` : ancienne table historique prevue pour des mesures futures ;
- `tracked_signals` : signaux detectes par les deux moteurs, dedupliques par moteur/ticker/jour d'observation ;
- `signal_outcomes` : performances futures des signaux suivis.

Champs importants visibles ou persistants :

- `Ticker` ;
- `Nom` ;
- `Score` ;
- `Raw_Score` ;
- `Age_Penalty` ;
- `Opportunity_Adjustment` ;
- `Setup_Type` ;
- `Confirmed` ;
- `Consecutive_Hits` ;
- `Recent_Top_Hits` ;
- `Stability_Score` ;
- `Signal_Age_Minutes` ;
- `market_region` ;
- `market_session` ;
- `last_market_timestamp` ;
- `last_observed_price` ;
- `new_observation` ;
- `why_selected` ;
- `risk_flags`.

## Suivi Des Signaux

Le suivi des signaux mesure ce que deviennent les idees apres detection.

Il couvre les deux moteurs :

- `standard` : recommandations stables issues de `job_score_stocks()` ;
- `smallcap` : signaux agressifs issus de `job_score_small_caps()`.

Le code principal est dans :

```text
signal_tracking.py
```

### Enregistrement Initial

Apres chaque run de detection, le worker appelle :

```python
register_detected_signals(...)
```

Chaque signal enregistre :

- moteur source ;
- ticker ;
- nom ;
- timestamp de detection ;
- prix de reference ;
- score ;
- setup ;
- `signal_quality` quand disponible ;
- tags en JSON ;
- risque ;
- rang ;
- timestamp de marche ;
- metadonnees utiles.

La regle de deduplication est :

```text
1 signal par moteur + ticker + jour d'observation marche
```

Cela evite de reinserer le meme ticker toutes les 7 minutes pour les small caps ou toutes les 30 minutes pour le moteur standard.

### Tables SQLite

Les nouvelles tables sont dans :

```text
data/signals.sqlite3
```

Table `tracked_signals` :

- `signal_id` ;
- `engine` ;
- `ticker` ;
- `name` ;
- `detected_at` ;
- `observation_key` ;
- `reference_price` ;
- `score` ;
- `setup` ;
- `signal_quality` ;
- `tags_json` ;
- `risk` ;
- `run_id` ;
- `rank` ;
- `market_timestamp` ;
- `metadata_json` ;
- `created_at`.

Table `signal_outcomes` :

- `signal_id` ;
- `engine` ;
- `ticker` ;
- `perf_1d_pct` ;
- `perf_3d_pct` ;
- `perf_5d_pct` ;
- `max_runup_pct` ;
- `max_drawdown_pct` ;
- `days_to_peak` ;
- `days_to_trough` ;
- `last_followup_timestamp` ;
- `followup_complete` ;
- `updated_at`.

### Job De Suivi

Le job dedie est :

```python
job_track_signal_outcomes()
```

Il tourne toutes les 60 minutes.

Il :

1. lit les signaux incomplets ;
2. recupere les historiques journaliers Yahoo Finance ;
3. cherche les bougies disponibles apres la date du signal ;
4. remplit J+1, J+3 et J+5 quand ces horizons existent ;
5. calcule le meilleur run-up et le pire drawdown jusqu'a J+5 ;
6. marque le signal complet quand J+5 est disponible.

Les horizons sont bases sur les bougies journalieres disponibles, pas sur des minutes intraday.

### Interpretation

Dans l'UI, la section `Suivi des signaux` affiche un resume comparatif :

- nombre de signaux suivis ;
- nombre de suivis complets ;
- performance moyenne J+1, J+3, J+5 ;
- taux de signaux positifs a J+1, J+3, J+5 ;
- run-up moyen ;
- drawdown moyen ;
- meilleurs setups recents quand assez de donnees existent.

Au debut, les performances peuvent rester vides : un signal detecte aujourd'hui n'a pas encore de bougie J+1, J+3 ou J+5. Les champs sont remplis progressivement par le worker.

## Pre/After-Market

Le pre-market et l'after-hours sont affichables dans le comparateur, mais ne pilotent pas le scoring principal.

Dans le comparateur, l'option `Pre/after` active :

```text
yfinance.download(..., prepost=True)
```

Le tableau distingue alors :

- `Prix regular` ;
- `Prix affiche` ;
- `Prix pre/post` ;
- `Session` ;
- `Dernier timestamp`.

Pour les recommandations, le choix reste prudent :

- pas de confirmation sur simple mouvement hors seance ;
- pas de score principal base sur pre/after-hours ;
- pas de melange silencieux entre prix regular et hors seance.

Raisons :

- volumes hors seance souvent faibles ;
- trous de donnees Yahoo ;
- latence possible ;
- couverture Europe irreguliere ;
- risque de faux signaux.

## Scanner Small Caps Explosives

Le scanner small caps explosives est volontairement separe du moteur standard.

Fichier principal :

```text
smallcap_scanner.py
```

Fonctions principales :

```python
scan_small_cap_opportunities()
save_smallcap_results()
```

Caches produits :

```text
data/cache/smallcap_ideas.json
data/cache/smallcap_ideas_meta.json
```

### Philosophie

Ce moteur cherche des mouvements speculatifs rapides.

Il privilegie :

- small caps et micro caps US tradables ;
- prix bas a moyen, par defaut environ 1 a 20 USD ;
- capitalisation inferieure a environ 2 milliards USD ;
- variation journaliere forte ;
- volume relatif anormal ;
- breakout 20 jours ou 60 jours ;
- continuation multi-jours ;
- cloture proche du plus haut de la seance ;
- gap haussier ;
- activite inhabituelle pouvant indiquer un catalyseur.

Il ne cherche pas a reproduire le moteur standard.

Il ne demande pas :

- MA50 > MA200 ;
- tendance lineaire propre ;
- stabilite multi-cycles ;
- RSI dans une zone confortable ;
- point d'entree proche de MA20.

Un RSI eleve ou une extension court terme peuvent rester acceptables dans ce moteur, car les runners speculatifs sont souvent deja tendus.

### Univers

Le scanner utilise `yf.screen()` avec :

- `region = us` ;
- exchanges `NMS`, `NYQ`, `ASE` ;
- capitalisation minimum et maximum ;
- prix minimum et maximum ;
- volume journalier minimum ;
- exclusion des instruments speciaux : warrants, rights, units, preferred, ETF, funds, acquisition corp.

Il combine plusieurs tris Yahoo :

- `percentchange` pour les movers ;
- `dayvolume` pour l'activite ;
- `intradaymarketcap` pour completer l'univers small caps.

Si Yahoo ne retourne pas assez de candidats, le scanner ajoute :

- quelques titres issus du cache standard `stock_ideas` quand ils respectent les seuils small caps ;
- une petite watchlist fallback de tickers speculatifs suivables.

### Filtres Minimums

Les filtres par defaut sont :

| Champ | Seuil par defaut |
| --- | ---: |
| Market cap minimum | `30M$` |
| Market cap maximum | `2B$` |
| Prix minimum | `1$` |
| Prix maximum | `20$` |
| Volume moyen minimum | `120K` |
| Volume jour minimum | `200K` |
| Candidats maximum | `220` |
| Resultats maximum | `30` |

Le volume relatif est un critere central.

Un titre avec :

```text
rel_volume < 0.8
```

est exclu. Cela evite de classer en haut des actions qui montent sans participation suffisante.

### Features Calculees

Chaque resultat peut contenir :

- `ticker` ;
- `name` ;
- `price` ;
- `change_pct` ;
- `market_cap` ;
- `volume` ;
- `avg_volume_20d` ;
- `rel_volume` ;
- `rsi_14` ;
- `distance_from_ma20_pct` ;
- `close_vs_day_high` ;
- `volatility` ;
- `setup` ;
- `risk` ;
- `signal_quality` ;
- `comment` ;
- `market_region` ;
- `market_session` ;
- `last_market_timestamp` ;
- `last_observed_price` ;
- `Explosion_Score` ;
- `tags`.

Tags possibles :

- `momentum_confirme` ;
- `speculatif` ;
- `faible_confirmation_volume` ;
- `first_move` ;
- `continuation` ;
- `overextended` ;
- `news_candidate` ;
- `gap_up`.

### Explosion_Score

Le score est borne entre 0 et 10.

Il favorise d'abord la participation en volume.

| Condition volume relatif | Effet |
| --- | ---: |
| `rel_volume >= 5.0` | `+3.6` |
| `rel_volume >= 3.0` | `+3.0` |
| `rel_volume >= 2.0` | `+1.9` |
| `rel_volume >= 1.2` | `+0.7` |
| `rel_volume >= 1.0` | `+0.2` |
| `rel_volume < 1.0` | malus |
| `rel_volume < 0.8` | exclusion |

La variation journaliere compte aussi, mais moins fortement que le volume :

```text
min(max(change_pct, 0), 35) * 0.06
```

Le but est de distinguer :

- une action qui monte ;
- une action qui monte avec vraie participation.

Autres bonus :

| Signal | Effet |
| --- | ---: |
| Cloture tres proche du plus haut du jour | bonus fort |
| Position intraday constructive | bonus |
| Breakout 20 jours | bonus |
| Breakout 60 jours | bonus supplementaire |
| Continuation multi-jours | bonus |
| Premier mouvement fort | bonus |
| Gap haussier | bonus |
| Prix entre 2 et 12 USD | leger bonus |
| `momentum_confirme` | bonus |
| `momentum_confirme` avec breakout ou continuation | bonus supplementaire |

Malus principaux :

| Situation | Effet |
| --- | ---: |
| Volume absolu trop proche du minimum | malus |
| `rel_volume < 1.2` | malus |
| `rel_volume < 1.0` | malus supplementaire |
| Forte hausse avec faible confirmation volume | malus |
| Range intraday extreme sans cloture proche du high | malus |
| Volatilite tres forte sans volume relatif suffisant | malus |
| Extension extreme | leger malus |

### Qualite Du Signal

Le champ `signal_quality` rend la lecture plus directe.

Valeurs principales :

- `momentum_confirme` : variation significative avec participation suffisante ;
- `speculatif` : signal encore exploitable, mais moins confirme ;
- `faible_confirmation_volume` : mouvement haussier avec volume relatif insuffisant.

Le top du classement doit etre domine par les titres avec vraie participation en volume.

Exemples de commentaires generes :

- `breakout avec volume anormal` ;
- `hausse avec participation elevee` ;
- `hausse forte mais volume peu confirme` ;
- `volume relatif faible x...` ;
- `continuation multi-jours` ;
- `gap haussier`.

### Risque

Le niveau de risque reste volontairement plus permissif que le moteur standard.

Valeurs possibles :

- `Speculatif` ;
- `Eleve` ;
- `Tres eleve`.

Un titre peut rester bien classe meme avec un risque eleve si le mouvement est actif et confirme par le volume. Le scanner n'est donc pas une recommandation prudente : c'est un detecteur de runners potentiels.

### Contexte News Small Caps

Le scanner small caps peut enrichir les meilleurs signaux avec un contexte Yahoo Finance leger, sans appel LLM automatique.

Le code est dans :

```text
news_context.py
```

Comportement :

- le worker recupere les dernieres news Yahoo Finance uniquement pour les meilleurs signaux small caps ;
- les news sont mises en cache dans `data/cache/yahoo_news_context.json` ;
- l'expiration par defaut est d'environ 45 minutes ;
- aucun LLM n'est appele par le worker ;
- une heuristique lit seulement les titres et attribue un label court.

Labels possibles :

- `news_confirmed` : catalyseur clair ou theme sectoriel identifiable ;
- `no_clear_news` : pas de news claire dans Yahoo ;
- `earnings` : resultats, guidance ou publication proche ;
- `offering_risk` : risque de dilution/offering/warrant ;
- `rumor_possible` : signal potentiellement lie a une rumeur.

Champs ajoutes aux resultats small caps :

- `smallcap_news_label` ;
- `smallcap_news_display` ;
- `smallcap_news_headlines` ;
- `smallcap_news_summary` ;
- `smallcap_news_adjustment`.

Impact sur le classement :

- `news_confirmed` peut apporter un leger bonus ;
- `earnings` apporte un petit bonus de contexte ;
- `offering_risk` applique un malus ;
- `rumor_possible` applique un petit malus ;
- `no_clear_news` reste neutre ou legerement negatif.

Desactivation possible :

```bash
SMALLCAP_NEWS_CONTEXT_ENABLED=0
```

TTL configurable :

```bash
YAHOO_NEWS_CONTEXT_TTL_MINUTES=45
```

Le bouton `Resume IA` dans Streamlit reste separe : il appelle le LLM uniquement quand l'utilisateur clique, puis reutilise `data/cache/news_context_llm.json` si les titres n'ont pas change.

## Lecture Dans L'UI

Dans `Analyse` > `Valeurs a fort potentiel`, l'interface separe les deux philosophies :

- `Signaux stables` : moteur standard ;
- `Small caps explosives` : scanner momentum agressif.

### Onglet Signaux Stables

Dans l'onglet `Signaux stables`, les champs les plus utiles sont :

- `Confirmed` : signal deja confirme par plusieurs observations utiles ;
- `Score` : qualite technique brute ;
- `Stability_Score` : regularite du signal ;
- `Consecutive_Hits` : cycles utiles consecutifs ;
- `new_observation` : indique si le dernier run a vu une nouvelle bougie/prix utile ;
- `last_market_timestamp` : derniere observation de marche qui sert au score ;
- `Setup_Type` : nature du signal ;
- `why_selected` : raisons positives ;
- `risk_flags` : risques proches.

### Vue Simple

Par defaut, l'interface affiche une vue decisionnelle reduite :

- `#` : rang d'affichage ;
- `Action` : nom + ticker ;
- `Setup` : `breakout`, `trend` ou `pullback` ;
- `Signal` : statut lisible ;
- `Opportunité` : qualite du timing d'entree ;
- `Variation` : variation recente ;
- `Risque` : niveau synthetique ;
- `Pourquoi` : principales raisons positives ;
- `Verdict` : lecture rapide.

`Signal` est derive de `Confirmed`, `Score`, `Age_Penalty`, `Signal_Age_Minutes`, `new_observation` et `Consecutive_Hits`.

Valeurs possibles :

- `Confirmé` ;
- `À surveiller` ;
- `Récent` ;
- `Trop ancien` ;
- `Faible`.

`Opportunité` traduit `Opportunity_Adjustment`, `Score`, `RSI` et `Distance_MA20 (%)` :

- `Très bonne` ;
- `Correcte` ;
- `Moyenne` ;
- `Faible`.

`Risque` resume `risk_flags`, `RSI`, `Distance_MA20 (%)` et `Age_Penalty` :

- `Faible` ;
- `Moyen` ;
- `Élevé`.

`Verdict` sert au tri et aux filtres :

- `Meilleure opportunité` ;
- `Confirmé mais tendu` ;
- `À surveiller` ;
- `Trop tendu` ;
- `Écarter`.

La vue simple trie d'abord par verdict, puis confirmation, opportunite, risque et score.

### Mode Avance

Le toggle `Mode avancé` affiche les colonnes techniques :

- score brut et score final ;
- malus de fraicheur ;
- timing numerique ;
- stabilite ;
- cycles consecutifs ;
- presence dans les tops recents ;
- RSI, MACD, RS/SPY ;
- distance MA20 ;
- timestamp de derniere observation ;
- drapeaux de risque complets.

Ce mode sert au debug et a l'analyse fine. Il n'est pas necessaire pour lire les meilleures opportunites.

### Onglet Small Caps Explosives

Dans l'onglet `Small caps explosives`, les champs les plus utiles sont :

- `Explosion_Score` : score agressif sur 10 ;
- `price` : dernier prix observe ;
- `change_pct` : variation recente ;
- `rel_volume` : volume relatif contre la moyenne 20 jours ;
- `setup` : breakout, continuation ou volume spike ;
- `risk` : niveau de risque speculatif ;
- `signal_quality` : `momentum_confirme`, `speculatif` ou `faible_confirmation_volume` ;
- `comment` : synthese courte du signal ;
- `tags` : drapeaux utiles comme `continuation`, `gap_up`, `news_candidate`, `overextended` ;
- `last_market_timestamp` : derniere observation de marche.

Le mode details small caps affiche aussi :

- capitalisation ;
- volume absolu ;
- volume moyen 20 jours ;
- RSI 14 ;
- distance a MA20 ;
- ratio close / plus haut du jour ;
- volatilite ;
- session de marche ;
- dernier prix observe.

## Interpretation Recommandee

Pour un usage personnel, privilegier :

1. signaux confirmes ;
2. nouvelle observation recente ;
3. stabilite elevee ;
4. risques immediats limites ;
5. setup compatible avec le style recherche ;
6. liquidite suffisante ;
7. contexte news/fondamental verifie dans la fiche entreprise.

Le score doit etre un point de depart, pas une decision automatique.

## Limites

- Le moteur est rule-based : il est explicable, mais il ne predit pas le futur.
- Yahoo Finance peut etre incomplet, surtout sur certaines places europeennes.
- L'univers Europe est local et doit etre maintenu.
- Les jours feries locaux ne sont pas modelises par un calendrier boursier complet.
- Les earnings dependent de la disponibilite Yahoo.
- Les backtests ne sont pas encore calcules automatiquement a chaque run.
