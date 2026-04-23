# Digest stable des actualites

Le podcast quotidien repose maintenant sur un digest intermediaire stable. Le but est de conserver les memes grands sujets pendant une courte periode, meme si les flux RSS bougent legerement.

## Fonctionnement general

1. Les articles RSS sont collectes par rubrique.
2. Les titres sont dedoublonnes.
3. Les contenus sont enrichis avec `trafilatura` quand c'est possible.
4. Les articles sont regroupes en clusters de sujets.
5. Chaque cluster recoit un score deterministe.
6. Les meilleurs clusters deviennent le sujet principal et les sujets secondaires.
7. Le script du podcast est genere a partir du digest fige.

## Score d'importance

Le score d'un cluster combine plusieurs signaux :

- nombre d'articles dans le cluster ;
- nombre de sources distinctes ;
- fraicheur moyenne des articles ;
- reputation des sources ;
- richesse du contenu extrait ;
- rubrique d'origine ;
- mots-cles a fort impact ;
- bonus de continuite si le sujet etait deja central dans le digest precedent.

Les scores ne sont pas des jugements absolus. Ils servent surtout a stabiliser la hierarchie editoriale.

## Sujet principal

Le sujet principal est le cluster avec le meilleur score, sauf si le precedent sujet principal reste tres proche. Dans ce cas, l'application le conserve pour eviter un changement brutal.

Un remplacement est normal si :

- un nouveau sujet a nettement plus de sources ;
- le score depasse largement l'ancien sujet ;
- l'ancien sujet n'est plus present dans les flux recents.

## Digest recent

Le cache `daily_news_digest.json` est reutilise pendant environ 60 minutes. Cela permet de generer plusieurs scripts ou audios proches sans refaire toute la hierarchie.

Depuis l'interface, l'option de reconstruction forcee permet de recalculer le digest quand on veut vraiment une nouvelle selection editoriale.

## Memoire editoriale

Le cache `editorial_state.json` conserve une memoire courte :

- dernier sujet principal ;
- clusters retenus recemment ;
- date du dernier digest ;
- identifiant du digest.

Cette memoire ne bloque pas les nouveaux sujets. Elle donne seulement un bonus de continuite aux sujets qui restent importants.

## Debug

Le cache `news_digest_debug.json` permet d'auditer la selection :

- clusters candidats ;
- score total ;
- raisons principales ;
- clusters retenus ;
- clusters ecartes ;
- raison du choix du sujet principal.

Dans l'interface, un panneau de debug affiche les sujets retenus avec leurs scores et leurs raisons.

## Interpretation

Un bon digest doit privilegier :

- un sujet principal repris par plusieurs sources ;
- quelques sujets secondaires differents ;
- peu de doublons thematiques ;
- des articles de reference clairs et recents.

Le systeme favorise la stabilite. Il peut donc conserver un sujet principal pendant quelques dizaines de minutes meme si un article plus recent arrive, tant que ce nouvel article ne forme pas un sujet nettement plus important.
