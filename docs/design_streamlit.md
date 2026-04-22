# Design Streamlit

Ce document decrit uniquement la partie visuelle de l'application Streamlit : organisation de l'ecran, composants, couleurs, cartes, tableaux et graphiques.

## Impression generale

L'application se presente comme un tableau de bord financier sobre, dense et orienté lecture rapide. Le design repose principalement sur les composants natifs de Streamlit, avec une mise en page large, des colonnes, des metriques, des tableaux interactifs et des graphiques Plotly.

L'ensemble donne une impression d'outil d'analyse marche plutot que de site marketing : beaucoup d'information est visible rapidement, avec une hierarchie simple entre titres, captions, filtres, tableaux et graphiques.

## Structure de page

La page est configuree en mode `wide`, ce qui permet d'occuper toute la largeur disponible. Il n'y a pas de sidebar dans l'interface principale : la navigation et les controles sont integres directement dans le contenu pour garder une interface epuree.

En haut de l'application, un en-tete fixe le contexte :

- un titre principal : `Comparateur Boursier Interactif` ;
- une courte phrase descriptive en caption ;
- trois zones de synthese avec des metriques et une note sur les sources de donnees.

Sous cet en-tete, la navigation principale est affichee sous forme de boutons radio horizontaux :

- `Comparateur` ;
- `Portefeuille` ;
- `Marche du jour` ;
- `Analyse` ;
- `Actualites`.

Cette navigation horizontale donne un aspect de tableau de bord a onglets, sans menu lateral.

## Hierarchie visuelle

La hierarchie est construite avec les styles Streamlit standards :

- `st.title` pour le titre global ;
- `st.subheader` pour les grandes sections ;
- `st.caption` pour les textes secondaires, les sources, les precisions et les avertissements legers ;
- `st.metric` pour les chiffres cles ;
- `st.dataframe` pour les donnees denses ;
- `st.tabs` pour separer des vues proches ;
- `st.expander` pour masquer les blocs avancés ou administratifs.

Les captions jouent un role important : elles expliquent le contexte sans alourdir la page. Visuellement, elles restent plus discretes que les titres et les tableaux.

## Palette graphique

La palette est majoritairement claire, avec un fond blanc et des tons gris bleutes pour les textes secondaires et les bordures.

Couleurs principales utilisees dans les blocs personnalises :

- vert positif : `#15803d` ;
- rouge negatif : `#b91c1c` ;
- jaune/brun attention ou mitige : `#a16207` ;
- texte principal fonce : `#0f172a` ;
- texte secondaire : `#334155`, `#475569`, `#64748b` ;
- bordures claires : `#e5e7eb`, `#e2e8f0` ;
- fond tres clair : `#f8fafc` ;
- fond vert pale : `#f0fdf4` ;
- fond rouge pale : `#fef2f2`.

Le code couleur suit les conventions financieres : le vert indique les hausses ou gains, le rouge indique les baisses ou pertes. Les graphes et tableaux reprennent cette logique pour les performances et le PnL.

## Navigation et filtres

Les controles sont places au plus pres des donnees qu'ils modifient. L'application utilise principalement :

- des `selectbox` pour choisir une periode, un actif, une rubrique ou un type de tri ;
- des `multiselect` pour comparer plusieurs actifs ;
- des `toggle` pour activer des options comme le mode avance ou l'inclusion du pre/post-market ;
- des boutons pleine largeur dans certains formulaires et actions importantes ;
- des onglets pour passer d'une representation a une autre.

Dans le comparateur, les filtres sont alignes en une ligne de colonnes : duree, mode avance, pre/post-market et recherche multi-actifs. Cette disposition donne une lecture compacte et efficace.

## Tableaux

Les tableaux sont un element central du design. Ils sont affiches en pleine largeur avec `st.dataframe`, sans index visible quand ce n'est pas utile.

Les tableaux servent a presenter :

- les vues d'ensemble des actifs ;
- les ratios fondamentaux ;
- les positions de portefeuille ;
- les benchmarks ;
- les recommandations ;
- les statistiques de suivi ;
- les actualites ou journaux techniques.

Certaines colonnes ont un format dedie : pourcentages signes, montants, prix, rangs ou textes courts. Dans le portefeuille, les valeurs de PnL sont colorees en vert ou rouge selon le signe, ce qui facilite la lecture immediate des gains et pertes.

## Graphiques

Les graphiques sont construits avec Plotly et affiches en pleine largeur. Le style utilise le template clair `plotly_white`, avec des marges controlees pour garder les graphiques lisibles.

Les principaux types de graphiques sont :

- courbes de prix ;
- courbes de performance cumulee ;
- courbe d'evolution de la valeur du portefeuille ;
- barres de performance mensuelle ;
- barres de contribution au PnL ;
- graphiques d'allocation.

Les barres positives sont vertes et les barres negatives rouges. Les graphiques restent fonctionnels et sobres, sans decoration excessive.

## Cartes personnalisees

Deux types de cartes personnalisees completent les composants Streamlit natifs.

### Cartes de variations marche

Dans `Marche du jour`, la section `A la une` affiche deux grandes cartes cote a cote :

- `Ca monte fort`, en vert ;
- `Ca baisse fort`, en rouge.

Chaque carte utilise :

- un fond colore tres pale ;
- une bordure de la couleur d'accent ;
- un rayon de bordure important ;
- une ombre douce ;
- un titre en uppercase ;
- une liste de cinq lignes separees par de fines bordures.

Chaque ligne affiche le nom de l'actif, son ticker, son marche, le cours, le volume et la variation en pourcentage. La variation est mise en avant en gras avec la couleur d'accent.

### Cartes d'actualites generales

Les actualites generales sont affichees dans des cartes blanches compactes :

- bordure gris clair ;
- rayon de bordure de 8 px ;
- padding de 12 px ;
- image a gauche ;
- texte a droite ;
- titre en gras ;
- meta-informations en gris ;
- resume en texte secondaire.

Quand une image est indisponible, un bloc placeholder gris clair indique `Image indisponible`, avec bordure et centrage du texte.

## Fiche entreprise

La fiche entreprise est affichee dans un conteneur borde. Elle ressemble a une carte d'analyse :

- nom de l'entreprise en titre ;
- ticker et marche en caption ;
- badge de sentiment a droite ;
- metriques principales sur quatre colonnes ;
- section `Lecture rapide` en trois colonnes ;
- informations sectorielles et analystes ;
- tableau de ratios ;
- expander optionnel pour la comparaison avec les pairs.

Le badge de sentiment est un element visuel distinctif : fond vert, jaune/brun ou rouge selon l'etat, texte blanc, forme arrondie en pilule.

## Portefeuille

La page portefeuille est structuree comme un tableau de bord de suivi :

- formulaire d'achat virtuel dans un expander ;
- quatre metriques de synthese ;
- tableau des lignes de portefeuille ;
- graphique d'evolution ;
- controle de suppression ;
- onglets `Allocation`, `Contribution`, `Benchmark`.

Visuellement, cette page met beaucoup l'accent sur les chiffres. Les metriques donnent le resume, puis les tableaux et graphiques permettent d'analyser plus finement.

## Actualites

La page actualites est separee en deux onglets :

- `Infos generales` ;
- `News marche`.

Les infos generales ont le rendu le plus editorial de l'application grace aux cartes avec image, titre, source, flux et resume. Les news marche sont plus simples : titre en sous-section, caption de contexte, resume puis lien.

Les controles de tri et de filtrage sont places avant les listes pour garder une logique de lecture descendante : choisir, filtrer, lire.

## Analyse et recommandations

La page analyse est plus proche d'un cockpit de screening :

- onglets pour separer les signaux stables et les small caps explosives ;
- filtres en ligne ;
- tableaux denses avec scores, rangs, setups et risques ;
- captions pour expliquer le regime de marche ;
- suivi des performances par moteur.

Le design privilégie ici la densite et la comparaison rapide. Les couleurs restent sobres, et la lisibilite vient surtout des tableaux structures.

## Authentification et administration

Les pages d'acces, de connexion et d'administration utilisent le style Streamlit standard :

- titres simples ;
- formulaires ;
- captions explicatives ;
- metriques d'administration ;
- tableaux de logs ;
- expanders pour les actions secondaires.

Ces ecrans sont moins personnalises graphiquement que les pages finance, mais restent coherents avec l'ensemble de l'application.

## Responsive et lisibilite

La mise en page repose sur les colonnes Streamlit. Sur grand ecran, l'application affiche plusieurs blocs cote a cote : metriques, filtres, cartes, graphiques et tableaux. Sur ecran plus petit, Streamlit reflow automatiquement les colonnes, ce qui conserve une interface lisible meme si elle devient plus verticale.

Les contenus importants utilisent la largeur complete quand necessaire, notamment les tableaux et graphiques. Les elements plus contextuels sont places dans des colonnes ou des expanders pour eviter de surcharger l'ecran.

## Style global

Graphiquement, l'application peut etre resumee ainsi :

- interface claire et fonctionnelle ;
- design financier sobre ;
- forte presence de tableaux et de metriques ;
- navigation horizontale sans sidebar ;
- couleurs semantiques vert/rouge pour la performance ;
- cartes personnalisees pour les mouvements de marche et les actualites ;
- graphiques Plotly blancs, lisibles et peu decoratifs ;
- experience orientee analyse, suivi et prise d'information rapide.

