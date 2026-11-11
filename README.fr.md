# arangodb.nvim

[![CI](https://github.com/LeuciRemi/arangodb.nvim/actions/workflows/ci.yml/badge.svg)](https://github.com/LeuciRemi/arangodb.nvim/actions/workflows/ci.yml)
[![Licence : MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

Parcourez, modifiez et administrez vos données ArangoDB sans quitter Neovim.

[English](README.md) | Français | [`:help arangodb.nvim`](doc/arangodb.nvim.txt)

![Navigation dans les utilisateurs, édition et sauvegarde JSON, puis requête AQL et résultats tabulaires](doc/assets/demo.gif)

Enregistré avec un conteneur ArangoDB local et des données fictives. [Reproduire la démo](demo/README.md).

[Installation](#installation-avec-lazynvim) · [Démarrage rapide](#démarrage-rapide) · [Configuration](#configuration) · [Connexions](#connexions-et-identifiants) · [Utilisation](#utilisation) · [Exemple AQL](#exemple-aql) · [Dépannage](#dépannage) · [Contribution](#contribution)

## Fonctionnalités

- Navigation et recherche dans les bases, collections et documents avec `snacks.nvim`.
- Édition JSON avec `:write`, gestion des conflits de révision et création de brouillons.
- Administration des collections, schémas, propriétés et index, y compris la suppression d’une collection.
- Écriture, validation, explication, profilage et sauvegarde AQL ; export de la page affichée ou de tout le curseur.
- Complétion des mots-clés, fonctions et collections AQL ; les erreurs de syntaxe apparaissent comme diagnostics.
- Navigation entre documents liés et exploration des graphes nommés.
- Connexions HTTP ou HTTPS, avec résolution différée des mots de passe.

## Prérequis

- Neovim `>= 0.10` ; la CI couvre `0.10.4` et la version stable actuelle.
- [`folke/snacks.nvim`](https://github.com/folke/snacks.nvim) avec son picker activé ; la version `2.31.0` est testée.
- `curl` pour HTTPS, et pour les connexions `http://` lorsqu’un proxy HTTP est configuré. Sans proxy, le HTTP simple utilise libuv.
- Un serveur ArangoDB accessible par son API HTTP. Les tests d’intégration couvrent ArangoDB `3.12`.

## Installation avec lazy.nvim

```lua
{
  "LeuciRemi/arangodb.nvim",
  dependencies = {
    { "folke/snacks.nvim", opts = { picker = { enabled = true } } },
  },
  opts = {
    connections = {
      local_db = "http://root:mot_de_passe@127.0.0.1:8529/ma_base",
    },
    default_database = "local_db",
  },
}
```

## Démarrage rapide

Après installation, une connexion suffit ; la configuration détaillée ci-dessous est facultative.

1. Configurez `connections` et `default_database` comme dans l’exemple d’installation, avec votre serveur et une base existante. `local_db` est un alias de connexion ; `ma_base` dans l’URL est le nom réel de la base.
2. Lancez `:checkhealth arangodb` pour vérifier les dépendances locales et les connexions détectées. Ce contrôle ne teste pas l’authentification ni les droits de lecture sur le serveur.
3. Lancez `:ArangoBrowse local_db`, sélectionnez une collection avec `<Entrée>`, puis ouvrez un document avec `<Entrée>`.
4. Modifiez le JSON et lancez `:write` pour sauvegarder dans ArangoDB. Dans un picker, `<C-x>` donne accès aux actions, notamment la création d’un brouillon et la gestion des index.
5. Utilisez `:ArangoResume` pour reprendre la navigation ou `:ArangoAql local_db` pour ouvrir un éditeur de requêtes.

Pour essayer avec une base jetable déjà remplie, suivez [la démo Docker](demo/README.md) et utilisez l’alias `demo`.

## Configuration

Appelez `setup()` même avec des connexions fournies par l’environnement. Ne renseignez que les options à personnaliser. `false` désactive un raccourci ; les raccourcis globaux, ceux d’écriture des pickers et l’auto-découverte sont désactivés par défaut.

<details>
<summary>Exemple de personnalisation</summary>

```lua
require("arangodb").setup({
  page_size = 50,
  http_timeout = 30000, -- millisecondes
  keymaps = {
    browse = "<leader>aB",
    resume = "<leader>aR",
    back = "<leader>aP",
  },
})
```

Les majuscules distinguent ces raccourcis globaux des raccourcis locaux AQL. `page_size` contrôle les pages du navigateur, `aql.batch_size` celles de l’éditeur AQL et `aql_batch_size` les lots des autres opérations du client. Les touches optionnelles `picker_keymaps.drop_collection`, `picker_keymaps.sort` et `picker_keymaps.open_aql` sont désactivées par défaut ; les actions restent accessibles par `<C-x>`.

</details>

Les [valeurs par défaut complètes](README.md#configuration) et [`:help arangodb.nvim-options`](doc/arangodb.nvim.txt) détaillent les options, leurs unités, le cache et les diagnostics.

## Connexions et identifiants

Format accepté :

```text
http[s]://[utilisateur:mot_de_passe@]hôte[:port]/base
```

Utilisez un profil structuré pour garder le mot de passe hors de la configuration :

```lua
connections = {
  travail = {
    url = "https://db.example.com:8529/travail",
    username = "lecteur",
    password_env = "ARANGODB_TRAVAIL_PASSWORD",
  },
}
```

Exportez `ARANGODB_TRAVAIL_PASSWORD` avant de lancer Neovim. Autre possibilité : `NVIM_ARANGO_<NOM>_URL` fournit une URL complète ; la connexion porte alors le nom de la base dans l’URL.

Une URL impossible à analyser, par exemple sans `http://` ou `https://`, provoque une erreur dans `setup()` et nomme la connexion concernée. Elle n’est jamais utilisée pour une requête.

Les sources sont lues dans cet ordre : `setup().connections`, `vim.g.arangodb_connections`, puis les URL d’environnement. La première source est prioritaire pour un nom donné. L’authentification est facultative ; les identifiants percent-encodés et les hôtes IPv6 entre crochets sont pris en charge.

Les connexions `http://` passent par le transport intégré, sauf si `http_proxy` (en minuscules uniquement, curl ignorant `HTTP_PROXY`), `all_proxy` ou `ALL_PROXY` est défini et non vide et que `curl` est installé. Elles passent alors par curl, qui applique le proxy et `no_proxy` comme pour `https://`. `tls_verify` et `tls_ca_file` ne concernent que `https://`. `:checkhealth arangodb` indique si le HTTP simple passe par un proxy et avertit si curl manque pour ce transport.

Pour les callbacks, commandes de mot de passe et variables de découverte, consultez [`:help arangodb.nvim-connections`](doc/arangodb.nvim.txt). Utilisez `tls_ca_file` pour une autorité privée et conservez `tls_verify = true`.

## Utilisation

| Commande | Action |
| --- | --- |
| `:ArangoBrowse [base]` | Ouvrir une connexion avec son alias configuré |
| `:ArangoResume` / `:ArangoBack` | Reprendre la navigation (en rouvrant la dernière vue collections ou collection si le picker a été fermé) / revenir en arrière |
| `:ArangoAql [base]` | Ouvrir un éditeur AQL |
| `:ArangoAqlAttach [base]` | Ajouter les outils AQL à un fichier `.aql` existant |
| `:ArangoAqlLibrary [base]` | Rechercher et charger les requêtes sauvegardées dans un picker |
| `:ArangoGraph [base]` | Explorer un graphe nommé |

Les arguments de base sont complétés à partir des connexions configurées et des noms de bases découverts plus tôt dans la session. La complétion n’interroge jamais le serveur.

Dans un document, `:write` sauvegarde dans ArangoDB. `:ArangoDocumentDuplicate`, `:ArangoDocumentDelete`, `:ArangoDocumentRelated` et `:ArangoDocumentGraph` donnent accès aux actions du document. En cas de conflit de révision, la sauvegarde propose de recharger, comparer ou forcer explicitement l’écrasement.

Les modifications faites pendant une suppression ou un vidage de collection sont conservées dans un buffer de récupération. Utilisez `:write <fichier>` pour enregistrer une copie locale ou `:ArangoDocumentDuplicate` pour récupérer son contenu dans un nouveau brouillon.

Une requête AQL s’ouvre avec un buffer JSON pour ses variables. Les commandes et raccourcis normaux fonctionnent dans les deux buffers ; les sélections visuelles concernent uniquement la requête. `:ArangoAqlExecute` et `:ArangoAqlProfile` n’envoient la requête d’explication et ne demandent confirmation que si la requête contient `INSERT`, `UPDATE`, `REPLACE`, `REMOVE` ou `UPSERT` (quelle que soit la casse) ; les autres requêtes s’exécutent directement. La validation et l’explication n’exécutent jamais la requête.

Les buffers AQL complètent les mots-clés (`k`), les fonctions intégrées d’ArangoDB 3.12 (`f`, insérées sans parenthèses) et les collections de la base de la session (`c`) via `omnifunc`. Les collections se chargent en arrière-plan : la première complétion peut ne proposer que les mots-clés et les fonctions. Les commentaires utilisent `//`. Une erreur de syntaxe signalée par le serveur devient un diagnostic `ERROR` à la position indiquée dans le buffer de requête ; il est effacé à la requête suivante ou après un succès. Ces diagnostics Neovim sont indépendants du journal `diagnostics` des requêtes.

Charger l’historique ou une requête nommée dans un fichier `.aql` attaché le laisse marqué comme modifié et demande confirmation avant de remplacer des éditions non sauvegardées. Les réponses Explain et Validate restent en JSON lorsque le format tableau est sélectionné.

L’historique et les requêtes nommées sont conservés localement, avec leurs variables. Leurs pickers offrent la recherche et un aperçu et nécessitent `snacks.nvim`, sans repli sur `vim.ui.select` ; `<C-x>` ouvre un menu d’actions : Restore, Delete entry et Clear history (avec confirmation) pour l’historique, Load ou Delete (avec confirmation) pour les requêtes nommées. Enregistrer une requête nommée sous un nom existant pour la même connexion et la même base demande confirmation. Un fichier d’historique ou de bibliothèque corrompu est renommé en `<path>.corrupt-<timestamp>` avec un avertissement avant l’écriture d’un nouveau fichier. Les fichiers d’historique et de requêtes nommées peuvent être partagés par plusieurs instances de Neovim : chaque écriture prend un fichier de verrou exclusif `<path>.lock`, réessaie pendant une seconde au plus et reprend un verrou de plus de dix secondes. Une entrée d’historique qui ne peut pas être enregistrée à temps est ignorée avec un avertissement, sans empêcher la requête. Pour des requêtes sensibles, désactivez `aql.history.enabled` ou `aql.history.store_bind_vars` et évitez d’enregistrer des valeurs sensibles dans la bibliothèque. [`:help arangodb.nvim-aql`](doc/arangodb.nvim.txt) détaille le stockage, les sessions et les commandes.

Raccourcis par défaut dans les buffers AQL :

| Touche | Action |
| --- | --- |
| `<leader>ar` | Exécuter la requête ou la sélection visuelle |
| `<leader>av` | Valider sans exécution |
| `<leader>ae` | Expliquer sans exécution |
| `<leader>ap` | Exécuter avec profilage |
| `<leader>ab` | Modifier les bind variables |
| `<leader>ah` | Parcourir l’historique local |
| `<leader>al` | Parcourir les requêtes nommées |
| `<leader>as` | Enregistrer la requête par nom |
| `<leader>ac` | Annuler et fermer le curseur actif |

Les pages de résultat appartiennent au buffer de résultat en lecture seule : `<C-p>` / `<C-n>` (ou `:ArangoAqlPrevPage` / `:ArangoAqlNextPage`) y parcourent les pages en cache. Ces deux touches viennent de `aql_keymaps` mais ne sont définies que dans le buffer de résultat.

Dans les pickers, `<Entrée>` ouvre la sélection, `<C-x>` ses actions et `<C-b>` revient en arrière lorsque possible.

Touches par défaut du picker de documents :

| Touche | Action |
| --- | --- |
| `<Entrée>` | Ouvrir le document |
| `<C-o>` | Parcourir les relations détectées |
| `<C-f>` | Changer le champ de recherche |
| `<C-u>` | Réinitialiser la recherche |
| `<C-p>` / `<C-n>` | Page précédente / suivante |
| `<C-x>` | Ouvrir le menu d’actions |
| `<C-b>` | Revenir en arrière |

La navigation fonctionne en modes normal et insertion. `<C-x>` donne accès à la création, la duplication, aux schémas, aux index, à la suppression de collection et aux actions destructives. Les raccourcis d’écriture configurés explicitement restent limités au mode normal. Les opérations destructives demandent confirmation, des garde-fous protègent les buffers modifiés, et la suppression d’une collection refuse les collections système puis demande confirmation en nommant la collection et, lorsqu’il est connu, son nombre de documents. `picker_keymaps.drop_collection` ajoute une touche dédiée.

La création d’une collection demande son nom et son type, puis propose *Create* ou *Create with properties…*. La seconde option ouvre un buffer JSON pré-rempli avec `numberOfShards = 1`, `replicationFactor = 1`, `writeConcern = 1`, `shardKeys = ["_key"]` et `waitForSync = false` ; modifiez-le et lancez `:write` pour créer la collection. Les propriétés acceptées sont listées dans [`:help arangodb.nvim-mappings`](doc/arangodb.nvim.txt). Une erreur serveur laisse le buffer ouvert avec son contenu ; en cas de succès il est fermé et la liste des collections se rouvre filtrée sur le nouveau nom.

Dans une vue de collection, le menu d’actions propose aussi *Sort by field…* (chemin d’attribut, puis Ascending, Descending ou Default order) et *Open in AQL* (ouvre l’éditeur AQL pré-rempli avec la requête équivalente et ses variables). Le tri apparaît dans le titre et est conservé entre les pages, `:ArangoBack` et `:ArangoResume` ; le changer repart de la page 1. Ces entrées n’existent que dans les vues de collection, pas dans les vues de relations. `picker_keymaps.sort` et `picker_keymaps.open_aql` ajoutent une touche dédiée ; ils sont désactivés par défaut.

ArangoDB applique les permissions : accès en lecture pour les consultations, en écriture pour les mutations et privilèges correspondants pour administrer collections et index. [`:help arangodb.nvim-development`](doc/arangodb.nvim.txt) détaille les permissions et les limites de duplication.

## Exemple AQL

Avec [la base de démonstration](demo/README.md), ouvrez `:ArangoAql demo` et saisissez :

```aql
FOR user IN @@collection
  FILTER user.active == @active
  SORT user.name
  RETURN { name: user.name, role: user.role }
```

Dans le buffer JSON associé (`:ArangoAqlBindVars`), saisissez :

```json
{
  "@collection": "users",
  "active": true
}
```

Lancez `:write` dans ce buffer pour valider le JSON, puis `:ArangoAqlExecute`. La variable de collection `@@collection` correspond à la clé JSON `"@collection"` ; la variable de valeur `@active` correspond à `"active"`.

Après initialisation des données, le tableau `result` contient :

```json
[
  { "name": "Alice Martin", "role": "Engineer" },
  { "name": "Ben Taylor", "role": "Designer" },
  { "name": "Chloe Dubois", "role": "Engineer" }
]
```

Le GIF remplace d’abord le rôle d’Alice par `Maintainer` : la requête affiche donc cette valeur sauvegardée. Dans le buffer de résultat, `:ArangoAqlResultFormat table` donne une vue compacte. `:ArangoAqlExport /tmp/active-users.csv` exporte la page affichée ; `:ArangoAqlExport!` récupère d’abord les pages restantes du curseur et exporte toutes les lignes dans l’ordre. Cet export est asynchrone, s’annule avec `:ArangoAqlCancel`, laisse la page affichée inchangée et n’écrit aucun fichier si une page échoue.

## Explorateur de graphes

Lancez `:ArangoGraph [base]` et choisissez un graphe et un document de départ, ou utilisez `:ArangoDocumentGraph` depuis un document. L’exploration est bornée en profondeur et en nombre de sommets ; lorsque le graphe contient plus de sommets que `graph.max_nodes`, l’en-tête affiche `Limit: N (truncated — raise graph.max_nodes)`. Voir [`:help arangodb.nvim-graph`](doc/arangodb.nvim.txt) pour les touches et limites.

## API Lua

```lua
require("arangodb").setup(opts)
require("arangodb").browse({ database = "travail" })
require("arangodb").aql({ database = "travail", query = "RETURN 1" })
require("arangodb").aql({
  database = "travail",
  query = "FOR user IN @@collection FILTER user.active == @active RETURN user.name",
  bind_vars = { ["@collection"] = "users", active = true },
})
require("arangodb").aql_attach({ database = "travail" })
require("arangodb").aql_library({ database = "travail" })
require("arangodb").graph({ database = "travail", graph = "social", start = "users/alice" })
require("arangodb").resume()
require("arangodb").back()
```

`aql()` pré-remplit l’éditeur : `query` définit le contenu du buffer de requête et `bind_vars` celui du buffer JSON associé. Les variables de collection utilisent une clé de la forme `@nom`, par exemple `["@collection"] = "users"` ; les variables de valeur gardent leur nom simple.

## Contrôle de santé

`:checkhealth arangodb` vérifie les dépendances locales et les connexions détectées sans afficher les mots de passe. Il ne teste pas les autorisations côté serveur.

## Dépannage

| Symptôme | Vérification |
| --- | --- |
| Aucune connexion proposée | Appelez `setup()`, vérifiez `connections` ou exportez `NVIM_ARANGO_<NOM>_URL` avant de lancer Neovim. L’auto-découverte est désactivée par défaut. |
| Mauvaise base ou mauvais serveur | Utilisez l’alias configuré, par exemple `:ArangoBrowse local_db`. Un nom inconnu construit une URL avec les paramètres `NVIM_ARANGO_HOST`. Les connexions d’environnement portent le nom de la base dans l’URL, pas celui de la variable. |
| Connexion refusée ou délai dépassé | Vérifiez l’hôte, le port, le serveur/conteneur et l’accès réseau. La démo utilise le port `18529`. |
| Erreur d’authentification ou de droits | Vérifiez `username`, le fournisseur de mot de passe et les droits sur la base et les collections. Le healthcheck ne teste pas les autorisations serveur. |
| Erreur de certificat HTTPS | Installez `curl` et configurez `tls_ca_file` pour une autorité privée, en conservant la vérification TLS. |
| Le picker ne s’ouvre pas | Lancez `:checkhealth arangodb`, vérifiez que Snacks est chargé et que son `picker` est activé. |
| Champ ou relation absent | La découverte est échantillonnée et heuristique ; vérifiez `field_sample_size` et `max_field_depth`, ou utilisez une requête AQL explicite. |

Pour signaler un bug, joignez `:messages` et les versions demandées dans [CONTRIBUTING.md](CONTRIBUTING.md). Relisez le journal `diagnostics` avant partage : il exclut les identifiants et les corps de requête, mais contient les noms d’hôtes et les chemins HTTP.

## Limites

- L’annulation AQL ferme les curseurs connus, mais une requête serveur en cours peut continuer ; utilisez `aql.max_runtime` pour borner son exécution.
- Les relations sont détectées par échantillonnage et heuristiques ; les graphes sont affichés sous forme de voisinages textuels bornés.
- Une commande externe de mot de passe peut brièvement bloquer Neovim.

## Contribution

`make lint` vérifie le formatage et lance les tests Neovim ; `make docs` régénère les tags de l’aide. Les tests d’intégration nécessitent une base ArangoDB jetable.

Consultez [CONTRIBUTING.md](CONTRIBUTING.md) pour contribuer ou signaler un bug, [la démo Docker](demo/README.md) pour les données fictives et l’enregistrement du GIF, et [SECURITY.md](SECURITY.md) pour les vulnérabilités.

## Licence

[MIT](LICENSE) © Remi Leuci et les contributeurs.
