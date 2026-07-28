# arangodb.nvim

[![CI](https://github.com/LeuciRemi/arangodb.nvim/actions/workflows/ci.yml/badge.svg)](https://github.com/LeuciRemi/arangodb.nvim/actions/workflows/ci.yml)
[![Licence : MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

Parcourez, modifiez et administrez vos données ArangoDB sans quitter Neovim.

[English](README.md) | Français | [`:help arangodb.nvim`](doc/arangodb.nvim.txt)

## Fonctionnalités

- Navigation dans les bases, collections et documents avec `snacks.nvim`.
- Recherche sur les champs échantillonnés avec pages AQL asynchrones basées sur des curseurs.
- Écriture, validation, explication, exécution et profilage AQL dans des buffers dédiés.
- Édition avec `:write` et résolution des conflits concurrents sur `_rev`.
- Création et duplication de documents sous forme de brouillons avant insertion.
- Création, duplication, renommage et troncature asynchrones des collections de documents ou d’arêtes.
- Édition des schémas/propriétés et gestion des index de collection.
- Duplication fidèle des propriétés prises en charge, schémas, valeurs calculées et index secondaires.
- Navigation vers les clés étrangères, relations imbriquées et références inverses détectées.
- Requêtes AQL nommées, fichiers `.aql` réels, résultats tabulaires et exports JSON/CSV/Markdown.
- Exploration bornée des graphes nommés depuis une commande ou un document.
- Résolution différée des mots de passe par callback, variable d’environnement ou commande.
- Connexions HTTP avec le transport Lua intégré, ou HTTPS avec `curl`.
- Diagnostic avec `:checkhealth arangodb`.

## Prérequis

- Neovim `>= 0.10` ; la CI couvre `0.10.4` et la version stable actuelle.
- [`folke/snacks.nvim`](https://github.com/folke/snacks.nvim) avec son picker activé ; la version `2.31.0` est testée.
- `curl` pour HTTPS. Le HTTP simple utilise libuv.
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

## Configuration

```lua
require("arangodb").setup({
  connections = {
    travail = "https://utilisateur:secret@db.example.com:8529/travail",
  },
  default_database = "travail",
  auto_discover = false,
  keymaps = {
    browse = "<leader>ab",
    resume = "<leader>ar",
    back = "<leader>aB",
  },
  document_keymaps = {
    save = nil,
    delete = nil,
    duplicate = nil,
    related = nil,
    graph = nil,
  },
  aql_keymaps = {
    execute = "<leader>ar",
    validate = "<leader>av",
    explain = "<leader>ae",
    profile = "<leader>ap",
    bind_vars = "<leader>ab",
    history = "<leader>ah",
    library = "<leader>al",
    save = "<leader>as",
    cancel = "<leader>ac",
    result_format = nil,
    export = nil,
    next_page = "<C-n>",
    prev_page = "<C-p>",
  },
  aql = {
    batch_size = 100,
    cursor_ttl = 300,
    max_runtime = nil,
    result_split = "auto",
    result_format = "json",
    history = {
      enabled = true,
      max_entries = 100,
      path = nil,
      store_bind_vars = true,
    },
    library = { path = nil },
  },
  graph = { depth = 2, max_nodes = 100, direction = "ANY" },
  layout = { preset = "auto", preview = true },
  page_size = 50,
  field_sample_size = 200,
  cache_ttl = 5000,
  http_timeout = 30000,
  tls_verify = true,
  tls_ca_file = nil,
  diagnostics = {
    enabled = false,
    path = nil,
    max_size = 1048576,
  },
})
```

Toutes les options et leurs valeurs par défaut figurent dans le [README anglais](README.md#configuration) et dans `:help arangodb.nvim`. Une touche peut être désactivée avec `false`. Les raccourcis globaux et les raccourcis d’écriture des pickers sont désactivés par défaut afin de respecter la configuration de chacun et de ne pas associer une mutation à une touche de saisie.

`auto_discover` est volontairement désactivé par défaut. Lorsqu’il est activé, le plugin interroge `/_api/database/user` avec les variables `NVIM_ARANGO_HOST`, port, schéma et identifiants. Aucun accès réseau implicite n’a donc lieu pendant la complétion des commandes ou le healthcheck.

Les métadonnées, champs échantillonnés et métriques utilisent un cache court contrôlé par `cache_ttl` (`0` le désactive). Les aperçus de collection chargent en arrière-plan les métriques de chaque collection ainsi que les totaux de documents et de taille approximative de la base. Le journal de diagnostic facultatif écrit des événements JSONL nettoyés, sans identifiants, headers ou corps de requête. Son chemin par défaut est `stdpath("log") .. "/arangodb.nvim.log"`.

## Connexions et identifiants

Format accepté :

```text
http[s]://[utilisateur:mot_de_passe@]hôte[:port]/base
```

L’authentification est facultative. Les identifiants percent-encodés et IPv6 sont pris en charge :

```lua
connections = {
  sans_auth = "http://127.0.0.1:8529/exemple",
  encoded = "https://user%40example.com:p%40ssword@db.example.com:8529/exemple",
  ipv6 = "http://[::1]:8529/exemple",
}
```

Évitez de versionner vos secrets. Les variables `NVIM_ARANGO_<NOM>_URL` sont détectées automatiquement :

```bash
export NVIM_ARANGO_TRAVAIL_URL='https://lecteur:secret@db.example.com:8529/travail'
```

Un profil structuré garde le mot de passe hors de l’URL et ne le résout qu’à l’ouverture de la connexion. `password` accepte aussi une fonction ; `password_command` accepte une liste d’arguments (recommandée) ou une commande shell, avec un délai maximal de 10 secondes par défaut (`password_command_timeout`) :

```lua
connections = {
  travail = {
    url = "https://db.example.com:8529/travail",
    username = "lecteur",
    password_env = "ARANGODB_TRAVAIL_PASSWORD",
  },
  coffre = {
    url = "https://db.example.com:8529/coffre",
    username = "lecteur",
    password_command = { "security", "find-generic-password", "-w", "-s", "arangodb-coffre" },
  },
  dynamique = {
    url = "http://127.0.0.1:8529/exemple",
    password = function(contexte)
      return charger_secret(contexte.name)
    end,
  },
}
```

Le secret résolu n’est jamais ajouté à la complétion ni au healthcheck. Une commande doit écrire uniquement le mot de passe sur sa sortie standard.

Variables disponibles pour l’auto-découverte :

- `NVIM_ARANGO_HOST` (défaut `127.0.0.1`)
- `NVIM_ARANGO_PORT` (défaut `8529`)
- `NVIM_ARANGO_SCHEME` (`http`, `https`, `ssl` ou `tls`)
- `NVIM_ARANGO_USER` (défaut `root`)
- `NVIM_ARANGO_PASSWORD` (défaut `root`)
- `NVIM_ARANGO_SYSTEM_URL`

Pour une autorité de certification privée, utilisez `tls_ca_file`. Désactiver `tls_verify` est possible mais déconseillé.

## Utilisation

```vim
:ArangoBrowse
:ArangoBrowse ma_base
:ArangoResume
:ArangoBack
:ArangoAql
:ArangoAql ma_base
:ArangoAqlAttach ma_base
:ArangoAqlLibrary ma_base
:ArangoGraph ma_base
```

Dans un buffer de document :

```vim
:write
:ArangoDocumentSave
:ArangoDocumentDuplicate
:ArangoDocumentDelete
:ArangoDocumentRelated
:ArangoDocumentGraph
```

Dans un éditeur ouvert par `:ArangoAql` :

```vim
:ArangoAqlExecute
:ArangoAqlValidate
:ArangoAqlExplain
:ArangoAqlProfile
:ArangoAqlBindVars
:ArangoAqlHistory
:ArangoAqlLibrary
:ArangoAqlSave
:ArangoAqlCancel
```

Les buffers de résultat proposent `:ArangoAqlResultFormat [json|table]` et `:ArangoAqlExport [chemin]` ; l’extension `.json`, `.csv`, `.md` ou `.markdown` choisit le format. L’écriture est atomique, les dossiers parents sont créés si nécessaire et le remplacement d’un fichier existant exige une confirmation explicite. Depuis un vrai fichier `.aql`, `:ArangoAqlAttach [base]` ajoute les mêmes commandes sans transformer le buffer en scratch.

La requête utilise le filetype `aql`. Le buffer JSON non listé associé aux bind variables s’ouvre automatiquement en dessous tandis que le focus reste sur la requête ; sélectionner un autre buffer de requête AQL dans la barre de buffers remplace automatiquement le split associé par les variables de cette session. Les commandes AQL ci-dessus et leurs raccourcis en mode normal sont disponibles depuis les deux buffers et ciblent toujours la requête associée ; les raccourcis sur sélection visuelle restent limités au buffer AQL. Une variable de collection `@@collection` utilise par exemple la clé `"@collection"`. Si l’onglet courant contient déjà une session AQL, `:ArangoAql` ouvre la suivante dans un nouvel onglet plutôt que d’empiler ses splits. Les résultats restent associés à leur session et apparaissent dans un split JSON en lecture seule, à droite sur écran large et en dessous sur écran étroit. Les pages déjà visitées restent en cache local.

L’exécution et le profilage demandent d’abord le plan optimisé à ArangoDB. Une requête dont `plan.isModificationQuery = true` exige une confirmation explicite affichant la base et les collections modifiées. Explain et validation n’exécutent jamais la requête.

L’historique est recherchable avec `snacks.nvim` et stocké par défaut dans `stdpath("data") .. "/arangodb.nvim/aql_history.json"` avec des permissions réservées à l’utilisateur. Il ne contient jamais URL, identifiants, résultats ou erreurs. Les requêtes et bind variables peuvent néanmoins être sensibles ; utilisez `aql.history.enabled = false` ou `store_bind_vars = false` si nécessaire. Après la désactivation de `store_bind_vars`, la prochaine écriture de l’historique supprime aussi les bind variables des entrées conservées.

Les requêtes nommées sont stockées séparément dans `stdpath("data") .. "/arangodb.nvim/aql_library.json"`, par connexion et base, avec des permissions utilisateur. Elles enregistrent les bind variables courantes, qui peuvent contenir des valeurs sensibles. `:ArangoAqlSave` crée ou remplace un nom ; `:ArangoAqlLibrary` charge ou supprime une entrée sans l’exécuter.

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
| `<C-p>` / `<C-n>` | Page de résultat précédente / suivante |

Touches par défaut du picker de collections :

| Touche | Action |
| --- | --- |
| `<Entrée>` | Ouvrir la collection |
| `<C-x>` | Ouvrir le menu d’actions |
| `<C-b>` | Revenir au choix de la base lorsque disponible |

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

La navigation et le menu d’actions sont disponibles en modes normal et insertion. Les raccourcis d’écriture des pickers sont désactivés par défaut ; création, duplication, renommage, suppression et troncature restent disponibles via `<C-x>`. Lorsqu’ils sont configurés explicitement, les raccourcis d’écriture restent limités au mode normal. Les opérations destructives demandent une confirmation qui affiche la base et la ressource ciblées ; la troncature comporte un avertissement d’irréversibilité. Le renommage, la troncature ou une suppression depuis un buffer concerné sont refusés si un buffer ArangoDB correspondant contient des changements non sauvegardés.

Le menu d’actions d’une collection permet aussi de gérer les index et d’éditer en JSON ses propriétés mutables, notamment le schéma de validation. La duplication crée les propriétés et index non système pris en charge avant de copier les documents ; un échec ou une annulation après la création de la cible supprime la collection partielle. Un échec de ce nettoyage est signalé explicitement.

### Permissions ArangoDB nécessaires

N’accordez que les droits requis par les parcours utilisés. La navigation, les lectures AQL et les traversées de graphes exigent un accès en lecture à la base et à chaque collection consultée. Les écritures de documents et mutations AQL exigent un accès en écriture aux collections concernées. La création, le renommage, la troncature ou la duplication de collections ainsi que la modification des propriétés, schémas et index nécessitent les privilèges d’administration de base/collection adaptés au déploiement. Le plugin ne contourne jamais l’autorisation ArangoDB ; les rôles exacts peuvent varier entre serveur unique, cluster et service managé.

La révision `_rev` protège les sauvegardes concurrentes. En cas de conflit, le plugin permet de recharger la version distante, de comparer les deux versions ou de forcer explicitement l’écrasement. Les lectures des pickers sont asynchrones et annulables ; la pagination utilise les curseurs ArangoDB.

## Explorateur de graphes

`:ArangoGraph [base]` liste les graphes nommés, demande un document de départ comme `users/alice`, puis affiche un voisinage borné en largeur. Depuis un document, utilisez `:ArangoDocumentGraph` ou le menu d’actions. Dans le buffer, `<CR>` ouvre le document du sommet, `s` repart du sommet sélectionné, `r` rafraîchit, `d` change la profondeur et `t` alterne `ANY`, `OUTBOUND` et `INBOUND`. Ces touches sont configurables ou désactivables avec `graph_keymaps`. La profondeur est plafonnée à 10 et `graph.max_nodes` borne le résultat.

## Limites

- Les opérations distantes des pickers, documents, collections, métadonnées et graphes sont asynchrones et annulables. Une commande locale de mot de passe est résolue à l’ouverture et peut brièvement bloquer Neovim.
- L’annulation AQL interrompt la requête locale et ferme les curseurs connus. Sans `aql.max_runtime`, une requête déjà lancée côté serveur peut continuer selon sa configuration.
- HTTPS nécessite actuellement `curl`.
- La détection des relations est heuristique.
- L’explorateur de graphes affiche un voisinage textuel borné, pas un canevas orienté par forces.

## Contribution

```bash
make test
make lint
make docs
```

Un test d’intégration facultatif nécessite une base jetable, car il effectue des opérations destructives sur les collections (avec tentative de nettoyage) :

```bash
ARANGODB_TEST_URL=http://127.0.0.1:8529/_system make integration
```

Les issues et pull requests sont les bienvenues. Indiquez vos versions de Neovim, `snacks.nvim` et ArangoDB, une configuration minimale sans secrets, ainsi que les étapes de reproduction. Consultez [CONTRIBUTING.md](CONTRIBUTING.md) pour le processus et [SECURITY.md](SECURITY.md) pour signaler une vulnérabilité en privé.

## Licence

[MIT](LICENSE) © Remi Leuci et les contributeurs.
