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
- Création, duplication, renommage et troncature des collections de documents ou d’arêtes.
- Navigation vers les clés étrangères, relations imbriquées et références inverses détectées.
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
  },
  aql_keymaps = {
    execute = "<leader>ar",
    validate = "<leader>av",
    explain = "<leader>ae",
    profile = "<leader>ap",
    bind_vars = "<leader>ab",
    history = "<leader>ah",
    cancel = "<leader>ac",
    next_page = "<C-n>",
    prev_page = "<C-p>",
  },
  aql = {
    batch_size = 100,
    cursor_ttl = 300,
    max_runtime = nil,
    result_split = "auto",
    history = {
      enabled = true,
      max_entries = 100,
      path = nil,
      store_bind_vars = true,
    },
  },
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

Toutes les options et leurs valeurs par défaut figurent dans le [README anglais](README.md#configuration) et dans `:help arangodb.nvim`. Une touche peut être désactivée avec `false`. Les raccourcis globaux sont désactivés par défaut afin de respecter la configuration de chacun.

`auto_discover` est volontairement désactivé par défaut. Lorsqu’il est activé, le plugin interroge `/_api/database/user` avec les variables `NVIM_ARANGO_HOST`, port, schéma et identifiants. Aucun accès réseau implicite n’a donc lieu pendant la complétion des commandes ou le healthcheck.

Les métadonnées, champs échantillonnés et métriques utilisent un cache court contrôlé par `cache_ttl` (`0` le désactive). Le journal de diagnostic facultatif écrit des événements JSONL nettoyés, sans identifiants, headers ou corps de requête. Son chemin par défaut est `stdpath("log") .. "/arangodb.nvim.log"`.

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
```

Dans un buffer de document :

```vim
:write
:ArangoDocumentSave
:ArangoDocumentDuplicate
:ArangoDocumentDelete
:ArangoDocumentRelated
```

Dans un éditeur ouvert par `:ArangoAql` :

```vim
:ArangoAqlExecute
:ArangoAqlValidate
:ArangoAqlExplain
:ArangoAqlProfile
:ArangoAqlBindVars
:ArangoAqlHistory
:ArangoAqlCancel
```

La requête utilise le filetype `aql`. Le buffer JSON non listé associé aux bind variables s’ouvre automatiquement en dessous tandis que le focus reste sur la requête ; sélectionner un autre buffer de requête AQL dans la barre de buffers remplace automatiquement le split associé par les variables de cette session. Les commandes AQL ci-dessus et leurs raccourcis en mode normal sont disponibles depuis les deux buffers et ciblent toujours la requête associée ; les raccourcis sur sélection visuelle restent limités au buffer AQL. Une variable de collection `@@collection` utilise par exemple la clé `"@collection"`. Si l’onglet courant contient déjà une session AQL, `:ArangoAql` ouvre la suivante dans un nouvel onglet plutôt que d’empiler ses splits. Les résultats restent associés à leur session et apparaissent dans un split JSON en lecture seule, à droite sur écran large et en dessous sur écran étroit. Les pages déjà visitées restent en cache local.

L’exécution et le profilage demandent d’abord le plan optimisé à ArangoDB. Une requête dont `plan.isModificationQuery = true` exige une confirmation explicite affichant la base et les collections modifiées. Explain et validation n’exécutent jamais la requête.

L’historique est recherchable avec `snacks.nvim` et stocké par défaut dans `stdpath("data") .. "/arangodb.nvim/aql_history.json"` avec des permissions réservées à l’utilisateur. Il ne contient jamais URL, identifiants, résultats ou erreurs. Les requêtes et bind variables peuvent néanmoins être sensibles ; utilisez `aql.history.enabled = false` ou `store_bind_vars = false` si nécessaire. Après la désactivation de `store_bind_vars`, la prochaine écriture de l’historique supprime aussi les bind variables des entrées conservées.

Raccourcis par défaut dans les buffers AQL :

| Touche | Action |
| --- | --- |
| `<leader>ar` | Exécuter la requête ou la sélection visuelle |
| `<leader>av` | Valider sans exécution |
| `<leader>ae` | Expliquer sans exécution |
| `<leader>ap` | Exécuter avec profilage |
| `<leader>ab` | Modifier les bind variables |
| `<leader>ah` | Parcourir l’historique local |
| `<leader>ac` | Annuler et fermer le curseur actif |
| `<C-p>` / `<C-n>` | Page de résultat précédente / suivante |

Touches par défaut du picker de collections :

| Touche | Action |
| --- | --- |
| `<Entrée>` | Ouvrir la collection |
| `<C-a>` | Créer un brouillon de document |
| `<C-n>` | Créer une collection |
| `<C-d>` | Dupliquer la collection |
| `<C-r>` | Renommer la collection |
| `<C-t>` | Vider la collection |
| `<C-x>` | Ouvrir le menu d’actions |
| `<C-b>` | Revenir au choix de la base lorsque disponible |

Touches par défaut du picker de documents :

| Touche | Action |
| --- | --- |
| `<Entrée>` | Ouvrir le document |
| `<C-a>` | Créer un brouillon |
| `<C-y>` | Dupliquer comme brouillon |
| `<C-d>` | Supprimer le document |
| `<C-o>` | Parcourir les relations détectées |
| `<C-f>` | Changer le champ de recherche |
| `<C-u>` | Réinitialiser la recherche |
| `<C-p>` / `<C-n>` | Page précédente / suivante |
| `<C-t>` | Vider la collection |
| `<C-x>` | Ouvrir le menu d’actions |
| `<C-b>` | Revenir en arrière |

Les opérations destructives demandent confirmation. Le renommage ou la troncature d’une collection est refusé si un buffer ArangoDB correspondant contient des changements non sauvegardés.

La révision `_rev` protège les sauvegardes concurrentes. En cas de conflit, le plugin permet de recharger la version distante, de comparer les deux versions ou de forcer explicitement l’écrasement. Les lectures des pickers sont asynchrones et annulables ; la pagination utilise les curseurs ArangoDB.

## Limites

- Les lectures des pickers sont asynchrones ; les commandes de mutation attendent encore la réponse du serveur.
- L’annulation AQL interrompt la requête locale et ferme les curseurs connus. Sans `aql.max_runtime`, une requête déjà lancée côté serveur peut continuer selon sa configuration.
- HTTPS nécessite actuellement `curl`.
- La détection des relations est heuristique.
- La duplication d’une collection copie ses documents et son type, mais pas ses index, schémas, valeurs calculées ou autres propriétés.

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
