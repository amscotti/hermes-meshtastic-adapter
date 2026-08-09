# Adaptateur Hermes Meshtastic

**Langues :** [English](README.md) · [Español](README.es-ES.md) · [Français](README.fr-FR.md)

`hermes-meshtastic-adapter` est un plugin de plateforme Hermes Agent qui relie Hermes à un maillage LoRa Meshtastic. Il reçoit des messages texte brut des nœuds du maillage, les transmet aux sessions Hermes et renvoie les réponses par LoRa en messages directs ou en diffusions de canal.

<p align="center">
  <img src="assets/demo-meshtastic-chat.jpg" alt="Chatting with the Hermes agent from the Meshtastic phone app, with replies split into numbered chunks and per-message SNR/RSSI" width="300">
  <br>
  <em>Discussion avec l’agent Hermes en LoRa depuis l’application Meshtastic : les longues réponses sont découpées en fragments numérotés, chacun étiqueté avec la qualité de signal en direct.</em>
</p>

Noms publics :

- Dépôt GitHub : `hermes-meshtastic-adapter`
- Nom du plugin Hermes : `meshtastic-platform`
- Nom de la plateforme Hermes : `meshtastic`

## Ce qu’il fait

- Relie les messages texte Meshtastic à Hermes Agent.
- Crée des sessions Hermes distinctes pour les DM de nœuds individuels, par ex. `meshtastic:!da1b1613`.
- Crée des sessions Hermes partagées pour les diffusions de canal, par ex. `meshtastic:channel:0` ou `meshtastic:channel:Primary`.
- Renvoie les réponses Hermes au nœud ou au canal d’origine.
- Découpe les longues réponses en fragments numérotés adaptés au LoRa.
- Expose des outils de maillage pour lister les nœuds, consulter les infos, lire la qualité de signal, envoyer des messages et interroger la télémétrie.
- Stocke l’historique de télémétrie, position et signal dans SQLite.

## Matériel pris en charge

L’adaptateur se connecte à un nœud passerelle en USB série ou en TCP/IP.

- Les cartes ESP32 USB-série telles que Heltec WiFi LoRa 32 V3 sont prises en charge et constituent le matériel de passerelle recommandé.
- Le nœud passerelle doit être alimenté sur secteur ou USB et configuré comme nœud base/client stable.
- Les trackers SenseCAP T1000-E et similaires nRF52 sont d’abord BLE ; leur port USB sert surtout au flash et aux logs série, pas au contrôle fiable. Ils ne sont pas pris en charge en USB série par ce plugin.
- Le support BLE n’est pas inclus en v1.

Réglages recommandés pour le nœud passerelle :

- Rôle : `CLIENT` ou `CLIENT_BASE`.
- Alimentation : USB ou secteur.
- Désactiver le deep sleep et l’économie d’énergie agressive sur le nœud relié à la passerelle.
- Le Bluetooth peut être désactivé après la configuration initiale via l’app Meshtastic.
- La région et le préréglage modem doivent correspondre à votre maillage.
- `LongFast` (ou le préréglage choisi) doit être configuré de façon cohérente sur tous les nœuds.

## Installation

Clonez ou téléchargez le plugin, puis copiez-le dans le répertoire des plugins Hermes :

```bash
git clone https://github.com/amscotti/hermes-meshtastic-adapter
mkdir -p ~/.hermes/plugins/meshtastic
cp -R hermes-meshtastic-adapter/* ~/.hermes/plugins/meshtastic/
```

Installez les dépendances dans l’environnement virtuel Hermes :

```bash
~/.hermes/hermes-agent/venv/bin/python -m pip install -r ~/.hermes/plugins/meshtastic/requirements.txt
```

Activez le plugin :

```bash
hermes plugins enable meshtastic-platform
```

Redémarrez la passerelle Hermes après toute modification des fichiers du plugin ou des variables d’environnement.

### Installation et mises à jour du plugin

Le champ `version` de `plugin.yaml` est purement informatif — `hermes plugins update` fait un `git pull`, donc `main` est le canal de mise à jour. `optional_env` n’apparaît pas dans `hermes config` pour les plugins installés par l’utilisateur ; définissez les variables via `.env` / la config. Avec l’installation par lien symbolique utilisée ici, mettez à jour par le nom du répertoire : `hermes plugins update meshtastic` (pas `meshtastic-platform`).

## Développement

Contributeurs : [`docs/DEVELOPING.md`](docs/DEVELOPING.md) décrit le flux de travail — setup du `.venv` du dépôt, exécution de la suite de tests et des portes (format/lint/types/coverage plus les portes d’architecture légères : complexity / layering / extraction), le test de fumée avec l’interface mock, et une checklist matériel. [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) cartographie les modules, les flux de données et les **règles anti–classe dieu pour les PR assistées par IA**. Lisez les deux avant de modifier le code.

## Configuration

Copiez le modèle fourni et adaptez-le à votre nœud et votre maillage :

```bash
cp .env.example .env
```

Exemple minimal de `.env` :

```env
MESHTASTIC_SERIAL_PORT=/dev/cu.usbserial-0001
MESHTASTIC_ALLOWED_NODES=!da1b1613
MESHTASTIC_HOME_CHANNEL=meshtastic:!da1b1613
MESHTASTIC_CHUNK_BYTES=170
MESHTASTIC_CHUNK_DELAY=4.0
MESHTASTIC_ACK_TIMEOUT=0
```

Variables d’environnement :

| Variable | Obligatoire | Défaut | Description |
| --- | --- | --- | --- |
| `MESHTASTIC_SERIAL_PORT` | Non* | `auto` | Chemin série comme `/dev/cu.usbserial-0001`, ou `auto` pour la découverte. *Configurez soit ceci, soit `MESHTASTIC_TCP_HOST`. |
| `MESHTASTIC_BAUD_RATE` | Non | `115200` | Informatif uniquement — la bibliothèque meshtastic ouvre toujours le port série à 115200. |
| `MESHTASTIC_TCP_HOST` | Non | Aucun | Nom d’hôte ou IP d’un nœud Wi‑Fi/Ethernet. Si défini, l’adaptateur se connecte en TCP plutôt qu’en série. |
| `MESHTASTIC_TCP_PORT` | Non | `4403` | Port de l’API TCP du nœud Meshtastic. |
| `MESHTASTIC_ALLOWED_NODES` | Non | Vide | Liste blanche préférée. IDs de nœuds séparés par des virgules autorisés à parler à Hermes. |
| `MESHTASTIC_ALLOWED_USERS` | Non | Vide | Alias historique de `MESHTASTIC_ALLOWED_NODES`. |
| `MESHTASTIC_ALLOW_ALL_USERS` | Non | `false` | Si true, tout nœud du maillage peut parler à Hermes. À utiliser avec prudence. |
| `MESHTASTIC_ALLOW_CHANNELS` | Non | `false` | Si true, l’agent répond aussi aux messages de **canal/diffusion** (dans le canal partagé). Désactivé par défaut pour ne répondre qu’aux DM et ne pas saturer l’airtime d’un canal public. |
| `MESHTASTIC_HOME_CHANNEL` | Non | Vide | Cible de livraison cron/par défaut, ex. `meshtastic:!da1b1613` ou `meshtastic:channel:0`. Un ID de nœud nu (`!da1b1613` ou `da1b1613`) / valeur `channel:N` est préfixé automatiquement en `meshtastic:` avec un avertissement. |
| `MESHTASTIC_CHUNK_BYTES` | Non | `170` | Max d’octets UTF‑8 par fragment LoRa sortant. `170` est prudent pour la fiabilité multi-sauts et laisse de la marge pour le surcoût DM chiffré (PKI) ; le plafond brut du protocole (et la borne de cette valeur) est `233`. |
| `MESHTASTIC_CHUNK_DELAY` | Non | `4.0` | Délai en secondes entre l’envoi des fragments. |
| `MESHTASTIC_ACK_TIMEOUT` | Non | `0` | Secondes d’attente d’ACK/NACK par fragment sortant. `0` = non bloquant. Mettez `30` pour faire échouer l’envoi sur NAK ou timeout. |
| `MESHTASTIC_SEND_RETRIES` | Non | `0` | Tentatives d’envoi supplémentaires pour les fragments de **message direct** non ACK. `> 0` implique d’attendre l’ACK ; les échecs transitoires (timeout, no-route) sont renvoyés, les permanents (ex. `TOO_LARGE`) non. Les diffusions ne sont jamais réessayées. |
| `MESHTASTIC_RETRY_BACKOFF` | Non | `5.0` | Secondes d’attente entre les nouvelles tentatives. |
| `MESHTASTIC_TELEMETRY_RETENTION_DAYS` | Non | `30` | Âge (jours) à partir duquel les lignes télémétrie/position/signal sont purgées de SQLite. `0` désactive la purge. La purge tourne au plus une fois par heure, de façon paresseuse à l’écriture. |
| `MESHTASTIC_TELEMETRY_MAX_ROWS` | Non | `100000` | Plafond dur de lignes **par table SQLite** (`telemetry` / `positions` / `signal_quality`), les plus récentes d’abord — pas par nœud. En cas d’inondation, un nœud bavard peut chasser les autres. `0` désactive le plafond. La rétention par âge s’applique toujours. |
| `MESHTASTIC_OPEN_TIMEOUT` | Non | `20` | Secondes pour borner l’attente d’ouverture d’interface sur le chemin de succès avant de la traiter comme un échec de connexion (le constructeur continue sur le worker daemon). `0` désactive la borne (attente indéfinie). Utile en série/Wi‑Fi lents où la découverte peut bloquer. |
| `MESHTASTIC_OPEN_CANCEL_TIMEOUT` | Non | `5` | Secondes d’attente pour qu’une ouverture d’interface annulée se termine. `0` abandonne immédiatement l’ouverture annulée. |
| `MESHTASTIC_EXECUTOR_SHUTDOWN_TIMEOUT` | Non | `5` | Secondes d’attente pour que le thread worker de transport draine les jobs de fermeture/liveness pendant la déconnexion. `0` n’attend pas. |
| `MESHTASTIC_MOCK` | Non | `false` | `true` exécute l’adaptateur contre l’interface mock (dry-run, pas de trafic radio réel). Pertinent seulement si la bibliothèque meshtastic est absente ; sinon l’adaptateur ouvre toujours l’interface série/TCP réelle. |
| `MESHTASTIC_AUTOINSTALL` | Non | `true` | Si la bibliothèque meshtastic manque, l’adaptateur lance une fois par processus `pip install -r requirements.txt` dans l’environnement Python de la passerelle (les mises à jour Hermes peuvent effacer les deps du plugin). Mettez `0`/`false` pour désactiver et échouer avec les instructions d’installation. |

## Connexion par IP (TCP)

Les nœuds Wi‑Fi ou Ethernet exposent une API TCP (port par défaut `4403`). Définissez `MESHTASTIC_TCP_HOST` pour vous connecter par le réseau plutôt qu’en USB série :

```env
MESHTASTIC_TCP_HOST=192.168.1.50
MESHTASTIC_TCP_PORT=4403
```

Quand `MESHTASTIC_TCP_HOST` est défini, il prime et la découverte série est ignorée — l’adaptateur n’utilise qu’un transport à la fois. Activez d’abord le Wi‑Fi/Ethernet et l’API réseau sur le nœud via l’app Meshtastic. La reconnexion avec backoff exponentiel et la file sortante fonctionnent comme en série.

## IDs de chat

Les messages directs utilisent des IDs de chat au scope nœud :

```text
meshtastic:!da1b1613
```

Les messages de canal utilisent des IDs de chat au scope groupe :

```text
meshtastic:channel:0
meshtastic:channel:Primary
```

## Outils

Le plugin enregistre ces outils Hermes :

- `mesh_list_nodes` : liste les nœuds visibles et l’état du signal.
- `mesh_node_info` : inspecte un nœud par ID ou nom.
- `mesh_signal_quality` : consulte le SNR/RSSI actuel et récent.
- `mesh_send_dm` : envoie un message direct à un nœud.
- `mesh_send_broadcast` : envoie une diffusion de canal.
- `mesh_telemetry` : lit la télémétrie récente d’un nœud.
- `mesh_telemetry_history` : interroge l’historique télémétrie, position ou signal persisté.
- `mesh_request_telemetry` : demande à un nœud d’envoyer une télémétrie fraîche (requête sollicitée).
- `mesh_request_position` : demande à un nœud sa position actuelle (requête sollicitée).
- `mesh_traceroute` : trace la route vers un nœud, avec SNR par saut dans les deux sens (requête sollicitée).
- `mesh_pause` : met la radio en pause — libère la connexion du nœud passerelle pour que l’app téléphone ou l’UI web puisse l’utiliser (les pauses temporisées se reprennent seules ; plafond `PAUSE_MAX_MINUTES`, 12 h).
- `mesh_resume` : reprend la radio après `mesh_pause`.

### Fraîcheur des nœuds

La bibliothèque meshtastic ne rafraîchit le `lastHeard` d’un nœud qu’à partir des paquets **NodeInfo** périodiques, donc il retarde par rapport aux transmissions réelles. L’adaptateur maintient donc une couche live depuis le flux de paquets : à chaque paquet reçu il met à jour le `last_heard` de l’émetteur (depuis le `rxTime` du paquet) et, pour les paquets directs (0 saut), son `snr`/`rssi` — comme le client officiel Meshtastic. C’est fait pour **chaque** nœud entendu (y compris hors liste blanche, pour surveiller un nœud que vous ne bridgez pas), et `mesh_list_nodes` / `mesh_node_info` / `mesh_signal_quality` rapportent le plus frais entre la valeur bibliothèque et cette couche. `mesh_node_info` renvoie aussi `last_heard` / `last_heard_epoch`.

## Progression des outils (courts libellés, pas de dumps d’étapes)

Quand l’agent utilise des outils (recherche web, terminal, …), Hermes peut émettre des lignes de **progression d’outil**. Sur les plateformes avec édition de messages, elles se mettent à jour sur place ; en LoRa elles deviendraient du trafic radio permanent.

Ce plugin limite l’airtime du maillage :

- La progression est un **court libellé emoji** par outil (ex. `🔍 Searching the web`), pas la requête complète, l’URL ou la commande shell.
- Les « éditions » ultérieures de progression **ne** sont **pas** retransmises à la radio.
- La réponse finale est toujours livrée en entier (fragmentée comme d’habitude).

Configuration d’affichage Hermes recommandée (`~/.hermes/config.yaml`) :

```yaml
display:
  platforms:
    meshtastic:
      tool_progress: new    # un libellé par outil
      streaming: false
```

Les invites d’**approbation** de commandes dangereuses sont distinctes du chrome de progression d’outil et peuvent encore apparaître en messages multilignes plus longs ; répondez par `/approve` (ou le flux d’approbation configuré) quand c’est demandé.

## Sémantique de livraison

La livraison Meshtastic et LoRa est au mieux effort.

- L’adaptateur demande des ACK avec `wantAck=True` pour les paquets sortants.
- L’adaptateur enregistre un callback `onAckNak` et journalise les ACK/NACK par ID de paquet quand Meshtastic les fournit.
- L’adaptateur distingue un ACK **réel** de bout en bout (envoyé par la destination elle-même) d’un ACK **implicite** relayé par un autre nœud (le paquet a atteint le maillage mais la destination n’a pas confirmé) — comme RECEIVED vs DELIVERED du client officiel. Un ACK réel **et** un ACK seulement implicite comptent comme **livrés** (`send_path.classify_ack_outcome`) ; un ACK implicite n’est **jamais** réessayé — le maillage a déjà porté le paquet, donc la non-livraison n’est pas établie. Le coût de garder ouverte la fenêtre d’upgrade vers un ACK réel est qu’une réponse seulement implicite consomme tout le timeout d’ACK avant le retour de l’envoi.
- Par défaut, les envois sont non bloquants : un succès de `sendText()` signifie que la radio locale a accepté le paquet ; les callbacks ACK/NACK ultérieurs sont journalisés s’ils arrivent.
- Définissez `MESHTASTIC_ACK_TIMEOUT=30` ou passez les métadonnées d’envoi `meshtastic_ack_timeout` pour attendre ACK/NACK par fragment. Dans ce mode, NAK et timeouts rendent `SendResult.success` faux.
- Les résultats ACK sont exposés dans `SendResult.raw_response["chunks"][i]["ack"]` pour les envois avec attente, et inspectables plus tard avec `adapter.get_ack_status(packet_id)`.
- Définissez `MESHTASTIC_SEND_RETRIES=3` pour renvoyer automatiquement les fragments de **message direct** non ACK. Un nouvel essai ne se déclenche que sur un échec transitoire (timeout ACK, no-route, max-retransmit) ; les NAK permanents (`TOO_LARGE`, `NO_CHANNEL`, erreurs auth/PKI) ne sont pas réessayés, et les diffusions ne le sont jamais (pas d’ACK par destinataire). Chaque nouvel essai attend `MESHTASTIC_RETRY_BACKOFF` secondes ; le nombre de tentatives par fragment est dans `SendResult.raw_response["chunks"][i]["attempts"]`. Note : si un message a bien été livré mais que l’ACK est perdu, un nouvel essai envoie un doublon.
- Les longues réponses sont découpées et cadencées, mais tout fragment peut encore être perdu par le maillage. Un échec permanent de fragment **interrompt le reste de la séquence** pour les DM et les diffusions (`SendResult.success` est `false` ; les IDs de paquets déjà envoyés sont conservés pour le diagnostic). Cela évite d’inonder le canal partagé après une erreur dure. Un contenu vide ou uniquement des espaces fait échouer l’envoi plutôt qu’un faux succès.

Même avec l’attente d’ACK activée, la livraison reste au mieux effort : le comportement d’ACK dépend de la qualité de route, du firmware du nœud et de l’éveil de la destination.

## Livraison cron

Définissez `MESHTASTIC_HOME_CHANNEL` pour que les jobs cron Hermes livrent leur sortie via Meshtastic.

Exemples :

```env
MESHTASTIC_HOME_CHANNEL=meshtastic:!da1b1613
MESHTASTIC_HOME_CHANNEL=meshtastic:channel:0
```

L’émetteur cron autonome crée une connexion d’adaptateur de courte durée si besoin et désactive la file d’attente pour que les échecs cron soient visibles. Une amélioration future devrait préférer réutiliser l’adaptateur de passerelle déjà connecté quand c’est possible.

## Alimentation et sommeil

Le plugin ne modifie pas les réglages d’alimentation Meshtastic et n’oblige pas les nœuds distants à rester éveillés.

Ce qu’il fait :

- Maintient une connexion USB série ouverte vers le nœud passerelle.
- Exécute des contrôles de reconnexion.
- Vide les messages sortants en file après reconnexion.

Ce qu’il ne fait pas :

- Désactiver le light sleep ou le deep sleep.
- Changer la config d’alimentation Meshtastic.
- Envoyer des paquets radio de keepalive.
- Empêcher les nœuds sur batterie de dormir.

Pour une passerelle/station de base, configurez le comportement d’alimentation sur le nœud lui-même via les réglages Meshtastic.

## Notes de sécurité

- N’activez pas `MESHTASTIC_ALLOW_ALL_USERS=true` sans en comprendre le risque.
- Préférez les listes blanches par nœud et les DM.
- Les envois en diffusion peuvent consommer rapidement l’airtime partagé du maillage.
- De longues réponses d’IA peuvent être malvenues sur des maillages publics ou chargés.
- Les messages du maillage peuvent être interceptés selon la config du canal et le partage de clés.
- N’exposez pas les clés de canal ou privées aux prompts, logs ou outils Hermes.

## Dépannage

### Le plugin utilise une connexion série mock

L’interface mock n’est utilisée que si vous l’activez explicitement ou si la découverte série automatique ne trouve rien :

- **Bibliothèque Meshtastic absente** — l’adaptateur échoue de façon explicite au lieu de faire semblant de fonctionner. Le log de la passerelle affiche un `RuntimeError` avec la commande d’installation, et l’adaptateur tente **une installation automatique** `pip install -r requirements.txt` dans l’interpréteur en cours (`MESHTASTIC_AUTOINSTALL=0` la désactive). Les auto-mises à jour Hermes reconstruisent le venv d’exécution et peuvent supprimer les deps du plugin — après une mise à jour, vérifiez le log pour cette erreur ou des lignes `mock_interface`.
- **`MESHTASTIC_MOCK=1`** — exécute explicitement contre l’interface mock (dry-run ; pas de trafic radio réel) même si la bibliothèque manque.
- **Aucun port série trouvé avec `auto`** — un avertissement est journalisé et l’adaptateur bascule sur `mock_port`. Définissez `MESHTASTIC_SERIAL_PORT` explicitement plutôt que `auto`.

Installation manuelle dans le venv Hermes :

```bash
~/.hermes/hermes-agent/venv/bin/python -m pip install -r ~/.hermes/plugins/meshtastic/requirements.txt
```

### Port série introuvable

- Sous macOS, les ports ressemblent souvent à `/dev/cu.usbserial-*` ou `/dev/cu.usbmodem*`.
- Sous Linux, souvent `/dev/ttyUSB*` ou `/dev/ttyACM*`.
- Installez les pilotes CP210X ou CH34X si votre carte les exige.
- Vérifiez qu’aucun autre client Meshtastic ne détient le port série.

### Les messages directs échouent en silence

Le nœud cible n’a peut‑être pas initialisé les métadonnées de clé publique. Appairez le nœud au moins une fois avec l’application mobile officielle Meshtastic, puis laissez les infos de nœud se propager dans le maillage.

### Les longues réponses manquent des fragments

- Définissez `MESHTASTIC_CHUNK_BYTES=170`.
- Augmentez `MESHTASTIC_CHUNK_DELAY` à `5.0` ou plus.
- Préférez des prompts et réponses plus courts sur des maillages faibles ou multi-sauts.

### Les nœuds sur batterie manquent des messages

Les nœuds en sommeil ou en économie d’énergie peuvent ne pas recevoir tout de suite. Configurez le comportement d’alimentation côté appareil dans Meshtastic.

## Limitations connues

- Les transports USB série et TCP/IP sont pris en charge ; le BLE n’est pas implémenté.
- Série et TCP ne peuvent pas être utilisés en même temps ; définir `MESHTASTIC_TCP_HOST` sélectionne le TCP.
- L’attente ACK/NACK est optionnelle via `MESHTASTIC_ACK_TIMEOUT` ; les envois par défaut sont non bloquants et journalisent les callbacks ACK/NACK ultérieurs.
- La nouvelle tentative de livraison (`MESHTASTIC_SEND_RETRIES`) est opt-in et réservée aux DM ; un ACK perdu sur un message déjà livré provoque un doublon.
- La livraison cron utilise une connexion série de courte durée plutôt que l’adaptateur de passerelle live.
- La file sortante est uniquement en mémoire (bornée à 100, évictions plus anciennes d’abord) ; les messages en file pendant une déconnexion sont perdus si la passerelle redémarre avant le drain.
- Le plugin ne gère ni le sommeil ni les réglages d’alimentation des nœuds.
- Les outils de diffusion doivent être utilisés avec parcimonie pour ne pas gaspiller l’airtime partagé.
