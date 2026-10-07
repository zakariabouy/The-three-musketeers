# Contributing

Ce document décrit les conventions de nommage et de contribution du projet **The Three Musketeers**. Merci de les respecter pour garder un dépôt cohérent et lisible.

## Table des matières

1. [Branches](#1-branches)
2. [Commits](#2-commits)
3. [Python](#3-python)
4. [Dossiers](#4-dossiers)
5. [Vérification automatique](#5-vérification-automatique)

---

## 1. Branches

Format : `<type>/<description-courte>` en minuscules, mots séparés par des tirets.

| Préfixe | Usage | Exemple |
|---|---|---|
| `feature/<fonctionnalite>` | Nouvelle fonctionnalité | `feature/kafka-healthcheck` |
| `fix/<bug>` | Correction de bug | `fix/crlf-entrypoint` |
| `test/<fonctionnalite>` | Ajout ou modification de tests | `test/e2e-cassandra` |
| `refactor/<fonctionnalite>` | Refactorisation sans changement de comportement | `refactor/compose-profiles` |
| `docs/<documentation>` | Documentation | `docs/contributing` |

Règles :
- La branche `main` est protégée : on ne pousse jamais directement dessus.
- Une branche = un sujet. On la supprime après le merge.
- Si l'équipe utilise un outil de tickets, on peut ajouter l'identifiant : `feature/PROJ-123-kafka-healthcheck`.

---

## 2. Commits

Format : [Conventional Commits](https://www.conventionalcommits.org/) : `<type>: <description à l'impératif>`.

| Type | Usage |
|---|---|
| `feat:` | Nouvelle fonctionnalité |
| `fix:` | Correction de bug |
| `refactor:` | Refactorisation |
| `test:` | Tests |
| `docs:` | Documentation |
| `chore:` | Maintenance (dépendances, config, CI) |

Exemples :

```text
feat: add kafka-init service to create topics
fix: force LF line endings on shell scripts
docs: add CONTRIBUTING.md
```

Règles :
- Première ligne : 72 caractères maximum, sans point final.
- Un commit = un changement logique.

---

## 3. Python

Le code suit [PEP 8](https://peps.python.org/pep-0008/).

| Élément | Convention | Exemple |
|---|---|---|
| Variables | `snake_case` | `batch_size` |
| Fonctions | `snake_case` | `to_contract()` |
| Classes | `PascalCase` | `UserProducer` |
| Constantes | `UPPER_SNAKE_CASE` | `KAFKA_BOOTSTRAP_SERVERS` |
| Fichiers | `snake_case.py` | `api_client.py` |

---

## 4. Dossiers

Règle générale : **minuscules, sans espaces, sans accents**.

| Type de dossier | Convention | Exemple |
|---|---|---|
| Dossiers généraux | `kebab-case` | `learning-log` |
| Packages Python (importables) | `snake_case` | `ingestion/src/ingestion` |

Un package Python devient un nom de module (`import mon_package`) : le tiret n'est pas valide, d'où le `snake_case`.

---

## 5. Vérification automatique

Les conventions Python sont vérifiées par `ruff` en CI (règle `N` = `pep8-naming`), configurée dans `pyproject.toml` :

```toml
[tool.ruff.lint]
select = ["E", "F", "I", "N"]
```

Lancez `make lint` avant de pousser vos changements.