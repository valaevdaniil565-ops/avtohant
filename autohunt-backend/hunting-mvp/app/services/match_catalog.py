from __future__ import annotations

from functools import lru_cache
import re
from typing import Any


RAW_MATCH_CATALOG = [
    {
        "role": "Vue Developer",
        "vacancy_aliases": "frontend developer, frontend engineer, web frontend engineer, frontend-разработчик, vue developer, js/ts developer",
        "stack": "Vue / TypeScript / Next.js / JavaScript",
        "stack_aliases": "typescript, ts; javascript, js; next, nextjs, next.js; vue, vuejs, vue.js; nuxt, nuxtjs;",
    },
    {
        "role": "React Developer",
        "vacancy_aliases": "frontend developer, frontend engineer, web frontend engineer, frontend-разработчик, react developer, js/ts developer",
        "stack": "React / TypeScript / Next.js / JavaScript",
        "stack_aliases": "react, reactjs, react.js; typescript, ts; javascript, js; next, nextjs, next.js; nuxt, nuxtjs;",
    },
    {
        "role": "Angular Developer",
        "vacancy_aliases": "frontend developer, frontend engineer, web frontend engineer, frontend-разработчик, angular developer, js/ts developer",
        "stack": "Angular / TypeScript / Next.js / JavaScript",
        "stack_aliases": "typescript, ts; javascript, js; next, nextjs, next.js; nuxt, nuxtjs; angular, angular2+",
    },
    {
        "role": "Backend Python Engineer",
        "vacancy_aliases": "python developer, python backend developer, backend-разработчик python, django developer, fastapi developer, backend, backend developer",
        "stack": "Python / Django / FastAPI / PostgreSQL / Redis / Kubernates / HTTP / Airflow",
        "stack_aliases": "python, py; django; fastapi; flask; postgres, postgresql, pg; redis; celery; rabbitmq; kafka; sql",
    },
    {
        "role": "Backend Java Engineer",
        "vacancy_aliases": "java developer, java backend developer, java engineer, spring developer, backend java, backend, backend developer, kotlin developer",
        "stack": "Java / Spring / Spring Boot / PostgreSQL / Oracle / Rest / SOAP/ kafka / rabbit / mssql / mysql / kotlin",
        "stack_aliases": "java; spring; spring boot; java spring framework; jpa; hibernate; postgres, postgresql; oracle; kafka; Kuber; K8s; kubernates",
    },
    {
        "role": "Backend Node.js Engineer",
        "vacancy_aliases": "node developer, backend developer node.js, nodejs backend, nestjs developer, backend js/ts",
        "stack": "Node.js / NestJS / TypeScript / PostgreSQL / Prisma",
        "stack_aliases": "node, nodejs, node.js, node js; nest, nestjs, nest.js; typescript, ts; prisma; postgres, postgresql; mongodb, mongo; redis; express",
    },
    {
        "role": "Fullstack JS/TS Engineer",
        "vacancy_aliases": "fullstack developer, full-stack developer, fs developer, react+node developer, fullstack js/ts",
        "stack": "React / Next.js + Node.js / NestJS",
        "stack_aliases": "fullstack, full-stack; react + node; react + nest; next + node; typescript fullstack; javascript fullstack",
    },
    {
        "role": "Android Engineer",
        "vacancy_aliases": "android developer, android-разработчик, mobile developer android",
        "stack": "Kotlin / Android SDK / Jetpack / Ktor",
        "stack_aliases": "kotlin; android sdk; jetpack; jetpack compose, compose; retrofit; coroutines; room",
    },
    {
        "role": "iOS Engineer",
        "vacancy_aliases": "ios developer, ios-разработчик, swift developer, mobile developer ios, iphone developer",
        "stack": "Swift / UIKit / SwiftUI / Objective-C",
        "stack_aliases": "swift; swiftui; uikit; objective-c, objc; xcode; cocoapods; spm, swift package manager",
    },
    {
        "role": "AQA Java",
        "vacancy_aliases": "qa automation engineer, automation qa, autotest engineer, sdet, test automation engineer, автотестировщик, aqa, aqa java, автоматизатор тестирования, fullstack тестировщик, full-stack тестировщик, специалист по автотестированию",
        "stack": "Java/JS + Selenium/Playwright/Cypress + API testing / SQLAlchemy / Cucumber / Mokito / Citrus / JUnit / JMeter",
        "stack_aliases": "qa auto; automation qa; autotests, auto tests; selenium; playwright; cypress; pytest; rest assured; postman; api testing, java, cucumber, mokito, citrus, junit, jmeter",
    },
    {
        "role": "AQA Python",
        "vacancy_aliases": "qa automation engineer, automation qa, autotest engineer, sdet, test automation engineer, автотестировщик, aqa, aqa python, автоматизатор тестирования, fullstack тестировщик, full-stack тестировщик, специалист по автотестированию",
        "stack": "Python/JS + Selenium/Playwright/Cypress + API testing / SQLAlchemy / Pytest / Cucumber / Mokito / Citrus / JUnit / JMeter",
        "stack_aliases": "qa auto; automation qa; autotests, auto tests; selenium; playwright; cypress; pytest; rest assured; postman; api testing, python, pytest, cucumber, mokito, citrus, junit, jmeter",
    },
    {
        "role": "QA",
        "vacancy_aliases": "тестировщик, qa, ручной тестировщик, ручное тестирование, тестировщик ПО, специалист по ручному тестированию",
        "stack": "Jira / Redmine / YouTrack / Trello / TestRail / Zephyr / Qase / SQL / DBeaver / Postman / Swagger / cURL / Charles / Fiddler / Kibana / Grafana / Git / HTML / CSS / DevTools / Xcode / ADB / TestFlight",
        "stack_aliases": "jira, redmine, youtrack, trello, testrail, zephyr, qase, sql, dbeaver, postman, swagger, curl, charles, fiddler, kibana, grafana, git, html, css, devtools, xcode, adb, testflight",
    },
    {
        "role": "Нагрузочное тестирование",
        "vacancy_aliases": "нагрузочное тестирование, qa нагрузочное, специалист по нагрузочному тестированию",
        "stack": "JMeter / Oracle / PostgreSQL / MSSQL / MySQL / SQL / Git / Jira",
        "stack_aliases": "jmeter, oracle, postgresql, mssql, mysql, sql, git, jira",
    },
    {
        "role": "DevOps / Системный администратор",
        "vacancy_aliases": "devops engineer, sre engineer, platform engineer, infrastructure engineer, cloud engineer, сис админ, системный администратор, devops",
        "stack": "Linux / Docker / Kubernetes / Terraform / Ansible / CI-CD",
        "stack_aliases": "devops; sre; platform; linux; docker; kubernetes, kuber, k8s; terraform, tf; ansible; helm; jenkins; gitlab ci, ci/cd; prometheus; grafana; oracle, postgresql, postgre",
    },
    {
        "role": "Data Analyst / BI Analyst",
        "vacancy_aliases": "data analyst, аналитик данных, bi analyst, bi developer, sql analyst, reporting analyst, llm",
        "stack": "SQL / Python / Power BI / Tableau / Superset / llm",
        "stack_aliases": "sql; python; power bi, powerbi, ms power bi; tableau; superset; clickhouse; excel; dashboards, дешборды; llm",
    },
    {
        "role": "Data Scientist / ML Engineer",
        "vacancy_aliases": "data scientist, ds, ml engineer, ml developer, research ds, ml scientist",
        "stack": "Python / PyTorch / TensorFlow / SQL / NLP/LLM",
        "stack_aliases": "pytorch; tensorflow; sklearn, scikit-learn; python; sql; nlp; llm; gpt; deep learning; machine learning",
    },
    {
        "role": "Data Engineer / DWH Engineer",
        "vacancy_aliases": "data engineer, dwh engineer, etl developer, analytics engineer, big data engineer, аналитик кхд, кхд, dwh",
        "stack": "SQL / Python / Airflow / Spark / Kafka / ClickHouse",
        "stack_aliases": "data engineer; dwh; etl; pyspark; spark; airflow; kafka; hadoop; clickhouse; postgres, postgresql; dbt",
    },
    {
        "role": "Системный аналитик",
        "vacancy_aliases": "системный аналитик, аналитик, фуллстек аналитик, fullstack analyst, system analyst, СА",
        "stack": "SQL / NoSQL / PosgreSql / Oracle / MySql / MsSql / Redis / MongoDB / Sequence / ER-диаграммы / IDEF / Rest / Soap / Jira / API / User Story / Use Case / UML / BPMN / DrawIO / Figma",
        "stack_aliases": "sql, nosql, posgresql, oracle, mysql, mssql, redis, mongodb, sequence, er-диаграммы, er-diagramm, user story, use case, uml, bpmn, drawio, figma",
    },
    {
        "role": "Техпис",
        "vacancy_aliases": "технический писатель, техпис, тех. писатель, тех. пис., техписец, техрайтер",
        "stack": "VS Code / Markdown / AsciiDoc / HTML / UML / Git / Draw.io / MS Visio / Figma / Confluence / Jira / Jenkins / PostgreSQL",
        "stack_aliases": "vs code, visual studio code, vscode, markdown, asciidoc, html, uml, git, draw.io, drawio, ms visio, microsoft visio, figma, confluence, jira, jenkins, postgresql",
    },
    {
        "role": "Бизнес аналитик",
        "vacancy_aliases": "бизнес аналитик, BA, БА, аналитик, business analyst",
        "stack": "SQL / BPMN / Jira / Confluence / User Stories / Use Cases / REST / JSON / XML",
        "stack_aliases": "sql, bpmn, jira, confluence, user stories, use cases, rest, json, xml",
    },
    {
        "role": "Аналитик DWH",
        "vacancy_aliases": "аналитик dwh, аналитик кхд, кхд, dwh, аналитик корпоративного хранилища данных, DWH analyst",
        "stack": "SQL / PL-SQL / T-SQL / ER / Jira / Confluence",
        "stack_aliases": "sql, pl-sql, psql, tsql, t-sql, er, jira, confluence",
    },
    {
        "role": "Руководитель проектов",
        "vacancy_aliases": "руководитель проектов, РП, менеджер проектов, project manager, technical project manager, delivery manager, agile project manager, руководитель it-проектов",
        "stack": "Jira / Confluence / Trello / Asana / MS Project / Notion / Slack / Miro / Figma / Power BI / Scrum / Kanban / Waterfall / Agile",
        "stack_aliases": "jira, confluence, trello, asana, ms project, msproject, notion, slack, miro, figma, power bi, powerbi, scrum, kanban, waterfall, agile",
    },
]


def _normalize_catalog_text(value: Any) -> str:
    text = str(value or "").lower()
    text = text.replace("&nbsp;", " ")
    text = text.replace("amp;", "")
    text = text.replace("c#", " csharp ")
    text = text.replace(".net", " dotnet ")
    text = text.replace("c++", " cpp ")
    text = re.sub(r"[^a-z0-9а-яё+#./ -]+", " ", text, flags=re.IGNORECASE)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _split_catalog_terms(value: str) -> list[str]:
    text = str(value or "").replace("/", ";").replace("+", ";")
    terms: list[str] = []
    for part in re.split(r"[;,]", text):
        normalized = _normalize_catalog_text(part)
        if normalized:
            terms.append(normalized)
    return terms


@lru_cache(maxsize=1)
def get_match_catalog() -> list[dict[str, Any]]:
    catalog: list[dict[str, Any]] = []
    for index, row in enumerate(RAW_MATCH_CATALOG, start=1):
        role = str(row["role"]).strip()
        vacancy_aliases = _split_catalog_terms(str(row.get("vacancy_aliases") or ""))
        stack_terms = _split_catalog_terms(str(row.get("stack") or ""))
        stack_aliases = _split_catalog_terms(str(row.get("stack_aliases") or ""))
        catalog.append(
            {
                "id": f"profile_{index}",
                "role": role,
                "vacancy_aliases": vacancy_aliases,
                "stack_terms": stack_terms,
                "stack_aliases": stack_aliases,
            }
        )
    return catalog


@lru_cache(maxsize=1)
def _catalog_frequencies() -> dict[str, dict[str, int]]:
    role_freq: dict[str, int] = {}
    stack_freq: dict[str, int] = {}
    for profile in get_match_catalog():
        for term in set(profile["vacancy_aliases"]):
            role_freq[term] = role_freq.get(term, 0) + 1
        for term in set([*profile["stack_terms"], *profile["stack_aliases"]]):
            stack_freq[term] = stack_freq.get(term, 0) + 1
    return {"role": role_freq, "stack": stack_freq}


def _contains_term(haystack: str, term: str) -> bool:
    if not haystack or not term:
        return False
    return f" {term} " in f" {haystack} "


def detect_catalog_profiles(text: str) -> list[dict[str, Any]]:
    haystack = _normalize_catalog_text(text)
    if not haystack:
        return []

    frequencies = _catalog_frequencies()
    scored: list[dict[str, Any]] = []
    for profile in get_match_catalog():
        score = 0.0
        matched_terms: list[str] = []

        for term in profile["vacancy_aliases"]:
            if _contains_term(haystack, term):
                score += 7.0 if frequencies["role"].get(term, 1) == 1 else 2.0
                matched_terms.append(term)

        for term in profile["stack_terms"]:
            if _contains_term(haystack, term):
                score += 4.0 if frequencies["stack"].get(term, 1) == 1 else 1.5
                matched_terms.append(term)

        for term in profile["stack_aliases"]:
            if _contains_term(haystack, term):
                score += 3.0 if frequencies["stack"].get(term, 1) == 1 else 1.0
                matched_terms.append(term)

        if score <= 0:
            continue

        scored.append(
            {
                "id": profile["id"],
                "role": profile["role"],
                "score": round(score, 3),
                "matched_terms": sorted(set(matched_terms)),
            }
        )

    return sorted(scored, key=lambda item: item["score"], reverse=True)


def detect_primary_catalog_profile(text: str) -> dict[str, Any] | None:
    scored = detect_catalog_profiles(text)
    if not scored:
        return None
    best = scored[0]
    second = scored[1] if len(scored) > 1 else None
    if best["score"] < 4:
        return None
    if second and (best["score"] - second["score"]) < 2:
        return None
    return best


def get_catalog_profile_by_id(profile_id: str) -> dict[str, Any] | None:
    for profile in get_match_catalog():
        if profile["id"] == profile_id:
            return profile
    return None
