
import React from "react";
import {
  benchItems,
  dashboardStats,
  inboxItems,
  logs,
  matches,
  navItems,
  vacancies
} from "./data";
import {
  API_BASE_URL,
  fetchImportJob,
  fetchRecentImportJobs,
  fetchMatches,
  fetchOwnBenchStatus,
  fetchSpecialists,
  fetchVacancies,
  submitFileImport,
  submitTextImport,
  submitUrlImport
} from "./api";

const iconPaths = {
  grid: "M4 4h7v7H4zM13 4h7v7h-7zM4 13h7v7H4zM13 13h7v7h-7z",
  inbox: "M4 5h16v10l-2 5H6l-2-5zM4 13h16",
  briefcase: "M8 6V4h8v2M4 8h16v10H4z",
  users: "M9 11a3 3 0 1 0 0-6a3 3 0 0 0 0 6zm6 1a2.5 2.5 0 1 0 0-5a2.5 2.5 0 0 0 0 5zM4 20a5 5 0 0 1 10 0M13 20a4 4 0 0 1 7 0",
  upload: "M12 16V5M8 9l4-4l4 4M5 19h14",
  link: "M10 14l4-4M7 17H6a4 4 0 1 1 0-8h3M17 7h1a4 4 0 1 1 0 8h-3",
  bolt: "M13 2L4 14h6l-1 8l9-12h-6z",
  wave: "M3 12h4l2-6l4 12l2-6h6",
  gear: "M12 3l1 2.2l2.4.6l-.5 2.4l1.7 1.7l-1.7 1.7l.5 2.4l-2.4.6L12 21l-1-2.2l-2.4-.6l.5-2.4L7.4 14l1.7-1.7l-.5-2.4l2.4-.6zM12 9a3 3 0 1 0 0 6a3 3 0 0 0 0-6z"
};

const initialTheme = "light";
const initialCollections = {
  inbox: inboxItems,
  vacancies,
  bench: benchItems,
  matches,
  logs
};
const emptyCollections = {
  inbox: [],
  vacancies: [],
  bench: [],
  matches: [],
  logs: []
};
const defaultSettings = {
  ttlDefault: "30",
  ttlLongTerm: "90",
  matchThreshold: "50"
};
const exampleTexts = {
  vacancy: [
    "Senior Java Developer",
    "Стек: Java 17, Spring Boot, Kafka, PostgreSQL.",
    "Формат: офис/гибрид, Москва.",
    "До 450 000 ₽."
  ].join("\n"),
  bench: [
    "Senior Python Developer",
    "Опыт: 6 лет.",
    "Стек: Python, Django, FastAPI, PostgreSQL.",
    "Локация: удаленно, ставка 400 000 ₽."
  ].join("\n")
};

const keywordStopwords = new Set([
  "это",
  "для",
  "или",
  "как",
  "что",
  "где",
  "при",
  "без",
  "под",
  "над",
  "из",
  "на",
  "по",
  "до",
  "от",
  "вы",
  "мы",
  "they",
  "with",
  "from",
  "that",
  "this",
  "your",
  "have",
  "has",
  "the",
  "and",
  "опыт",
  "лет",
  "год",
  "года"
]);

function normalizeText(value) {
  return String(value ?? "")
    .toLowerCase()
    .replace(/\s+/g, " ")
    .trim();
}

function itemContainsFilters(item, filters) {
  const haystack = normalizeText(JSON.stringify(item));
  return filters.every((filter) => !filter || haystack.includes(filter));
}

function extractKeywords(value) {
  return normalizeText(value)
    .replace(/[^\p{L}\p{N}+#.]/gu, " ")
    .split(" ")
    .filter((word) => word.length > 2)
    .filter((word) => !keywordStopwords.has(word));
}

function buildRunCorpus(mode) {
  if (mode === "bench") {
    return benchItems.map((item) => ({
      id: item[0],
      title: item[0],
      subtitle: `${item[2]} • ${item[4]}`,
      meta: item[3],
      tags: item[1],
      kindLabel: "Бенч",
      searchableText: [item[0], item[1].join(" "), item[2], item[3], item[4], item[5], item[6]].join(" ")
    }));
  }

  return vacancies.map((item) => ({
    id: item[0],
    title: item[0],
    subtitle: `${item[2]} • ${item[4]}`,
    meta: item[3],
    tags: item[1],
    kindLabel: "Вакансия",
    searchableText: [item[0], item[1].join(" "), item[2], item[3], item[4], item[5]].join(" ")
  }));
}

function buildManualRunResult(queryText, mode) {
  if (!queryText.trim()) {
    return {
      title: "Нет текста для анализа",
      description: "Вставьте описание вакансии или бенча, чтобы получить релевантную выдачу.",
      items: []
    };
  }

  const queryKeywords = extractKeywords(queryText);
  if (!queryKeywords.length) {
    return {
      title: "Ключевые слова не выделены",
      description: "Добавьте больше деталей: стек, грейд, домен, локацию, формат работы и ставку.",
      items: []
    };
  }

  const querySet = new Set(queryKeywords);
  const ranked = buildRunCorpus(mode)
    .map((item) => {
      const targetKeywords = extractKeywords(item.searchableText);
      const targetSet = new Set(targetKeywords);
      const overlap = [...querySet].filter((word) => targetSet.has(word));
      const score = targetSet.size
        ? Math.round((overlap.length / querySet.size) * 70 + (overlap.length / targetSet.size) * 30)
        : 0;

      return {
        ...item,
        score,
        overlap
      };
    })
    .filter((item) => item.score > 0)
    .sort((a, b) => b.score - a.score)
    .slice(0, 5);

  return {
    title: mode === "bench" ? "Топ бенчей" : "Топ вакансий",
    description: ranked.length
      ? "Показаны самые релевантные записи по ключевым словам."
      : "Подходящих записей по текущему тексту не найдено.",
    items: ranked
  };
}

function buildActivitySeries(seed, modifier) {
  return [0, 0, 0, 0, 0, 0, 0].map((_, index) => Math.max(0, Math.round(seed * modifier[index])));
}

function splitTags(value) {
  return String(value ?? "")
    .split(/[,\n]/)
    .map((item) => item.trim())
    .filter(Boolean);
}

function getTodayLabel() {
  return new Intl.DateTimeFormat("ru-RU", {
    day: "2-digit",
    month: "short",
    year: "numeric"
  }).format(new Date());
}

function formatDateLabel(value) {
  if (!value) return "—";
  try {
    return new Intl.DateTimeFormat("ru-RU", {
      day: "2-digit",
      month: "short",
      year: "numeric"
    }).format(new Date(value));
  } catch (error) {
    return String(value);
  }
}

function formatDateTimeLabel(value) {
  if (!value) return "—";
  try {
    return new Intl.DateTimeFormat("ru-RU", {
      day: "2-digit",
      month: "short",
      year: "numeric",
      hour: "2-digit",
      minute: "2-digit"
    }).format(new Date(value));
  } catch (error) {
    return String(value);
  }
}

function formatRate(item) {
  const parts = [item.rate_min, item.rate_max].filter((value) => value !== null && value !== undefined);
  if (!parts.length) return "—";
  if (parts.length === 1) return `${parts[0]} ${item.currency || ""}`.trim();
  return `${parts[0]}-${parts[1]} ${item.currency || ""}`.trim();
}

function mapVacancyItem(item) {
  return [
    item.company || item.role || "Не указан",
    item.stack || [],
    item.grade || "—",
    formatRate(item),
    item.status || "active",
    formatDateLabel(item.created_at),
    item
  ];
}

function mapSpecialistItem(item) {
  return [
    item.role || "Не указан",
    item.stack || [],
    item.grade || "—",
    formatRate(item),
    item.location || "Не указано",
    item.status || "active",
    formatDateLabel(item.created_at),
    item
  ];
}

function mapMatchItem(item) {
  return {
    score: Math.round(Number(item.similarity_score || 0) * 100),
    title: `${item.vacancy_role || "Вакансия"} ↔ ${item.specialist_role || "Специалист"}`,
    vacancy: { company: item.vacancy_role || "Вакансия", stack: [], grade: "—", rate: "—" },
    candidate: { name: item.specialist_role || "Специалист", stack: [], grade: "—", rate: "—" },
    status: "На проверке",
    date: `Найдено: ${formatDateTimeLabel(item.created_at)}`,
    raw: item
  };
}

function createVacancyEntry(form) {
  return [
    form.company.trim() || "Не указан",
    splitTags(form.stack),
    form.grade.trim() || "Middle",
    form.rate.trim() || "—",
    form.status.trim() || "Активно",
    getTodayLabel()
  ];
}

function createBenchEntry(form) {
  return [
    form.name.trim() || "Не указан",
    splitTags(form.stack),
    form.grade.trim() || "Middle",
    form.rate.trim() || "—",
    form.location.trim() || "Не указано",
    form.status.trim() || "Активно",
    getTodayLabel()
  ];
}

function App() {
  const [path, setPath] = React.useState(window.location.pathname);
  const [theme, setTheme] = React.useState(initialTheme);
  const [sourceCollections, setSourceCollections] = React.useState(initialCollections);
  const [collections, setCollections] = React.useState(initialCollections);
  const [runText, setRunText] = React.useState("");
  const [runMode, setRunMode] = React.useState("bench");
  const [runResult, setRunResult] = React.useState(null);
  const [matchesSortOrder, setMatchesSortOrder] = React.useState("desc");
  const [settings, setSettings] = React.useState(defaultSettings);
  const [settingsTheme, setSettingsTheme] = React.useState(initialTheme);
  const [apiState, setApiState] = React.useState({
    status: "idle",
    message: "Подключение к backend не проверялось.",
    lastSync: null,
    ownBench: null
  });

  React.useEffect(() => {
    const onChange = () => setPath(window.location.pathname);
    window.addEventListener("popstate", onChange);
    return () => window.removeEventListener("popstate", onChange);
  }, []);

  React.useEffect(() => {
    document.documentElement.dataset.theme = theme;
    document.documentElement.style.colorScheme = theme;
  }, [theme]);

  const loadBackendData = React.useCallback(async () => {
    setApiState((prev) => ({ ...prev, status: "loading", message: "Обновляю данные из backend..." }));
    try {
      const [vacanciesResponse, specialistsResponse, matchesResponse, ownBenchResponse] = await Promise.all([
        fetchVacancies(),
        fetchSpecialists(),
        fetchMatches(),
        fetchOwnBenchStatus()
      ]);

      const nextCollections = {
        inbox: inboxItems,
        vacancies: (vacanciesResponse.items || []).map(mapVacancyItem),
        bench: (specialistsResponse.items || []).map(mapSpecialistItem),
        matches: (matchesResponse.items || []).map(mapMatchItem),
        logs
      };

      setSourceCollections(nextCollections);
      setCollections(nextCollections);
      setApiState({
        status: "online",
        message: "Frontend подключён к live backend API.",
        lastSync: new Date().toISOString(),
        ownBench: ownBenchResponse.sync
      });
    } catch (error) {
      setSourceCollections(initialCollections);
      setCollections(initialCollections);
      setApiState({
        status: "offline",
        message: `Backend недоступен: ${error.message}. Показаны fallback-данные.`,
        lastSync: null,
        ownBench: null
      });
    }
  }, []);

  React.useEffect(() => {
    loadBackendData();
  }, [loadBackendData]);

  const current = navItems.find((item) => item.path === path) ?? navItems[0];
  const sortedMatches = React.useMemo(() => {
    const copy = [...collections.matches];
    copy.sort((a, b) => (matchesSortOrder === "desc" ? b.score - a.score : a.score - b.score));
    return copy;
  }, [collections.matches, matchesSortOrder]);

  const navigate = (nextPath) => {
    if (nextPath === window.location.pathname) return;
    window.history.pushState({}, "", nextPath);
    setPath(nextPath);
  };

  const clearAllResults = React.useCallback(() => {
    setSourceCollections(emptyCollections);
    setCollections(emptyCollections);
    setRunResult(null);
  }, []);

  const applyTheme = (nextTheme) => {
    setTheme(nextTheme);
    setSettingsTheme(nextTheme);
  };

  const dashboardStatsView = dashboardStats.map((stat, index) => ({
    ...stat,
    value: String([collections.inbox.length, collections.vacancies.length, collections.bench.length, collections.matches.length][index] ?? 0)
  }));

  const statsByPage = {
    inbox: collections.inbox.length,
    vacancies: collections.vacancies.length,
    bench: collections.bench.length,
    matches: collections.matches.length,
    logs: collections.logs.length
  };
  const activityLabels = ["25.03", "26.03", "27.03", "28.03", "29.03", "30.03", "31.03"];
  const benchActivity = buildActivitySeries(collections.bench.length, [0.4, 0.6, 0.8, 0.7, 1, 0.5, 0.9]);
  const vacancyActivity = buildActivitySeries(collections.vacancies.length, [0.6, 0.5, 0.7, 0.9, 0.8, 0.6, 1]);

  const handlePageRefresh = (pageName) => {
    if (pageName === "run") {
      setRunResult(null);
    } else if (["vacancies", "bench", "matches", "inbox"].includes(pageName)) {
      loadBackendData();
    } else {
      setCollections((prev) => ({ ...prev, [pageName]: sourceCollections[pageName] ?? [] }));
    }
  };

  const handleSearch = (pageName, rawFilters) => {
    const filters = rawFilters.map(normalizeText).filter(Boolean);
    const source = sourceCollections[pageName];

    if (!source) {
      return;
    }

    const filtered = filters.length ? source.filter((item) => itemContainsFilters(item, filters)) : source;
    setCollections((prev) => ({ ...prev, [pageName]: filtered }));
  };

  const handleManualCreate = (kind, payload) => {
    const collectionKey = kind === "vacancy" ? "vacancies" : "bench";
    const nextEntry = kind === "vacancy" ? createVacancyEntry(payload) : createBenchEntry(payload);

    setSourceCollections((prev) => {
      const nextItems = [nextEntry, ...prev[collectionKey]];
      setCollections((current) => ({ ...current, [collectionKey]: nextItems }));
      return { ...prev, [collectionKey]: nextItems };
    });
  };

  const handleRunProcess = () => {
    const result = buildManualRunResult(runText, runMode);
    setRunResult(result);
  };

  const handleMatchAction = (action, matchTitle) => {
    setCollections((prev) => ({
      ...prev,
      matches: prev.matches.filter((item) => item.title !== matchTitle)
    }));
  };

  const handleSettingsChange = (key, value) => {
    setSettings((prev) => ({ ...prev, [key]: value }));
  };

  const handleSettingsSave = () => {
    setTheme(settingsTheme);
  };

  const handleSettingsReset = () => {
    setSettings(defaultSettings);
    setSettingsTheme(initialTheme);
    setTheme(initialTheme);
  };

  const handleRowAction = (kind, label) => {
    return { kind, label };
  };

  return (
    <div className="app-shell">
      <aside className="sidebar">
        <div className="brand">
          <div className="brand__mark">⚡</div>
          <div>
            <div className="brand__title">Matcher</div>
            <div className="brand__subtitle">Recruiting Platform</div>
          </div>
        </div>

        <nav className="menu">
          {navItems.map((item) => (
            <button
              key={item.id}
              className={`menu__item ${current.id === item.id ? "is-active" : ""}`}
              type="button"
              onClick={() => navigate(item.path)}
            >
              <Icon name={item.icon} />
              <span>{item.label}</span>
            </button>
          ))}
        </nav>

        <div className="sidebar__bottom">
          <div className="theme-toggle">
            <span>Тема</span>
            <button type="button" onClick={() => applyTheme(theme === "dark" ? "light" : "dark")}>
              {theme === "dark" ? "☾" : "☀"}
            </button>
          </div>

          <div className="profile">
            <div className="profile__avatar">Д</div>
            <div>
              <div className="profile__name">Дмитрий Петров</div>
              <div className="profile__role">Admin</div>
            </div>
          </div>

          <button
            className="logout"
            type="button"
            onClick={() => {
              clearAllResults();
              navigate("/");
            }}
          >
            ⌫ Выйти
          </button>
        </div>
      </aside>

      <main className="content">
        <TopBar
          apiState={apiState}
          onClear={clearAllResults}
          onToggleTheme={() => applyTheme(theme === "dark" ? "light" : "dark")}
          onRefresh={loadBackendData}
        />
        {current.id === "dashboard" && (
          <DashboardPage
            navigate={navigate}
            stats={dashboardStatsView}
            matches={sortedMatches}
            inboxCount={statsByPage.inbox}
            logsCount={statsByPage.logs}
            benchActivity={benchActivity}
            vacancyActivity={vacancyActivity}
            activityLabels={activityLabels}
          />
        )}
        {current.id === "inbox" && (
          <InboxPage
            items={collections.inbox}
            total={statsByPage.inbox}
            onRefresh={() => handlePageRefresh("inbox")}
            onSearch={(filters) => handleSearch("inbox", filters)}
            onOpen={(label) => handleRowAction("inbox", label)}
          />
        )}
        {current.id === "vacancies" && (
          <VacanciesPage
            items={collections.vacancies}
            total={statsByPage.vacancies}
            onRefresh={() => handlePageRefresh("vacancies")}
            onSearch={(filters) => handleSearch("vacancies", filters)}
            onOpen={(label) => handleRowAction("vacancies", label)}
          />
        )}
        {current.id === "bench" && (
          <BenchPage
            items={collections.bench}
            total={statsByPage.bench}
            onRefresh={() => handlePageRefresh("bench")}
            onSearch={(filters) => handleSearch("bench", filters)}
            onOpen={(label) => handleRowAction("bench", label)}
          />
        )}
        {current.id === "manual-upload" && (
          <ManualUploadPage
            onComplete={loadBackendData}
            apiState={apiState}
          />
        )}
        {current.id === "matches" && (
          <MatchesPage
            items={sortedMatches}
            sortOrder={matchesSortOrder}
            onSortChange={setMatchesSortOrder}
            onRefresh={() => handlePageRefresh("matches")}
            onAction={handleMatchAction}
          />
        )}
        {current.id === "run" && (
          <RunPage
            mode={runMode}
            value={runText}
            result={runResult}
            onModeChange={setRunMode}
            onChange={setRunText}
            onClear={() => {
              setRunText("");
              setRunResult(null);
            }}
            onProcess={handleRunProcess}
          />
        )}
        {current.id === "logs" && (
          <LogsPage
            items={collections.logs}
            total={statsByPage.logs}
            onRefresh={() => handlePageRefresh("logs")}
            onSearch={(filters) => handleSearch("logs", filters)}
            onOpen={(label) => handleRowAction("logs", label)}
          />
        )}
        {current.id === "settings" && (
          <SettingsPage
            settings={settings}
            theme={settingsTheme}
            apiState={apiState}
            onThemeChange={setSettingsTheme}
            onSettingsChange={handleSettingsChange}
            onSave={handleSettingsSave}
            onReset={handleSettingsReset}
          />
        )}
      </main>
    </div>
  );
}

function TopBar({ apiState, onClear, onToggleTheme, onRefresh }) {
  return (
    <header className="topbar topbar--stacked">
      <div className="topbar__row">
        <div className="topbar__left topbar__status">
          <Badge tone={apiState.status === "online" ? "green" : apiState.status === "loading" ? "blue" : "red"}>
            {apiState.status === "online" ? "API online" : apiState.status === "loading" ? "Обновление..." : "API offline"}
          </Badge>
          <span className="topbar__status-text">{apiState.message}</span>
        </div>
        <div className="topbar__actions">
          <button type="button" className="icon-button" onClick={onRefresh} title="Обновить live данные">
            ↻
          </button>
          <button type="button" className="icon-button" onClick={onClear} title="Очистить результаты">
            ✕
          </button>
          <button type="button" className="icon-button" onClick={onToggleTheme} title="Переключить тему">
            ◐
          </button>
        </div>
      </div>
    </header>
  );
}

function DashboardPage({ navigate, stats, matches, inboxCount, logsCount, benchActivity, vacancyActivity, activityLabels }) {
  return (
    <section className="page">
      <PageHeading
        title="Дашборд"
        subtitle="Обзор активности системы рекрутинга"
        actionLabel="Открыть входящие"
        onAction={() => navigate("/inbox")}
      />

      <div className="stat-grid">
        {stats.map((stat) => (
          <article key={stat.label} className={`stat-card stat-card--${stat.accent}`}>
            <div className="stat-card__value">{stat.value}</div>
            <div className="stat-card__label">{stat.label}</div>
          </article>
        ))}
      </div>

      <div className="micro-stat-grid">
        <article className="micro-stat-card">
          <span className="micro-stat-card__icon">◫</span>
          <div>
            <div className="micro-stat-card__label">Активных чатов</div>
            <strong>{inboxCount}</strong>
          </div>
        </article>
        <article className="micro-stat-card">
          <span className="micro-stat-card__icon">◌</span>
          <div>
            <div className="micro-stat-card__label">Ожидают проверки</div>
            <strong>{matches.length}</strong>
          </div>
        </article>
        <article className="micro-stat-card">
          <span className="micro-stat-card__icon">△</span>
          <div>
            <div className="micro-stat-card__label">Логов обработки</div>
            <strong>{logsCount}</strong>
          </div>
        </article>
      </div>

      <Panel title="Активность за неделю" subtitle="">
        <ActivityChart labels={activityLabels} benchValues={benchActivity} vacancyValues={vacancyActivity} />
      </Panel>

      <div className="dashboard-grid">
        <Panel title="Последние совпадения" subtitle="Новые релевантные пары">
          {matches.length ? (
            <div className="compact-list">
              {matches.map((match) => (
                <div key={match.title} className="compact-list__item">
                  <div>
                    <div className="compact-list__title">
                      {match.vacancy.company} ↔ {match.candidate.name}
                    </div>
                    <div className="compact-list__text">{match.title}</div>
                  </div>
                  <Badge tone="purple">{match.score}%</Badge>
                </div>
              ))}
            </div>
          ) : (
            <InlineEmptyState text="Пока нет ни одного совпадения." />
          )}
        </Panel>

        <Panel title="Системный статус" subtitle="Сводка по обработке">
          <div className="progress-block">
            <div className="progress-block__row">
              <span>Успешность</span>
              <strong>0%</strong>
            </div>
            <div className="progress">
              <span style={{ width: "0%" }} />
            </div>
            <div className="kpi-row">
              <Badge tone="green">0 OK</Badge>
              <Badge tone="red">0 ошибок</Badge>
              <Badge tone="blue">0 мс</Badge>
            </div>
          </div>
        </Panel>
      </div>
    </section>
  );
}

function InboxPage({ items, total, onRefresh, onSearch, onOpen }) {
  return (
    <section className="page">
      <PageHeading title="Входящие сообщения" subtitle={`Всего сообщений: ${total}`} actionLabel="Обновить" onAction={onRefresh} />
      <FilterBar fields={["Поиск по чату...", "Тип", "Статус"]} buttonLabel="Искать" onSubmit={onSearch} />
      <Table
        columns={["Тип", "Чат / источник", "Превью", "Статус", "Дата", ""]}
        rows={items.map((item) => [
          <Badge key={`t-${item[1]}`} tone={item[0] === "Бенч" ? "blue" : item[0] === "Другое" ? "gray" : "purple"}>
            {item[0]}
          </Badge>,
          <div key={`c-${item[1]}`}>
            <div className="cell-title">{item[1]}</div>
            <div className="cell-subtitle">{item[2]}</div>
          </div>,
          item[3],
          <Badge key={`s-${item[1]}`} tone={item[4] === "Активно" ? "green" : "gray"}>
            {item[4]}
          </Badge>,
          item[5],
          <button key={`open-${item[1]}`} className="table-action" type="button" onClick={() => onOpen(item[1])} title="Открыть источник">
            ↗
          </button>
        ])}
        emptyMessage="Входящих сообщений пока нет."
      />
    </section>
  );
}

function VacanciesPage({ items, total, onRefresh, onSearch, onOpen }) {
  return (
    <section className="page">
      <PageHeading title="Вакансии" subtitle={`Всего вакансий: ${total}`} actionLabel="Обновить" onAction={onRefresh} />
      <FilterBar fields={["Поиск по заказчику...", "Стек", "Грейд", "Статус"]} buttonLabel="Искать" onSubmit={onSearch} />
      <Table
        columns={["Заказчик", "Стек", "Грейд", "Ставка", "Статус", "Дата", ""]}
        rows={items.map((item) => [
          <div key={`company-${item[0]}`} className="cell-title">{item[0]}</div>,
          <TagList key={`tags-${item[0]}`} items={item[1]} />,
          <Badge key={`grade-${item[0]}`} tone={item[2] === "Senior" ? "purple" : item[2] === "Junior" ? "green" : "blue"}>
            {item[2]}
          </Badge>,
          item[3],
          <Badge key={`status-${item[0]}`} tone="green">{item[4]}</Badge>,
          item[5],
          <button key={`details-${item[0]}`} className="table-action" type="button" onClick={() => onOpen(item[0])} title="Показать детали">
            ⋯
          </button>
        ])}
        emptyMessage="Список вакансий пуст."
      />
    </section>
  );
}

function BenchPage({ items, total, onRefresh, onSearch, onOpen }) {
  return (
    <section className="page">
      <PageHeading title="Бенч (Специалисты)" subtitle={`Всего специалистов: ${total}`} actionLabel="Обновить" onAction={onRefresh} />
      <FilterBar fields={["Поиск по локации...", "Стек", "Грейд", "Статус"]} buttonLabel="Искать" onSubmit={onSearch} />
      <Table
        columns={["Специалист", "Стек", "Грейд", "Ставка", "Локация", "Статус", "Дата", ""]}
        rows={items.map((item) => [
          <div key={`person-${item[0]}`} className="person-cell">
            <div className="avatar-mini">{item[0][0]}</div>
            <span>{item[0]}</span>
          </div>,
          <TagList key={`stack-${item[0]}`} items={item[1]} />,
          <Badge key={`grade-${item[0]}`} tone={item[2] === "Lead" ? "gold" : item[2] === "Senior" ? "purple" : "blue"}>
            {item[2]}
          </Badge>,
          item[3],
          item[4],
          <Badge key={`status-${item[0]}`} tone="green">{item[5]}</Badge>,
          item[6],
          <button key={`candidate-${item[0]}`} className="table-action" type="button" onClick={() => onOpen(item[0])} title="Открыть профиль">
            ↗
          </button>
        ])}
        emptyMessage="Бенч сейчас пуст."
      />
    </section>
  );
}

function MatchesPage({ items, sortOrder, onSortChange, onRefresh, onAction }) {
  return (
    <section className="page">
      <PageHeading title="Совпадения" subtitle={`Всего совпадений: ${items.length}`} actionLabel="Обновить" onAction={onRefresh} />
      <div className="stat-grid stat-grid--three">
        <article className="stat-card">
          <div className="stat-card__value">{items.length}</div>
          <div className="stat-card__label">Всего</div>
        </article>
        <article className="stat-card stat-card--gold">
          <div className="stat-card__value">{items.length}</div>
          <div className="stat-card__label">На проверке</div>
        </article>
        <article className="stat-card stat-card--green">
          <div className="stat-card__value">0</div>
          <div className="stat-card__label">Подтверждено</div>
        </article>
      </div>

      <div className="sort-switch" role="group" aria-label="Сортировка совпадений">
        <span>Сортировка по релевантности:</span>
        <button
          className={`ghost-tab ${sortOrder === "desc" ? "is-active" : ""}`}
          type="button"
          onClick={() => onSortChange("desc")}
        >
          Сначала больше
        </button>
        <button
          className={`ghost-tab ${sortOrder === "asc" ? "is-active" : ""}`}
          type="button"
          onClick={() => onSortChange("asc")}
        >
          Сначала меньше
        </button>
      </div>

      <div className="match-list">
        {items.length ? (
          items.map((match) => (
            <article key={match.title} className="match-card">
              <div className="match-card__head">
                <div className="score-box">{match.score}%</div>
                <div>
                  <div className="match-card__eyebrow">Релевантность</div>
                  <h3>{match.title}</h3>
                </div>
                <Badge tone="blue">{match.status}</Badge>
              </div>

              <div className="match-card__content">
                <CompareCard
                  title="Вакансия"
                  name={match.vacancy.company}
                  stack={match.vacancy.stack}
                  grade={match.vacancy.grade}
                  rate={match.vacancy.rate}
                />
                <div className="match-card__connector">⇄</div>
                <CompareCard
                  title="Специалист"
                  name={match.candidate.name}
                  stack={match.candidate.stack}
                  grade={match.candidate.grade}
                  rate={match.candidate.rate}
                />
              </div>

              <div className="match-card__footer">
                <span>{match.date}</span>
                <div className="match-card__actions">
                  <button className="btn btn--ghost" type="button" onClick={() => onAction("reject", match.title)}>
                    Отклонить
                  </button>
                  <button className="btn btn--success" type="button" onClick={() => onAction("approve", match.title)}>
                    Подтвердить
                  </button>
                </div>
              </div>
            </article>
          ))
        ) : (
          <Panel title="Результаты" subtitle="">
            <InlineEmptyState text="Совпадений пока нет." />
          </Panel>
        )}
      </div>
    </section>
  );
}

function RunPage({ mode, value, result, onModeChange, onChange, onClear, onProcess }) {
  return (
    <section className="page">
      <PageHeading title="Ручной прогон" subtitle="Вставьте текст и получите топ релевантной выдачи с противоположной стороны" />
      <div className="split-grid">
        <Panel title="Входные данные" subtitle="">
          <div className="panel-actions">
            <button className={`ghost-tab ${mode === "bench" ? "is-active" : ""}`} type="button" onClick={() => onModeChange("bench")}>
              Вакансия → бенчи
            </button>
            <button className={`ghost-tab ${mode === "vacancy" ? "is-active" : ""}`} type="button" onClick={() => onModeChange("vacancy")}>
              Бенч → вакансии
            </button>
          </div>

          <div className="input-label">{mode === "bench" ? "Текст вакансии" : "Текст бенча"}</div>
          <textarea
            className="editor"
            value={value}
            onChange={(event) => onChange(event.target.value)}
            placeholder={mode === "bench" ? "Вставьте описание вакансии..." : "Вставьте описание специалиста..."}
          />

          <div className="editor-footer">
            <button className="btn btn--ghost" type="button" onClick={onClear}>
              Очистить
            </button>
            <button className="btn btn--primary" type="button" onClick={onProcess}>
              Найти топ
            </button>
          </div>
        </Panel>

        <Panel title="Результат" subtitle="">
          {result ? (
            <div className="run-results">
              <div className="result-card result-card--summary">
                <div className="result-card__title">{result.title}</div>
                <p>{result.description}</p>
              </div>
              {result.items.length ? (
                result.items.map((item) => (
                  <article key={item.id} className="run-result-item">
                    <div className="run-result-item__head">
                      <div>
                        <div className="run-result-item__title">{item.title}</div>
                        <div className="cell-subtitle">{item.subtitle}</div>
                      </div>
                      <Badge tone="purple">{item.score}%</Badge>
                    </div>
                    <div className="run-result-item__meta">
                      <span>{item.kindLabel}</span>
                      <span>{item.meta}</span>
                    </div>
                    <TagList items={item.tags} />
                    {item.overlap.length ? (
                      <div className="run-result-item__footer">
                        <span>Совпало:</span>
                        <div className="tag-list">
                          {item.overlap.slice(0, 6).map((word) => (
                            <Badge key={word} tone="blue">{word}</Badge>
                          ))}
                        </div>
                      </div>
                    ) : null}
                  </article>
                ))
              ) : null}
            </div>
          ) : (
            <div className="empty-state">
              <div className="empty-state__icon">⚡</div>
              <p>Вставьте текст и запустите ручной прогон, чтобы увидеть топ выдачи.</p>
            </div>
          )}
        </Panel>
      </div>
      <footer className="page-footer">© 2026 ООО "Пятый элемент". Все права защищены.</footer>
    </section>
  );
}

function ActivityChart({ labels, benchValues, vacancyValues }) {
  const maxValue = Math.max(1, ...benchValues, ...vacancyValues);
  const chartHeight = 180;
  const hasActivity = [...benchValues, ...vacancyValues].some((value) => value > 0);
  const benchPoints = benchValues
    .map((value, index) => `${(index / (benchValues.length - 1 || 1)) * 100},${chartHeight - (value / maxValue) * chartHeight}`)
    .join(" ");
  const vacancyPoints = vacancyValues
    .map((value, index) => `${(index / (vacancyValues.length - 1 || 1)) * 100},${chartHeight - (value / maxValue) * chartHeight}`)
    .join(" ");

  return (
    <div className="activity-chart">
      <svg viewBox={`0 0 100 ${chartHeight}`} preserveAspectRatio="none" aria-hidden="true">
        {[0, 1, 2, 3, 4].map((step) => (
          <line key={step} x1="0" y1={(chartHeight / 4) * step} x2="100" y2={(chartHeight / 4) * step} />
        ))}
        {hasActivity ? <polyline className="activity-chart__line activity-chart__line--bench" points={benchPoints} /> : null}
        {hasActivity ? <polyline className="activity-chart__line activity-chart__line--vacancy" points={vacancyPoints} /> : null}
      </svg>
      <div className="activity-chart__labels">
        {labels.map((label) => (
          <span key={label}>{label}</span>
        ))}
      </div>
      <div className="activity-chart__legend">
        <Badge tone="blue">Бенч</Badge>
        <Badge tone="purple">Вакансии</Badge>
      </div>
    </div>
  );
}

function buildImportJobState(job) {
  const summary = job?.summary || null;
  if (!job) {
    return {
      status: "idle",
      message: "Импорт ещё не запускался.",
      jobId: null,
      summary: null
    };
  }

  if (job.status === "completed") {
    return {
      status: "completed",
      message: `Импорт завершён: вакансий ${summary?.vacancies || 0}, специалистов ${summary?.specialists || 0}, пропущено ${summary?.skipped || 0}.`,
      jobId: job.job_id,
      summary
    };
  }

  if (job.status === "failed") {
    return {
      status: "failed",
      message: job.error || "Импорт завершился с ошибкой.",
      jobId: job.job_id,
      summary
    };
  }

  if (job.status === "processing") {
    return {
      status: "processing",
      message: "Импорт сейчас обрабатывается в фоне.",
      jobId: job.job_id,
      summary
    };
  }

  return {
    status: job.status || "queued",
    message: "Job создан, ожидаю завершения...",
    jobId: job.job_id,
    summary
  };
}

function ManualUploadPage({ onComplete, apiState }) {
  const [mode, setMode] = React.useState("vacancy");
  const [selectedFileName, setSelectedFileName] = React.useState("");
  const [selectedFile, setSelectedFile] = React.useState(null);
  const [inputText, setInputText] = React.useState("");
  const [urlValue, setUrlValue] = React.useState("");
  const [jobState, setJobState] = React.useState({
    status: "idle",
    message: "Импорт ещё не запускался.",
    jobId: null,
    summary: null
  });

  const forcedType = mode === "vacancy" ? "VACANCY" : "BENCH";

  const waitForJob = React.useCallback(async (jobId) => {
    for (let attempt = 0; attempt < 20; attempt += 1) {
      const job = await fetchImportJob(jobId);
      if (job.status === "completed" || job.status === "failed") {
        return job;
      }
      await new Promise((resolve) => window.setTimeout(resolve, 1500));
    }
    return await fetchImportJob(jobId);
  }, []);

  const runImport = React.useCallback(async (submitter, successLabel) => {
    try {
      setJobState({ status: "submitting", message: "Создаю job импорта...", jobId: null, summary: null });
      const accepted = await submitter();
      setJobState({ status: accepted.status, message: "Job создан, ожидаю завершения...", jobId: accepted.job_id, summary: null });
      const finalJob = await waitForJob(accepted.job_id);
      setJobState(
        finalJob.status === "completed"
          ? { ...buildImportJobState(finalJob), message: successLabel }
          : buildImportJobState(finalJob)
      );
      if (finalJob.status === "completed") {
        await onComplete();
      }
    } catch (error) {
      setJobState({ status: "failed", message: error.message, jobId: null, summary: null });
    }
  }, [onComplete, waitForJob]);

  React.useEffect(() => {
    let ignore = false;

    async function loadLatestImportJob() {
      try {
        const payload = await fetchRecentImportJobs(1);
        const latestJob = payload?.items?.[0] || null;
        if (!ignore && latestJob) {
          setJobState(buildImportJobState(latestJob));
        }
      } catch (_error) {
        // Keep current UI state if recent jobs are temporarily unavailable.
      }
    }

    loadLatestImportJob();
    return () => {
      ignore = true;
    };
  }, [apiState.lastSync]);

  return (
    <section className="page">
      <PageHeading title="Ручная загрузка" subtitle="Подгрузите файл или вставьте текст для дальнейшей обработки" />

      <div className="split-grid split-grid--manual-upload">
        <Panel title="Тип данных" subtitle="Выберите, с чем работаете сейчас">
          <div className="panel-actions panel-actions--stacked">
            <button className={`ghost-tab ${mode === "vacancy" ? "is-active" : ""}`} type="button" onClick={() => setMode("vacancy")}>
              Вакансия
            </button>
            <button className={`ghost-tab ${mode === "bench" ? "is-active" : ""}`} type="button" onClick={() => setMode("bench")}>
              Бенч
            </button>
          </div>
          <div className="manual-upload-hint">
            {mode === "vacancy"
              ? "Загрузите описание вакансии файлом или вставьте текст напрямую в поле справа."
              : "Загрузите резюме или описание специалиста файлом, либо вставьте текст вручную."}
          </div>
        </Panel>

        <div className="manual-upload-stack">
          <Panel title="Подгрузить файл" subtitle="Поддерживаются текстовые и офисные форматы">
            <label className="upload-dropzone">
              <input
                className="upload-dropzone__input"
                type="file"
                onChange={(event) => {
                  const nextFile = event.target.files?.[0] ?? null;
                  setSelectedFile(nextFile);
                  setSelectedFileName(nextFile?.name ?? "");
                }}
              />
              <span className="upload-dropzone__icon">⇪</span>
              <strong>{selectedFileName || "Выберите файл для загрузки"}</strong>
              <span>{selectedFileName ? "Файл выбран и готов к дальнейшей обработке." : "Нажмите сюда, чтобы выбрать файл с компьютера."}</span>
            </label>
            <div className="editor-footer">
              <button
                className="btn btn--primary"
                type="button"
                disabled={!selectedFile}
                onClick={() =>
                  runImport(
                    () => submitFileImport(selectedFile, forcedType),
                    "Файловый импорт завершён. Данные на странице обновлены."
                  )
                }
              >
                Загрузить файл в backend
              </button>
            </div>
          </Panel>

          <Panel title="Вставить текст" subtitle="Сюда можно вставить описание вакансии, резюме или сообщение целиком">
            <textarea
              className="editor manual-upload-editor"
              value={inputText}
              onChange={(event) => setInputText(event.target.value)}
              placeholder={mode === "vacancy" ? "Вставьте сюда текст вакансии..." : "Вставьте сюда текст по специалисту..."}
            />
            <div className="editor-footer">
              <button className="btn btn--ghost" type="button" onClick={() => setInputText("")}>Очистить</button>
              <button
                className="btn btn--primary"
                type="button"
                disabled={!inputText.trim()}
                onClick={() =>
                  runImport(
                    () => submitTextImport(inputText, forcedType),
                    "Текстовый импорт завершён. Данные на странице обновлены."
                  )
                }
              >
                Отправить текст
              </button>
            </div>
          </Panel>

          <Panel title="Импорт по ссылке" subtitle="Поддерживаются Google Docs/Sheets, Drive, Yandex Disk и allowlisted URL">
            <input
              className="editor manual-upload-url"
              value={urlValue}
              onChange={(event) => setUrlValue(event.target.value)}
              placeholder="Вставьте публичную ссылку..."
            />
            <div className="editor-footer">
              <button className="btn btn--ghost" type="button" onClick={() => setUrlValue("")}>Очистить</button>
              <button
                className="btn btn--primary"
                type="button"
                disabled={!urlValue.trim()}
                onClick={() =>
                  runImport(
                    () => submitUrlImport(urlValue, forcedType),
                    "Импорт по ссылке завершён. Данные на странице обновлены."
                  )
                }
              >
                Импортировать URL
              </button>
            </div>
          </Panel>

          <Panel title="Статус backend" subtitle="Что уже работает через сайт">
            <div className="compact-list">
              <div className="compact-list__item">
                <div>
                  <div className="compact-list__title">Состояние API</div>
                  <div className="compact-list__text">{apiState.message}</div>
                </div>
                <Badge tone={apiState.status === "online" ? "green" : apiState.status === "loading" ? "blue" : "red"}>{apiState.status}</Badge>
              </div>
              <div className="compact-list__item">
                <div>
                  <div className="compact-list__title">Последний import job</div>
                  <div className="compact-list__text">{jobState.message}</div>
                </div>
                <Badge tone={jobState.status === "completed" ? "green" : jobState.status === "failed" ? "red" : "blue"}>{jobState.status}</Badge>
              </div>
              {jobState.jobId ? (
                <div className="compact-list__item">
                  <div>
                    <div className="compact-list__title">Job ID</div>
                    <div className="compact-list__text">{jobState.jobId}</div>
                  </div>
                </div>
              ) : null}
              {jobState.summary ? (
                <div className="compact-list__item">
                  <div>
                    <div className="compact-list__title">Результат</div>
                    <div className="compact-list__text">
                      Вакансий: {jobState.summary.vacancies || 0}, специалистов: {jobState.summary.specialists || 0}, пропущено: {jobState.summary.skipped || 0}
                    </div>
                  </div>
                </div>
              ) : null}
            </div>
          </Panel>
        </div>
      </div>
    </section>
  );
}

function LogsPage({ items, total, onRefresh, onSearch, onOpen }) {
  return (
    <section className="page">
      <PageHeading title="Логи обработки" subtitle="Диагностика и история обработки сообщений" actionLabel="Обновить" onAction={onRefresh} />

      <div className="stat-grid">
        <article className="stat-card stat-card--blue"><div className="stat-card__value">{total}</div><div className="stat-card__label">Всего обработок</div></article>
        <article className="stat-card stat-card--green"><div className="stat-card__value">0</div><div className="stat-card__label">Успешных</div></article>
        <article className="stat-card stat-card--red"><div className="stat-card__value">0</div><div className="stat-card__label">С ошибками</div></article>
        <article className="stat-card stat-card--purple"><div className="stat-card__value">0 мс</div><div className="stat-card__label">Среднее время</div></article>
      </div>

      <Panel title="Успешность обработки" subtitle="">
        <div className="progress-block">
          <div className="progress-block__row">
            <span>Успешность обработки</span>
            <strong>0%</strong>
          </div>
          <div className="progress"><span style={{ width: "0%" }} /></div>
        </div>
      </Panel>

      <FilterBar fields={["Результат"]} buttonLabel="Искать" onSubmit={onSearch} />
      <Table
        columns={["Статус", "Модель", "ID сообщения", "Время (мс)", "Дата", ""]}
        rows={items.map((item) => [
          <Badge key={`log-status-${item[2]}`} tone={item[0] === "OK" ? "green" : "red"}>{item[0]}</Badge>,
          item[1],
          item[2],
          item[3],
          item[4],
          <button key={`log-${item[2]}`} className="table-action" type="button" onClick={() => onOpen(item[2])} title="Показать лог">
            ⋯
          </button>
        ])}
        emptyMessage="Логи пока отсутствуют."
      />
    </section>
  );
}

function SettingsPage({ settings, theme, apiState, onThemeChange, onSettingsChange, onSave, onReset }) {
  return (
    <section className="page">
      <PageHeading title="Настройки" subtitle="Настройка внешнего вида интерфейса и алгоритма" />
      <div className="settings-stack">
        <Panel title="Оформление" subtitle="Настройка внешнего вида интерфейса">
          <div className="theme-switch">
            <button className={`theme-switch__item ${theme === "dark" ? "is-selected" : ""}`} type="button" onClick={() => onThemeChange("dark")}>
              <span>☾</span>
              <strong>Темная тема</strong>
            </button>
            <button className={`theme-switch__item ${theme === "light" ? "is-selected" : ""}`} type="button" onClick={() => onThemeChange("light")}>
              <span>☀</span>
              <strong>Светлая тема</strong>
            </button>
          </div>
          <p className="hint">Выбранная тема сохраняется после нажатия на кнопку сохранения.</p>
        </Panel>

        <Panel title="Время жизни записей (TTL)" subtitle="Настройка срока актуальности вакансий и бенча">
          <div className="form-grid">
            <Field
              label="TTL по умолчанию (дней)"
              value={settings.ttlDefault}
              hint="Стандартный срок актуальности записи"
              onChange={(value) => onSettingsChange("ttlDefault", value)}
            />
            <Field
              label="TTL для long-term (дней)"
              value={settings.ttlLongTerm}
              hint="Увеличенный срок для долгосрочных проектов"
              onChange={(value) => onSettingsChange("ttlLongTerm", value)}
            />
          </div>
        </Panel>

        <Panel title="Параметры рекрутинга" subtitle="Настройка алгоритма подбора совпадений">
          <div className="form-grid form-grid--single">
            <Field
              label="Минимальный порог релевантности (%)"
              value={settings.matchThreshold}
              hint="Совпадения ниже этого порога не показываются"
              onChange={(value) => onSettingsChange("matchThreshold", value)}
            />
          </div>
          <div className="editor-footer">
            <button className="btn btn--ghost" type="button" onClick={onReset}>
              Сбросить
            </button>
            <button className="btn btn--primary" type="button" onClick={onSave}>
              Сохранить
            </button>
          </div>
        </Panel>

        <Panel title="Backend интеграция" subtitle="Текущее подключение frontend к API">
          <div className="compact-list">
            <div className="compact-list__item">
              <div>
                <div className="compact-list__title">API base URL</div>
                <div className="compact-list__text">{API_BASE_URL}</div>
              </div>
            </div>
            <div className="compact-list__item">
              <div>
                <div className="compact-list__title">Статус</div>
                <div className="compact-list__text">{apiState.message}</div>
              </div>
              <Badge tone={apiState.status === "online" ? "green" : apiState.status === "loading" ? "blue" : "red"}>
                {apiState.status}
              </Badge>
            </div>
            {apiState.ownBench ? (
              <div className="compact-list__item">
                <div>
                  <div className="compact-list__title">Наш бенч</div>
                  <div className="compact-list__text">
                    Активных строк: {apiState.ownBench.active_rows || 0}. Последний sync: {apiState.ownBench.last_success_label || "—"}
                  </div>
                </div>
              </div>
            ) : null}
          </div>
        </Panel>
      </div>
    </section>
  );
}

function PageHeading({ title, subtitle, actionLabel, onAction }) {
  return (
    <div className="page-heading">
      <div>
        <h1>{title}</h1>
        <p>{subtitle}</p>
      </div>
      {actionLabel ? (
        <button className="btn btn--ghost" type="button" onClick={onAction}>
          ⟳ {actionLabel}
        </button>
      ) : null}
    </div>
  );
}

function FilterBar({ fields, buttonLabel, onSubmit }) {
  const [values, setValues] = React.useState(() => fields.map(() => ""));

  React.useEffect(() => {
    setValues(fields.map(() => ""));
  }, [fields]);

  return (
    <div className="filter-bar">
      {fields.map((field, index) => (
        <div key={field} className="filter-input">
          <input
            value={values[index] ?? ""}
            placeholder={field}
            onChange={(event) =>
              setValues((prev) => prev.map((item, itemIndex) => (itemIndex === index ? event.target.value : item)))
            }
            onKeyDown={(event) => {
              if (event.key === "Enter") onSubmit(values);
            }}
          />
        </div>
      ))}
      {buttonLabel ? (
        <button className="btn btn--ghost btn--search" type="button" onClick={() => onSubmit(values)}>
          ⌕ {buttonLabel}
        </button>
      ) : null}
    </div>
  );
}

function Panel({ title, subtitle, children }) {
  return (
    <section className="panel">
      <div className="panel__header">
        <div>
          <h2>{title}</h2>
          {subtitle ? <p>{subtitle}</p> : null}
        </div>
      </div>
      {children}
    </section>
  );
}

function Table({ columns, rows, emptyMessage = "Результатов нет." }) {
  return (
    <div className="table-wrap">
      <table className="table">
        <thead>
          <tr>
            {columns.map((column) => (
              <th key={column}>{column}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.length ? (
            rows.map((row, rowIndex) => (
              <tr key={rowIndex}>
                {row.map((cell, cellIndex) => (
                  <td key={cellIndex}>{cell}</td>
                ))}
              </tr>
            ))
          ) : (
            <tr>
              <td className="table__empty" colSpan={columns.length}>
                {emptyMessage}
              </td>
            </tr>
          )}
        </tbody>
      </table>
    </div>
  );
}

function TagList({ items }) {
  if (!items.length) return <span className="muted">—</span>;
  return (
    <div className="tag-list">
      {items.map((item) => (
        <Badge key={item} tone="purple">{item}</Badge>
      ))}
    </div>
  );
}

function Badge({ tone = "gray", children }) {
  return <span className={`badge badge--${tone}`}>{children}</span>;
}

function CompareCard({ title, name, stack, grade, rate }) {
  return (
    <div className="compare-card">
      <div className="compare-card__label">{title}</div>
      <div className="compare-card__name">{name}</div>
      <TagList items={stack} />
      <div className="compare-card__meta">
        <Badge tone="blue">{grade}</Badge>
        <span>{rate}</span>
      </div>
    </div>
  );
}

function Field({ label, value, hint, onChange }) {
  return (
    <label className="field">
      <span>{label}</span>
      <input value={value} onChange={(event) => onChange(event.target.value)} />
      <small>{hint}</small>
    </label>
  );
}

function InlineEmptyState({ text }) {
  return <div className="inline-empty-state">{text}</div>;
}

function Icon({ name }) {
  return (
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
      <path d={iconPaths[name]} />
    </svg>
  );
}

export default App;
