
import React from "react";
import {
  dashboardStats,
  navItems
} from "./data";
import { api } from "./api";

const iconPaths = {
  grid: "M4 4h7v7H4zM13 4h7v7h-7zM4 13h7v7H4zM13 13h7v7h-7z",
  inbox: "M4 5h16v10l-2 5H6l-2-5zM4 13h16",
  briefcase: "M8 6V4h8v2M4 8h16v10H4z",
  users: "M9 11a3 3 0 1 0 0-6a3 3 0 0 0 0 6zm6 1a2.5 2.5 0 1 0 0-5a2.5 2.5 0 0 0 0 5zM4 20a5 5 0 0 1 10 0M13 20a4 4 0 0 1 7 0",
  link: "M10 14l4-4M7 17H6a4 4 0 1 1 0-8h3M17 7h1a4 4 0 1 1 0 8h-3",
  bolt: "M13 2L4 14h6l-1 8l9-12h-6z",
  wave: "M3 12h4l2-6l4 12l2-6h6",
  gear: "M12 3l1 2.2l2.4.6l-.5 2.4l1.7 1.7l-1.7 1.7l.5 2.4l-2.4.6L12 21l-1-2.2l-2.4-.6l.5-2.4L7.4 14l1.7-1.7l-.5-2.4l2.4-.6zM12 9a3 3 0 1 0 0 6a3 3 0 0 0 0-6z"
};

const initialTheme = "light";
const initialCollections = {
  inbox: [],
  vacancies: [],
  bench: [],
  matches: [],
  logs: []
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
const defaultTelegramChannelForm = {
  telegram_id: "",
  title: "",
  username: "",
  source_kind: "vacancy"
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
  return filters.every((filter) => {
    if (!filter) {
      return true;
    }

    const aliases = filter === "ок" || filter === "ok" ? ["ок", "ok"] : [filter];
    return aliases.some((alias) => haystack.includes(alias));
  });
}

function extractKeywords(value) {
  return normalizeText(value)
    .replace(/[^\p{L}\p{N}+#.]/gu, " ")
    .split(" ")
    .filter((word) => word.length > 2)
    .filter((word) => !keywordStopwords.has(word));
}

function buildRunCorpus(mode, collections) {
  if (mode === "bench") {
    return collections.vacancies.map((item) => ({
      id: item[0],
      title: item[0],
      subtitle: `${item[2]} • ${item[4]}`,
      meta: item[3],
      tags: item[1],
      kindLabel: "Вакансия",
      searchableText: [item[0], item[1].join(" "), item[2], item[3], item[4], item[5]].join(" ")
    }));
  }

  return collections.bench.map((item) => ({
    id: item[0],
    title: item[0],
    subtitle: `${item[2]} • ${item[4]}`,
    meta: item[3],
    tags: item[1],
    kindLabel: "Бенч",
    searchableText: [item[0], item[1].join(" "), item[2], item[3], item[4], item[5], item[6]].join(" ")
  }));
}

function buildManualRunResult(queryText, mode, collections) {
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
  const ranked = buildRunCorpus(mode, collections)
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
    .slice(0, 10);

  return {
    title: mode === "bench" ? "Топ-10 вакансий для специалиста" : "Топ-10 специалистов под вакансию",
    description: ranked.length
      ? "Показаны самые релевантные записи по ключевым словам."
      : "Подходящих записей по текущему тексту не найдено.",
    items: ranked
  };
}

function buildActivitySeries(seed, modifier) {
  return [0, 0, 0, 0, 0, 0, 0].map((_, index) => Math.max(0, Math.round(seed * modifier[index])));
}

const collectionLoaders = {
  inbox: api.getInbox,
  vacancies: api.getVacancies,
  bench: api.getBench,
  matches: api.getMatches,
  logs: api.getLogs
};

function App() {
  const [path, setPath] = React.useState(window.location.pathname);
  const [theme, setTheme] = React.useState(initialTheme);
  const [sourceCollections, setSourceCollections] = React.useState(initialCollections);
  const [collections, setCollections] = React.useState(initialCollections);
  const [notice, setNotice] = React.useState(null);
  const [actionDialog, setActionDialog] = React.useState(null);
  const [runText, setRunText] = React.useState("");
  const [runMode, setRunMode] = React.useState("bench");
  const [runResult, setRunResult] = React.useState(null);
  const [matchesSortOrder, setMatchesSortOrder] = React.useState("desc");
  const [settings, setSettings] = React.useState(defaultSettings);
  const [settingsTheme, setSettingsTheme] = React.useState(initialTheme);
  const [benchScope, setBenchScope] = React.useState("all");
  const [telegramChannels, setTelegramChannels] = React.useState([]);

  React.useEffect(() => {
    const onChange = () => setPath(window.location.pathname);
    window.addEventListener("popstate", onChange);
    return () => window.removeEventListener("popstate", onChange);
  }, []);

  React.useEffect(() => {
    document.documentElement.dataset.theme = theme;
    document.documentElement.style.colorScheme = theme;
  }, [theme]);

  React.useEffect(() => {
    if (!notice) {
      return undefined;
    }

    const timeoutId = window.setTimeout(() => setNotice(null), 3200);
    return () => window.clearTimeout(timeoutId);
  }, [notice]);

  const loadCollection = React.useCallback(async (pageName) => {
    const loader = collectionLoaders[pageName];

    if (!loader) {
      return [];
    }

    const params = pageName === "bench" ? { bench_scope: benchScope } : undefined;
    const items = await loader(params);
    setSourceCollections((prev) => ({ ...prev, [pageName]: items }));
    setCollections((prev) => ({ ...prev, [pageName]: items }));
    return items;
  }, [benchScope]);

  const loadTelegramChannels = React.useCallback(async () => {
    try {
      const payload = await api.getTelegramChannels();
      setTelegramChannels(Array.isArray(payload?.items) ? payload.items : []);
    } catch (error) {
      console.error("Failed to load telegram channels:", error);
    }
  }, []);

  React.useEffect(() => {
    Object.keys(collectionLoaders).forEach((pageName) => {
      loadCollection(pageName).catch((error) => {
        console.error(`Failed to load ${pageName}:`, error);
      });
    });
    loadTelegramChannels();
  }, [loadCollection, loadTelegramChannels]);

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
  const ownBenchMatchesCount = sortedMatches.filter((item) => item.source === "own_bench").length;
  const partnerBenchMatchesCount = sortedMatches.filter((item) => item.source === "partner_bench").length;
  const digestSummary = {
    time: "16:00",
    todaysVacancies: collections.vacancies.length,
    channelsStatus: "Ожидаем список приоритетных каналов",
    partnersStatus: "Ожидаем список приоритетных партнеров"
  };
  const activityLabels = ["25.03", "26.03", "27.03", "28.03", "29.03", "30.03", "31.03"];
  const benchActivity = buildActivitySeries(collections.bench.length, [0.4, 0.6, 0.8, 0.7, 1, 0.5, 0.9]);
  const vacancyActivity = buildActivitySeries(collections.vacancies.length, [0.6, 0.5, 0.7, 0.9, 0.8, 0.6, 1]);

  const handlePageRefresh = (pageName) => {
    if (pageName === "run") {
      setRunResult(null);
      return;
    }

    loadCollection(pageName).catch((error) => {
      console.error(`Failed to refresh ${pageName}:`, error);
    });
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

  const handleRunProcess = () => {
    const result = buildManualRunResult(runText, runMode, sourceCollections);
    setRunResult(result);
  };

  const handleMatchAction = (action, matchTitle) => {
    setCollections((prev) => ({
      ...prev,
      matches: prev.matches.filter((item) => item.title !== matchTitle)
    }));
    setNotice(action === "approve" ? `Совпадение "${matchTitle}" подтверждено.` : `Совпадение "${matchTitle}" отклонено.`);
  };

  const handleSettingsChange = (key, value) => {
    setSettings((prev) => ({ ...prev, [key]: value }));
  };

  const handleSettingsSave = () => {
    setTheme(settingsTheme);
    setNotice("Настройки сохранены.");
  };

  const handleSettingsReset = () => {
    setSettings(defaultSettings);
    setSettingsTheme(initialTheme);
    setTheme(initialTheme);
    setNotice("Настройки сброшены.");
  };

  const handleManualImport = async ({ sourceKind, payload, forcedType, benchOrigin }) => {
    if (sourceKind === "text") {
      await api.importTextSync({ text: payload, forced_type: forcedType, bench_origin: benchOrigin });
    } else if (sourceKind === "url") {
      await api.importUrlSync({ url: payload, forced_type: forcedType, bench_origin: benchOrigin });
    } else if (sourceKind === "file") {
      await api.importFileSync({ file: payload, forcedType, benchOrigin });
    }
    await Promise.all([loadCollection("vacancies"), loadCollection("bench"), loadCollection("matches"), loadCollection("inbox")]);
    setNotice("Импорт завершен, база обновлена.");
  };

  const handleTelegramChannelSave = async (payload) => {
    await api.upsertTelegramChannel({
      telegram_id: Number(payload.telegram_id),
      title: payload.title,
      username: payload.username || null,
      source_kind: payload.source_kind,
      is_active: true
    });
    await loadTelegramChannels();
    setNotice("Telegram-канал сохранен.");
  };

  const handleTelegramVacanciesImport = async () => {
    const result = await api.importTelegramVacancies({ limit: 300 });
    await Promise.all([loadCollection("vacancies"), loadCollection("matches"), loadCollection("inbox")]);
    setNotice(`Из Telegram подтянуто вакансий: ${result.imported_vacancies ?? 0}.`);
  };

  const handleMatchingRebuild = async () => {
    const result = await api.rebuildMatching({ limit: 500 });
    await Promise.all([loadCollection("matches"), loadCollection("vacancies"), loadCollection("bench")]);
    setNotice(`Релевантность пересчитана. Обработано вакансий: ${result.processed_vacancies ?? 0}.`);
  };

  const handleRowAction = (kind, payload) => {
    if (!payload) {
      return;
    }

    const dialogByKind = {
      inbox: {
        title: `Источник: ${payload[1] ?? "—"}`,
        rows: [
          ["Тип", payload[0]],
          ["Автор", payload[2]],
          ["Превью", payload[3]],
          ["Статус", payload[4]],
          ["Дата", payload[5]]
        ]
      },
      vacancies: {
        title: `Вакансия: ${payload[0] ?? "—"}`,
        rows: [
          ["Стек", Array.isArray(payload[1]) ? payload[1].join(", ") : payload[1]],
          ["Грейд", payload[2]],
          ["Ставка", payload[3]],
          ["Статус", payload[4]],
          ["Дата", payload[5]]
        ]
      },
      bench: {
        title: `Профиль: ${payload[0] ?? "—"}`,
        rows: [
          ["Стек", Array.isArray(payload[1]) ? payload[1].join(", ") : payload[1]],
          ["Грейд", payload[2]],
          ["Ставка", payload[3]],
          ["Локация", payload[4]],
          ["Статус", payload[5]],
          ["Дата", payload[6]]
        ]
      },
      logs: {
        title: `Лог: ${payload[2] ?? "—"}`,
        rows: [
          ["Статус", payload[0]],
          ["Модель", payload[1]],
          ["Message ID", payload[2]],
          ["Длительность", payload[3]],
          ["Дата", payload[4]]
        ]
      }
    };

    const dialog = dialogByKind[kind];
    if (dialog) {
      setActionDialog(dialog);
    }
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
              setNotice("Сессия завершена. Интерфейс возвращен в исходное состояние.");
            }}
          >
            ⌫ Выйти
          </button>
        </div>
      </aside>

      <main className="content">
        <TopBar
          onClear={clearAllResults}
        />
        {current.id === "dashboard" && (
          <DashboardPage
            navigate={navigate}
            stats={dashboardStatsView}
            matches={sortedMatches}
            inboxCount={statsByPage.inbox}
            logsCount={statsByPage.logs}
            ownBenchMatchesCount={ownBenchMatchesCount}
            partnerBenchMatchesCount={partnerBenchMatchesCount}
            digestSummary={digestSummary}
            benchActivity={benchActivity}
            vacancyActivity={vacancyActivity}
            activityLabels={activityLabels}
            telegramChannels={telegramChannels}
            onManualImport={handleManualImport}
            onTelegramChannelSave={handleTelegramChannelSave}
            onTelegramVacanciesImport={handleTelegramVacanciesImport}
            onMatchingRebuild={handleMatchingRebuild}
          />
        )}
        {current.id === "inbox" && (
          <InboxPage
            items={collections.inbox}
            total={statsByPage.inbox}
            onRefresh={() => handlePageRefresh("inbox")}
            onSearch={(filters) => handleSearch("inbox", filters)}
            onOpen={(item) => handleRowAction("inbox", item)}
          />
        )}
        {current.id === "vacancies" && (
          <VacanciesPage
            items={collections.vacancies}
            total={statsByPage.vacancies}
            onRefresh={() => handlePageRefresh("vacancies")}
            onSearch={(filters) => handleSearch("vacancies", filters)}
            onOpen={(item) => handleRowAction("vacancies", item)}
          />
        )}
        {current.id === "bench" && (
          <BenchPage
            items={collections.bench}
            total={statsByPage.bench}
            benchScope={benchScope}
            onBenchScopeChange={setBenchScope}
            onRefresh={() => handlePageRefresh("bench")}
            onSearch={(filters) => handleSearch("bench", filters)}
            onOpen={(item) => handleRowAction("bench", item)}
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
            onOpen={(item) => handleRowAction("logs", item)}
          />
        )}
        {current.id === "settings" && (
          <SettingsPage
            settings={settings}
            theme={settingsTheme}
            onThemeChange={setSettingsTheme}
            onSettingsChange={handleSettingsChange}
            onSave={handleSettingsSave}
            onReset={handleSettingsReset}
          />
        )}
      </main>
      {notice ? <Notice text={notice} /> : null}
      {actionDialog ? <ActionDialog {...actionDialog} onClose={() => setActionDialog(null)} /> : null}
    </div>
  );
}

function TopBar({ onClear }) {
  return (
    <header className="topbar topbar--stacked">
      <div className="topbar__row">
        <div className="topbar__left" />
        <div className="topbar__actions">
          <button type="button" className="icon-button" onClick={onClear} title="Очистить результаты">
            ✕
          </button>
        </div>
      </div>
    </header>
  );
}

function DashboardPage({
  navigate,
  stats,
  matches,
  inboxCount,
  logsCount,
  ownBenchMatchesCount,
  partnerBenchMatchesCount,
  digestSummary,
  benchActivity,
  vacancyActivity,
  activityLabels,
  telegramChannels,
  onManualImport,
  onTelegramChannelSave,
  onTelegramVacanciesImport,
  onMatchingRebuild
}) {
  return (
    <section className="page">
      <PageHeading
        title="Дашборд"
        subtitle="Обзор активности мэтчинга и ежедневных подборок"
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
            <div className="micro-stat-card__label">Наш бенч в мэтчинге</div>
            <strong>{ownBenchMatchesCount}</strong>
          </div>
        </article>
        <article className="micro-stat-card">
          <span className="micro-stat-card__icon">△</span>
          <div>
            <div className="micro-stat-card__label">Партнерские совпадения</div>
            <strong>{partnerBenchMatchesCount}</strong>
          </div>
        </article>
      </div>

      <Panel title="Активность за неделю" subtitle="">
        <ActivityChart labels={activityLabels} benchValues={benchActivity} vacancyValues={vacancyActivity} />
      </Panel>

      <div className="dashboard-grid">
        <Panel title="Ручная загрузка и актуализация" subtitle="Бенч или вакансия, файл или ссылка, плюс операции по Telegram и релевантности">
          <ImportControlPanel
            telegramChannels={telegramChannels}
            onManualImport={onManualImport}
            onTelegramChannelSave={onTelegramChannelSave}
            onTelegramVacanciesImport={onTelegramVacanciesImport}
            onMatchingRebuild={onMatchingRebuild}
          />
        </Panel>

        <Panel title="Приоритет мэтчинга" subtitle="Сначала наш бенч, затем партнеры">
          {matches.length ? (
            <div className="compact-list">
              {matches.map((match) => (
                <div key={match.title} className="compact-list__item">
                  <div>
                    <div className="compact-list__title">
                      {match.priorityLabel}: {match.vacancy.company} ↔ {match.candidate.name}
                    </div>
                    <div className="compact-list__text">{match.title}</div>
                  </div>
                  <Badge tone="purple">{match.score}%</Badge>
                </div>
              ))}
            </div>
          ) : (
            <InlineEmptyState text="Совпадений пока нет. При запуске сначала проверяем наш бенч, затем партнеров." />
          )}
        </Panel>

        <Panel title="Ежедневный дайджест" subtitle="Подборка новых вакансий к 16:00">
          <div className="digest-card">
            <div className="digest-card__row">
              <span>Время отправки</span>
              <strong>{digestSummary.time}</strong>
            </div>
            <div className="digest-card__row">
              <span>Сегодняшних вакансий в витрине</span>
              <strong>{digestSummary.todaysVacancies}</strong>
            </div>
            <div className="digest-card__row">
              <span>Каналы для парсинга</span>
              <Badge tone="blue">{digestSummary.channelsStatus}</Badge>
            </div>
            <div className="digest-card__row">
              <span>Приоритет партнеров</span>
              <Badge tone="gold">{digestSummary.partnersStatus}</Badge>
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
          <button key={`open-${item[1]}`} className="table-action" type="button" onClick={() => onOpen(item)} title="Открыть источник">
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
          <button key={`details-${item[0]}`} className="table-action" type="button" onClick={() => onOpen(item)} title="Показать детали">
            ⋯
          </button>
        ])}
        emptyMessage="Список вакансий пуст."
      />
    </section>
  );
}

function BenchPage({ items, total, benchScope, onBenchScopeChange, onRefresh, onSearch, onOpen }) {
  return (
    <section className="page">
      <PageHeading title="Бенч (Специалисты)" subtitle={`Всего специалистов на бенче: ${total}`} actionLabel="Обновить" onAction={onRefresh} />
      <div className="panel-actions">
        <button className={`ghost-tab ${benchScope === "all" ? "is-active" : ""}`} type="button" onClick={() => onBenchScopeChange("all")}>
          Весь бенч
        </button>
        <button className={`ghost-tab ${benchScope === "own" ? "is-active" : ""}`} type="button" onClick={() => onBenchScopeChange("own")}>
          Наш бенч
        </button>
        <button className={`ghost-tab ${benchScope === "partner" ? "is-active" : ""}`} type="button" onClick={() => onBenchScopeChange("partner")}>
          Бенч партнеров
        </button>
      </div>
      <FilterBar fields={["Поиск по локации...", "Стек", "Грейд", "Статус"]} buttonLabel="Искать" onSubmit={onSearch} />
      <Panel title="Логика подбора" subtitle="">
        <div className="priority-note">
          Специалист на бенче считается доступным по умолчанию. Для подбора вакансий используем стек, грейд, ставку и локацию/формат.
        </div>
      </Panel>
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
          <button key={`candidate-${item[0]}`} className="table-action" type="button" onClick={() => onOpen(item)} title="Открыть профиль">
            ↗
          </button>
        ])}
        emptyMessage="Бенч сейчас пуст."
      />
    </section>
  );
}

function MatchesPage({ items, sortOrder, onSortChange, onRefresh, onAction }) {
  const ownBenchMatches = items.filter((item) => item.source === "own_bench");
  const partnerBenchMatches = items.filter((item) => item.source === "partner_bench");

  return (
    <section className="page">
      <PageHeading title="Совпадения" subtitle={`Всего совпадений: ${items.length}`} actionLabel="Обновить" onAction={onRefresh} />
      <div className="stat-grid stat-grid--three">
        <article className="stat-card">
          <div className="stat-card__value">{items.length}</div>
          <div className="stat-card__label">Всего</div>
        </article>
        <article className="stat-card stat-card--gold">
          <div className="stat-card__value">{ownBenchMatches.length}</div>
          <div className="stat-card__label">Наш бенч</div>
        </article>
        <article className="stat-card stat-card--green">
          <div className="stat-card__value">{partnerBenchMatches.length}</div>
          <div className="stat-card__label">Партнеры</div>
        </article>
      </div>

      <Panel title="Правило приоритета" subtitle="">
        <div className="priority-note">
          Сначала ищем совпадения по нашему бенчу. Если внутренних совпадений нет, показываем партнерский бенч. Если не подошел никто, явно фиксируем, что вакансия обработана и совпадений не найдено.
        </div>
      </Panel>

      {!ownBenchMatches.length && partnerBenchMatches.length ? (
        <div className="status-banner status-banner--warning">
          По нашему бенчу совпадений не найдено. Ниже показаны только партнерские специалисты.
        </div>
      ) : null}

      {!items.length ? (
        <div className="status-banner">
          Совпадений не найдено. Вакансия обработана: сначала проверен наш бенч, затем партнерские бенчи.
        </div>
      ) : null}

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
                <div className="match-card__badges">
                  <Badge tone={match.source === "own_bench" ? "green" : "gold"}>{match.priorityLabel}</Badge>
                  <Badge tone="blue">{match.status}</Badge>
                </div>
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
      <PageHeading title="Ручной прогон" subtitle="Вставьте текст и получите Top-10 релевантной выдачи с противоположной стороны" />
      <div className="split-grid">
        <Panel title="Входные данные" subtitle="">
          <div className="panel-actions">
            <button className={`ghost-tab ${mode === "bench" ? "is-active" : ""}`} type="button" onClick={() => onModeChange("bench")}>
              Специалист на бенче → вакансии
            </button>
            <button className={`ghost-tab ${mode === "vacancy" ? "is-active" : ""}`} type="button" onClick={() => onModeChange("vacancy")}>
              Вакансия → специалисты
            </button>
          </div>

          <div className="input-label">{mode === "bench" ? "Текст специалиста на бенче" : "Текст вакансии"}</div>
          <textarea
            className="editor"
            value={value}
            onChange={(event) => onChange(event.target.value)}
            placeholder={mode === "bench" ? "Вставьте описание специалиста..." : "Вставьте описание вакансии..."}
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
          <button key={`log-${item[2]}`} className="table-action" type="button" onClick={() => onOpen(item)} title="Показать лог">
            ⋯
          </button>
        ])}
        emptyMessage="Логи пока отсутствуют."
      />
    </section>
  );
}

function SettingsPage({ settings, theme, onThemeChange, onSettingsChange, onSave, onReset }) {
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

function ImportControlPanel({
  telegramChannels,
  onManualImport,
  onTelegramChannelSave,
  onTelegramVacanciesImport,
  onMatchingRebuild
}) {
  const [sourceKind, setSourceKind] = React.useState("text");
  const [forcedType, setForcedType] = React.useState("VACANCY");
  const [benchOrigin, setBenchOrigin] = React.useState("own");
  const [textValue, setTextValue] = React.useState("");
  const [urlValue, setUrlValue] = React.useState("");
  const [fileValue, setFileValue] = React.useState(null);
  const [channelForm, setChannelForm] = React.useState(defaultTelegramChannelForm);
  const [isSubmitting, setIsSubmitting] = React.useState(false);

  const submitImport = async () => {
    const payload =
      sourceKind === "text"
        ? textValue
        : sourceKind === "url"
          ? urlValue
          : fileValue;
    if (!payload) {
      return;
    }
    setIsSubmitting(true);
    try {
      await onManualImport({
        sourceKind,
        payload,
        forcedType,
        benchOrigin: forcedType === "BENCH" ? benchOrigin : null
      });
      setTextValue("");
      setUrlValue("");
      setFileValue(null);
    } finally {
      setIsSubmitting(false);
    }
  };

  const saveChannel = async () => {
    if (!channelForm.telegram_id || !channelForm.title.trim()) {
      return;
    }
    setIsSubmitting(true);
    try {
      await onTelegramChannelSave(channelForm);
      setChannelForm(defaultTelegramChannelForm);
    } finally {
      setIsSubmitting(false);
    }
  };

  return (
    <div className="settings-stack">
      <div className="panel-actions">
        <button className={`ghost-tab ${sourceKind === "text" ? "is-active" : ""}`} type="button" onClick={() => setSourceKind("text")}>
          Текст
        </button>
        <button className={`ghost-tab ${sourceKind === "url" ? "is-active" : ""}`} type="button" onClick={() => setSourceKind("url")}>
          Ссылка
        </button>
        <button className={`ghost-tab ${sourceKind === "file" ? "is-active" : ""}`} type="button" onClick={() => setSourceKind("file")}>
          Файл
        </button>
      </div>

      <div className="form-grid">
        <div className="field">
          <label>Что загружаем</label>
          <div className="panel-actions">
            <button className={`ghost-tab ${forcedType === "VACANCY" ? "is-active" : ""}`} type="button" onClick={() => setForcedType("VACANCY")}>
              Вакансия
            </button>
            <button className={`ghost-tab ${forcedType === "BENCH" ? "is-active" : ""}`} type="button" onClick={() => setForcedType("BENCH")}>
              Бенч
            </button>
          </div>
        </div>
        <div className="field">
          <label>Для бенча</label>
          <div className="panel-actions">
            <button className={`ghost-tab ${benchOrigin === "own" ? "is-active" : ""}`} type="button" onClick={() => setBenchOrigin("own")}>
              Наш бенч
            </button>
            <button className={`ghost-tab ${benchOrigin === "partner" ? "is-active" : ""}`} type="button" onClick={() => setBenchOrigin("partner")}>
              Партнёры
            </button>
          </div>
        </div>
      </div>

      {sourceKind === "text" ? (
        <textarea className="editor editor--compact" value={textValue} onChange={(event) => setTextValue(event.target.value)} placeholder="Вставь текст вакансии или бенча..." />
      ) : null}
      {sourceKind === "url" ? (
        <input className="standalone-input" value={urlValue} onChange={(event) => setUrlValue(event.target.value)} placeholder="https://..." />
      ) : null}
      {sourceKind === "file" ? (
        <input className="standalone-input" type="file" onChange={(event) => setFileValue(event.target.files?.[0] ?? null)} />
      ) : null}

      <div className="editor-footer">
        <button className="btn btn--primary" type="button" disabled={isSubmitting} onClick={submitImport}>
          Загрузить в базу
        </button>
        <button className="btn btn--ghost" type="button" disabled={isSubmitting} onClick={onTelegramVacanciesImport}>
          Подтянуть вакансии из TG
        </button>
        <button className="btn btn--ghost" type="button" disabled={isSubmitting} onClick={onMatchingRebuild}>
          Пересчитать релевантность
        </button>
      </div>

      <div className="form-grid">
        <Field
          label="Telegram ID"
          value={channelForm.telegram_id}
          onChange={(value) => setChannelForm((prev) => ({ ...prev, telegram_id: value }))}
          hint="ID канала/чата для коллектора"
        />
        <Field
          label="Название"
          value={channelForm.title}
          onChange={(value) => setChannelForm((prev) => ({ ...prev, title: value }))}
          hint="Как показывать канал в системе"
        />
        <Field
          label="Username"
          value={channelForm.username}
          onChange={(value) => setChannelForm((prev) => ({ ...prev, username: value }))}
          hint="@username, если есть"
        />
        <Field
          label="Тип канала"
          value={channelForm.source_kind}
          onChange={(value) => setChannelForm((prev) => ({ ...prev, source_kind: value }))}
          hint="vacancy/bench/chat"
        />
      </div>

      <div className="editor-footer">
        <button className="btn btn--ghost" type="button" disabled={isSubmitting} onClick={saveChannel}>
          Сохранить TG-канал
        </button>
      </div>

      <div className="compact-list">
        {telegramChannels.length ? (
          telegramChannels.map((item) => (
            <div key={item.telegram_id} className="compact-list__item">
              <div>
                <div className="compact-list__title">{item.title}</div>
                <div className="compact-list__text">
                  {item.username ? `@${item.username}` : "без username"} • {item.source_kind}
                </div>
              </div>
              <Badge tone={item.source_kind === "vacancy" ? "purple" : item.source_kind === "bench" ? "blue" : "gray"}>
                {item.source_kind}
              </Badge>
            </div>
          ))
        ) : (
          <InlineEmptyState text="Telegram-каналы пока не добавлены." />
        )}
      </div>
    </div>
  );
}

function FilterBar({ fields, buttonLabel, onSubmit }) {
  const [values, setValues] = React.useState(() => fields.map(() => ""));
  const fieldsSignature = fields.join("|");

  React.useEffect(() => {
    setValues(fields.map(() => ""));
  }, [fieldsSignature]);

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

function Notice({ text }) {
  return (
    <div className="notice-toast" role="status" aria-live="polite">
      {text}
    </div>
  );
}

function ActionDialog({ title, rows, onClose }) {
  return (
    <div className="dialog-backdrop" role="presentation" onClick={onClose}>
      <div className="dialog-card" role="dialog" aria-modal="true" aria-label={title} onClick={(event) => event.stopPropagation()}>
        <div className="dialog-card__head">
          <h3>{title}</h3>
          <button className="icon-button" type="button" onClick={onClose} title="Закрыть">
            ✕
          </button>
        </div>
        <div className="dialog-card__body">
          {rows.map(([label, value]) => (
            <div key={label} className="dialog-card__row">
              <span>{label}</span>
              <strong>{value || "—"}</strong>
            </div>
          ))}
        </div>
      </div>
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
