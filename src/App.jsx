import React from "react";
import {
  dashboardStats,
  navItems
} from "./data";

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

const miniNav = ["◌", "◍", "◼", "◎", "◀", "◊", "○", "◉", "◆", "✦"];
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
  "или",
  "для",
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

function buildManualMatchResult(vacancyText, benchText) {
  const vacancyKeywords = extractKeywords(vacancyText);
  const benchKeywords = extractKeywords(benchText);

  if (!vacancyText.trim() || !benchText.trim()) {
    return {
      title: "Недостаточно данных",
      description: "Заполните оба поля: текст вакансии и текст бенча.",
      overlap: [],
      score: 0
    };
  }

  if (!vacancyKeywords.length || !benchKeywords.length) {
    return {
      title: "Ключевые слова не выделены",
      description: "Добавьте больше фактуры: стек, грейд, домен, формат работы и вилку.",
      overlap: [],
      score: 0
    };
  }

  const vacancySet = new Set(vacancyKeywords);
  const benchSet = new Set(benchKeywords);
  const overlap = [...vacancySet].filter((word) => benchSet.has(word));

  const vacancyCoverage = overlap.length / vacancySet.size;
  const benchCoverage = overlap.length / benchSet.size;
  const score = Math.round((vacancyCoverage * 0.7 + benchCoverage * 0.3) * 100);

  let verdict = "Низкая релевантность";
  if (score >= 75) verdict = "Высокая релевантность";
  else if (score >= 50) verdict = "Средняя релевантность";

  const overlapPreview = overlap.slice(0, 10).join(", ");
  const description = overlap.length
    ? `Общие ключевые слова: ${overlapPreview}${overlap.length > 10 ? "..." : ""}.`
    : "Пересечений по ключевым словам не найдено.";

  return {
    title: `${verdict}: ${score}%`,
    description,
    overlap,
    score
  };
}

function App() {
  const [path, setPath] = React.useState(window.location.pathname);
  const [theme, setTheme] = React.useState(initialTheme);
  const [notice, setNotice] = React.useState("Интерфейс загружен. Все кнопки готовы к работе.");
  const [collections, setCollections] = React.useState(initialCollections);
  const [runVacancyText, setRunVacancyText] = React.useState("");
  const [runBenchText, setRunBenchText] = React.useState("");
  const [runResult, setRunResult] = React.useState(null);
  const [matchesSortOrder, setMatchesSortOrder] = React.useState("desc");
  const [settings, setSettings] = React.useState(defaultSettings);
  const [settingsTheme, setSettingsTheme] = React.useState(initialTheme);

  React.useEffect(() => {
    const onChange = () => setPath(window.location.pathname);
    window.addEventListener("popstate", onChange);
    return () => window.removeEventListener("popstate", onChange);
  }, []);

  React.useEffect(() => {
    document.documentElement.dataset.theme = theme;
    document.documentElement.style.colorScheme = theme;
  }, [theme]);

  const current = navItems.find((item) => item.path === path) ?? navItems[0];
  const sortedMatches = React.useMemo(() => {
    const copy = [...collections.matches];
    copy.sort((a, b) => (matchesSortOrder === "desc" ? b.score - a.score : a.score - b.score));
    return copy;
  }, [collections.matches, matchesSortOrder]);

  const showNotice = (message) => setNotice(message);

  const navigate = (nextPath) => {
    if (nextPath === window.location.pathname) {
      showNotice("Вы уже на этой странице.");
      return;
    }
    window.history.pushState({}, "", nextPath);
    setPath(nextPath);
    showNotice("Переход выполнен.");
  };

  const clearAllResults = React.useCallback(() => {
    setCollections(emptyCollections);
    setRunResult(null);
    showNotice("Результаты очищены.");
  }, []);

  const applyTheme = (nextTheme) => {
    setTheme(nextTheme);
    setSettingsTheme(nextTheme);
    showNotice(nextTheme === "dark" ? "Включена темная тема." : "Включена светлая тема.");
  };

  const railActions = React.useMemo(
    () => [
      { label: "Дашборд", onClick: () => navigate("/") },
      { label: "Входящие", onClick: () => navigate("/inbox") },
      { label: "Вакансии", onClick: () => navigate("/vacancies") },
      { label: "Бенч", onClick: () => navigate("/bench") },
      { label: "Совпадения", onClick: () => navigate("/matches") },
      { label: "Ручной прогон", onClick: () => navigate("/process") },
      { label: "Логи", onClick: () => navigate("/logs") },
      { label: "Настройки", onClick: () => navigate("/settings") },
      { label: "Очистить", onClick: clearAllResults },
      { label: "Светлая тема", onClick: () => applyTheme(theme === "dark" ? "light" : "dark") }
    ],
    [clearAllResults, theme]
  );

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

  const handlePageRefresh = (pageName) => {
    if (pageName === "run") {
      setRunResult(null);
    } else {
      setCollections((prev) => ({ ...prev, [pageName]: initialCollections[pageName] }));
    }
    showNotice("Страница обновлена.");
  };

  const handleSearch = (pageName, rawFilters) => {
    const filters = rawFilters.map(normalizeText).filter(Boolean);
    const source = initialCollections[pageName];

    if (!source) {
      showNotice("Для этой страницы поиск не настроен.");
      return;
    }

    const filtered = filters.length ? source.filter((item) => itemContainsFilters(item, filters)) : source;
    setCollections((prev) => ({ ...prev, [pageName]: filtered }));
    showNotice(
      filters.length
        ? filtered.length
          ? `Найдено записей: ${filtered.length}.`
          : "По запросу ничего не найдено."
        : "Фильтры очищены. Показаны все записи."
    );
  };

  const handleRunExample = (kind) => {
    if (kind === "vacancy") setRunVacancyText(exampleTexts.vacancy);
    if (kind === "bench") setRunBenchText(exampleTexts.bench);
    setRunResult(null);
    showNotice(kind === "vacancy" ? "Подставлен пример вакансии." : "Подставлен пример бенча.");
  };

  const handleRunProcess = () => {
    const result = buildManualMatchResult(runVacancyText, runBenchText);
    setRunResult(result);
    showNotice(result.score ? `Ручной прогон завершен. Релевантность: ${result.score}%.` : "Ручной прогон завершен.");
  };

  const handleMatchAction = (action, matchTitle) => {
    setCollections((prev) => ({
      ...prev,
      matches: prev.matches.filter((item) => item.title !== matchTitle)
    }));
    showNotice(action === "approve" ? "Совпадение подтверждено и убрано из списка." : "Совпадение отклонено и убрано из списка.");
  };

  const handleSettingsChange = (key, value) => {
    setSettings((prev) => ({ ...prev, [key]: value }));
  };

  const handleSettingsSave = () => {
    setTheme(settingsTheme);
    showNotice("Настройки сохранены.");
  };

  const handleSettingsReset = () => {
    setSettings(defaultSettings);
    setSettingsTheme(initialTheme);
    setTheme(initialTheme);
    showNotice("Настройки сброшены к значениям по умолчанию.");
  };

  const handleRowAction = (kind, label) => {
    const messages = {
      inbox: `Открыт источник сообщения: ${label}.`,
      vacancies: `Показаны детали вакансии: ${label}.`,
      bench: `Показан профиль специалиста: ${label}.`,
      logs: `Показана запись лога: ${label}.`
    };
    showNotice(messages[kind] ?? "Действие выполнено.");
  };

  return (
    <div className="app-shell">
      <aside className="rail">
        <div className="rail__spacer" />
        <div className="rail__icons">
          {miniNav.map((symbol, index) => (
            <button
              key={index}
              className="rail__icon"
              type="button"
              title={railActions[index].label}
              onClick={railActions[index].onClick}
            >
              {symbol}
            </button>
          ))}
        </div>
        <div className="rail__footer">GX</div>
      </aside>

      <aside className="sidebar">
        <div className="brand">
          <div className="brand__mark">⚡</div>
          <div>
            <div className="brand__title">Matcher</div>
            <div className="brand__subtitle">Bench & Vacancy</div>
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
              showNotice("Сессия завершена. Интерфейс возвращен в исходное состояние.");
            }}
          >
            ⌫ Выйти
          </button>
        </div>
      </aside>

      <main className="content">
        <TopBar
          notice={notice}
          onClear={clearAllResults}
          onToggleTheme={() => applyTheme(theme === "dark" ? "light" : "dark")}
        />
        {current.id === "dashboard" && <DashboardPage navigate={navigate} stats={dashboardStatsView} matches={sortedMatches} />}
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
            vacancyText={runVacancyText}
            benchText={runBenchText}
            result={runResult}
            onVacancyChange={setRunVacancyText}
            onBenchChange={setRunBenchText}
            onExample={handleRunExample}
            onClear={() => {
              setRunVacancyText("");
              setRunBenchText("");
              setRunResult(null);
              showNotice("Поля ручного прогона очищены.");
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

function TopBar({ notice, onClear, onToggleTheme }) {
  return (
    <header className="topbar topbar--stacked">
      <div className="topbar__row">
        <div className="topbar__left" />
        <div className="topbar__actions">
          <button type="button" className="icon-button" onClick={onClear} title="Очистить результаты">
            ✕
          </button>
          <button type="button" className="icon-button" onClick={onToggleTheme} title="Переключить тему">
            ◐
          </button>
        </div>
      </div>
      <div className="status-banner">{notice}</div>
    </header>
  );
}

function DashboardPage({ navigate, stats, matches }) {
  return (
    <section className="page">
      <PageHeading
        title="Дашборд"
        subtitle="Краткий обзор статуса матчинг-системы"
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

function RunPage({
  vacancyText,
  benchText,
  result,
  onVacancyChange,
  onBenchChange,
  onExample,
  onClear,
  onProcess
}) {
  return (
    <section className="page">
      <PageHeading title="Ручной прогон" subtitle="Сравнение бенча и вакансии с выделением ключевых слов" />
      <div className="split-grid">
        <Panel title="Входные данные" subtitle="">
          <div className="panel-actions">
            <button className="ghost-tab" type="button" onClick={() => onExample("vacancy")}>
              Пример вакансии
            </button>
            <button className="ghost-tab" type="button" onClick={() => onExample("bench")}>
              Пример бенча
            </button>
          </div>
          <div className="manual-input-grid">
            <div>
              <div className="input-label">Текст вакансии</div>
              <textarea
                className="editor editor--compact"
                value={vacancyText}
                onChange={(event) => onVacancyChange(event.target.value)}
                placeholder="Вставьте описание вакансии..."
              />
            </div>
            <div>
              <div className="input-label">Текст бенча</div>
              <textarea
                className="editor editor--compact"
                value={benchText}
                onChange={(event) => onBenchChange(event.target.value)}
                placeholder="Вставьте описание специалиста..."
              />
            </div>
          </div>
          <div className="editor-footer">
            <button className="btn btn--ghost" type="button" onClick={onClear}>
              Очистить
            </button>
            <button className="btn btn--primary" type="button" onClick={onProcess}>
              Сравнить
            </button>
          </div>
        </Panel>

        <Panel title="Результат" subtitle="">
          {result ? (
            <div className="result-card">
              <div className="result-card__title">{result.title}</div>
              <p>{result.description}</p>
              {result.overlap?.length ? (
                <div className="tag-list">
                  {result.overlap.slice(0, 12).map((word) => (
                    <Badge key={word} tone="blue">
                      {word}
                    </Badge>
                  ))}
                </div>
              ) : null}
            </div>
          ) : (
            <div className="empty-state">
              <div className="empty-state__icon">⚡</div>
              <p>Добавьте вакансию и бенч, затем запустите сравнение.</p>
            </div>
          )}
        </Panel>
      </div>
      <footer className="page-footer">© 2026 ООО "Пятый элемент". Все права защищены.</footer>
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

        <Panel title="Параметры матчинга" subtitle="Настройка алгоритма поиска совпадений">
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


