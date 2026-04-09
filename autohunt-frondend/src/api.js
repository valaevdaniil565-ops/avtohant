import { benchItems, inboxItems, logs, matches, vacancies } from "./data";

const API_BASE_URL = String(import.meta.env.VITE_API_BASE_URL ?? "").replace(/\/+$/, "");
const USE_MOCKS_ON_ERROR = import.meta.env.VITE_API_FALLBACK_TO_MOCKS !== "false";

function buildUrl(path, params) {
  const base = path.startsWith("http") ? path : `${API_BASE_URL}${path}`;
  const url = new URL(base, window.location.origin);

  if (params) {
    Object.entries(params).forEach(([key, value]) => {
      if (value === undefined || value === null || value === "") {
        return;
      }

      if (Array.isArray(value)) {
        value.forEach((item) => url.searchParams.append(key, item));
        return;
      }

      url.searchParams.set(key, value);
    });
  }

  return url.toString();
}

async function request(path, options = {}) {
  const { params, ...fetchOptions } = options;
  const response = await fetch(buildUrl(path, params), {
    headers: {
      Accept: "application/json",
      ...fetchOptions.headers
    },
    ...fetchOptions
  });

  if (!response.ok) {
    const errorText = await response.text();
    throw new Error(errorText || `Request failed with status ${response.status}`);
  }

  if (response.status === 204) {
    return null;
  }

  const contentType = response.headers.get("content-type") ?? "";
  if (!contentType.includes("application/json")) {
    return null;
  }

  return response.json();
}

function extractItems(payload) {
  if (Array.isArray(payload)) {
    return payload;
  }

  if (Array.isArray(payload?.items)) {
    return payload.items;
  }

  if (Array.isArray(payload?.results)) {
    return payload.results;
  }

  if (Array.isArray(payload?.data)) {
    return payload.data;
  }

  return [];
}

function toArray(value) {
  if (Array.isArray(value)) {
    return value.filter(Boolean);
  }

  if (typeof value === "string") {
    return value
      .split(",")
      .map((item) => item.trim())
      .filter(Boolean);
  }

  return [];
}

function toDisplayDate(value) {
  if (!value) {
    return "—";
  }

  return String(value);
}

function formatMoneyRange(min, max, currency = "RUB") {
  const symbol = currency === "RUB" ? "₽" : currency ?? "";
  if (min && max) {
    return `${min}–${max} ${symbol}`.trim();
  }
  if (max) {
    return `до ${max} ${symbol}`.trim();
  }
  if (min) {
    return `от ${min} ${symbol}`.trim();
  }
  return null;
}

function normalizeVacancy(item) {
  if (Array.isArray(item)) {
    return item;
  }

  const rate = item.rate
    ?? item.salary
    ?? item.compensation
    ?? formatMoneyRange(item.rate_min, item.rate_max, item.currency);

  return [
    item.company ?? item.client ?? item.customer ?? item.title ?? item.role ?? "—",
    toArray(item.stack ?? item.skills ?? item.tech_stack),
    item.grade ?? item.level ?? "—",
    rate ?? "—",
    item.status ?? "Активно",
    toDisplayDate(item.date ?? item.created_at ?? item.published_at)
  ];
}

function normalizeBench(item) {
  if (Array.isArray(item)) {
    return item;
  }

  const rate = item.rate
    ?? item.salary
    ?? item.compensation
    ?? formatMoneyRange(item.rate_min, item.rate_max, item.currency);

  return [
    item.name ?? item.candidate ?? item.specialist ?? item.role ?? item.source_display ?? "—",
    toArray(item.stack ?? item.skills ?? item.tech_stack),
    item.grade ?? item.level ?? "—",
    rate ?? "—",
    item.location ?? item.city ?? "—",
    item.status ?? "Активно",
    toDisplayDate(item.date ?? item.created_at ?? item.updated_at)
  ];
}

function normalizeInbox(item) {
  if (Array.isArray(item)) {
    return item;
  }

  return [
    item.type ?? item.kind ?? "Другое",
    item.source ?? item.chat ?? item.channel ?? "—",
    item.author ?? item.username ?? item.sender ?? "—",
    item.preview ?? item.text ?? item.summary ?? "—",
    item.status ?? "Активно",
    toDisplayDate(item.date ?? item.created_at ?? item.received_at)
  ];
}

function normalizeLog(item) {
  if (Array.isArray(item)) {
    return item;
  }

  return [
    item.status ?? item.result ?? "—",
    item.model ?? item.provider ?? "—",
    String(item.message_id ?? item.messageId ?? item.id ?? "—"),
    String(item.duration_ms ?? item.duration ?? item.latency_ms ?? "—"),
    toDisplayDate(item.date ?? item.created_at ?? item.timestamp)
  ];
}

function normalizeMatch(item) {
  if (item && !Array.isArray(item)) {
    const source = item.source ?? item.origin ?? (item.is_internal || item.is_own_bench_source ? "own_bench" : "partner_bench");
    const vacancyRole = item.vacancy?.role ?? item.vacancy_role ?? item.vacancy?.company ?? item.company ?? item.role ?? "—";
    const specialistRole = item.candidate?.role ?? item.specialist_role ?? item.specialist?.role ?? item.candidate?.name ?? item.specialist?.name ?? "—";
    const score = Number(item.score ?? item.similarity_score ?? 0);

    return {
      score,
      title: item.title ?? item.reason ?? `${specialistRole} → ${vacancyRole}`,
      vacancy: {
        company: item.vacancy?.company ?? item.company ?? item.vacancy_company ?? vacancyRole,
        stack: toArray(item.vacancy?.stack ?? item.vacancy_stack ?? item.vacancy_skills),
        grade: item.vacancy?.grade ?? item.vacancy_grade ?? "—",
        rate: item.vacancy?.rate ?? item.vacancy_rate ?? "—"
      },
      candidate: {
        name: item.candidate?.name ?? item.specialist?.name ?? item.candidate_name ?? specialistRole,
        stack: toArray(item.candidate?.stack ?? item.specialist?.stack ?? item.candidate_stack),
        grade: item.candidate?.grade ?? item.specialist?.grade ?? item.candidate_grade ?? "—",
        rate: item.candidate?.rate ?? item.specialist?.rate ?? item.candidate_rate ?? formatMoneyRange(item.rate_min, item.rate_max, item.currency)
      },
      status: item.status ?? "На проверке",
      date: item.date ?? toDisplayDate(item.created_at),
      source,
      priorityLabel:
        item.priorityLabel
        ?? item.priority_label
        ?? (source === "own_bench" ? "Наш специалист" : "Партнерский бенч")
    };
  }

  return {
    score: 0,
    title: "Автоматический мэтчинг",
    vacancy: { company: "—", stack: [], grade: "—", rate: "—" },
    candidate: { name: "—", stack: [], grade: "—", rate: "—" },
    status: "На проверке",
    date: "—",
    source: "partner_bench",
    priorityLabel: "Партнерский бенч"
  };
}

async function getList(path, normalizer, fallbackItems, params) {
  try {
    const payload = await request(path, { params });
    return extractItems(payload).map(normalizer);
  } catch (error) {
    if (!USE_MOCKS_ON_ERROR) {
      throw error;
    }

    console.warn(`API fallback for ${path}:`, error);
    return fallbackItems.map(normalizer);
  }
}

export async function getVacancies(params) {
  return getList("/api/vacancies", normalizeVacancy, vacancies, params);
}

export async function getBench(params) {
  return getList("/api/specialists", normalizeBench, benchItems, params);
}

export async function getInbox(params) {
  return getList("/api/inbox", normalizeInbox, inboxItems, params);
}

export async function getLogs(params) {
  return getList("/api/logs", normalizeLog, logs, params);
}

export async function getMatches(params) {
  try {
    const payload = await request("/api/matches", { params });
    return extractItems(payload).map(normalizeMatch);
  } catch (error) {
    if (!USE_MOCKS_ON_ERROR) {
      throw error;
    }

    console.warn("API fallback for /api/matches:", error);
    return matches.map((item, index) =>
      normalizeMatch({
        ...item,
        source: item.source ?? (index === 0 ? "own_bench" : "partner_bench"),
        priorityLabel: item.priorityLabel ?? (index === 0 ? "Наш специалист" : "Партнерский бенч")
      })
    );
  }
}

export async function getHealth() {
  return request("/api/health");
}

export const api = {
  getVacancies,
  getBench,
  getInbox,
  getLogs,
  getMatches,
  getHealth
};
