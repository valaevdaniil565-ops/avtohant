import { benchItems, inboxItems, logs, matches, vacancies } from "./data";

const DEV_DEFAULT_API_BASE_URL = "http://127.0.0.1:8000";

function resolveApiBaseUrl() {
  const explicitBaseUrl = String(import.meta.env.VITE_API_BASE_URL ?? "").trim();
  if (explicitBaseUrl) {
    return explicitBaseUrl.replace(/\/+$/, "");
  }

  if (typeof window !== "undefined") {
    const { hostname } = window.location;
    if (hostname === "127.0.0.1" || hostname === "localhost") {
      return DEV_DEFAULT_API_BASE_URL;
    }
  }

  return import.meta.env.DEV ? DEV_DEFAULT_API_BASE_URL : "";
}

const API_BASE_URL = resolveApiBaseUrl();
const USE_MOCKS_ON_ERROR = import.meta.env.VITE_API_FALLBACK_TO_MOCKS === "true";

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

function repairMojibake(value) {
  const text = String(value ?? "");
  if (!text) {
    return "";
  }

  try {
    const originalCyrillic = (text.match(/[А-Яа-яЁё]/g) ?? []).length;
    const candidates = [text];

    if (/[ÐÑ]/.test(text)) {
      const utf8FromLatin1 = new TextDecoder("utf-8", { fatal: false }).decode(
        Uint8Array.from(Array.from(text, (char) => char.charCodeAt(0) & 0xff))
      );
      candidates.push(utf8FromLatin1);
    }

    return candidates.reduce((best, candidate) => {
      const score = (candidate.match(/[А-Яа-яЁё]/g) ?? []).length;
      const penalty = /[ÐÑ]|Р[А-я]|С[А-я]|Ѓ|�/.test(candidate) ? 1000 : 0;
      const bestScore = (best.match(/[А-Яа-яЁё]/g) ?? []).length;
      const bestPenalty = /[ÐÑ]|Р[А-я]|С[А-я]|Ѓ|�/.test(best) ? 1000 : 0;
      return score - penalty > bestScore - bestPenalty ? candidate : best;
    }, originalCyrillic ? text : candidates[0]);
  } catch {
    return text;
  }
}

function looksBrokenText(value) {
  const text = String(value ?? "").trim();
  if (!text) {
    return false;
  }

  if (/[ÐÑ]|Р[А-яЁё]|С[А-яЁё]|Ѓ|�/.test(text)) {
    return true;
  }

  const punctuationMatches = text.match(/[=;:<>{}\[\]\\]/g) ?? [];
  const cyrillicMatches = text.match(/[А-Яа-яЁё]/g) ?? [];
  const wordMatches = text.match(/[A-Za-zА-Яа-яЁё]{2,}/g) ?? [];

  if (punctuationMatches.length >= 3 && cyrillicMatches.length === 0) {
    return true;
  }

  if (wordMatches.length === 0 && punctuationMatches.length >= 2) {
    return true;
  }

  return false;
}

function toReadableText(value) {
  const repaired = repairMojibake(value).trim();
  if (!repaired || looksBrokenText(repaired)) {
    return "";
  }
  return repaired;
}

function toArray(value) {
  if (Array.isArray(value)) {
    return value.filter(Boolean).map((item) => toReadableText(item)).filter(Boolean);
  }

  if (typeof value === "string") {
    return toReadableText(value)
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

  return toReadableText(value) || "—";
}

function extractUrls(value) {
  return Array.from(String(value ?? "").matchAll(/https?:\/\/\S+/gi)).map((match) => match[0]);
}

function isTelegramUrl(value) {
  return /https?:\/\/t\.me\//i.test(String(value ?? "").trim());
}

function pickActualSpecialistUrl(...values) {
  const urls = values.flatMap((value) => extractUrls(value));
  const nonTelegram = urls.find((url) => !isTelegramUrl(url));
  if (nonTelegram) {
    return nonTelegram;
  }
  const explicit = values
    .map((value) => String(value ?? "").trim())
    .find((value) => value && /^https?:\/\//i.test(value) && !isTelegramUrl(value));
  return explicit ?? "";
}

function attachMetadata(row, item) {
  if (Array.isArray(row) && item && typeof item === "object") {
    row.meta = item;
  }
  return row;
}

function normalizeMatchScore(value) {
  const rawScore = Number(value ?? 0);
  if (!Number.isFinite(rawScore) || rawScore <= 0) {
    return 0;
  }

  const percentScore = rawScore <= 1 ? rawScore * 100 : rawScore;
  return Math.max(1, Math.min(100, Math.round(percentScore)));
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

function firstNonEmptyString(...values) {
  for (const value of values) {
    const text = toReadableText(value);
    if (text) {
      return text;
    }
  }
  return "";
}

function deriveVacancyOwner(item) {
  return firstNonEmptyString(
    item.source_meta?.manager_name,
    item.source_meta?.source_sender_name,
    item.sender_name,
    item.chat_title,
    item.author,
    "—"
  );
}

function deriveSpecialistName(item) {
  return firstNonEmptyString(
    item.name,
    item.candidate,
    item.specialist,
    item.source_meta?.structured_fields?.name,
    item.source_meta?.row_map?.name,
    item.source_meta?.source_person_name,
    "—"
  );
}

function deriveSpecialistRole(item) {
  return firstNonEmptyString(
    item.role,
    item.title,
    item.source_meta?.structured_fields?.role,
    item.source_meta?.row_map?.role,
    toArray(item.stack ?? item.skills ?? item.tech_stack)[0],
    "—"
  );
}

function extractLabeledValue(text, labels) {
  const source = repairMojibake(text);
  if (!source) {
    return "";
  }

  for (const label of labels) {
    const pattern = new RegExp(`${label}\\s*[:\\-]\\s*([^\\n;]+)`, "i");
    const match = source.match(pattern);
    if (match?.[1]) {
      const candidate = toReadableText(match[1]);
      if (candidate) {
        return candidate;
      }
    }
  }

  return "";
}

function parseSpecialistName(item, fallbackName) {
  const sourceMeta = item.specialist_source_meta && typeof item.specialist_source_meta === "object" ? item.specialist_source_meta : {};
  const explicitName = firstNonEmptyString(
    item.candidate?.displayName,
    item.candidate?.name,
    item.specialist?.displayName,
    item.specialist?.name,
    item.candidate_name,
    sourceMeta.source_person_name,
    sourceMeta.structured_fields?.name,
    sourceMeta.row_map?.name
  );
  if (explicitName) {
    return explicitName;
  }

  const sourceName = extractLabeledValue(item.specialist_source_display, ["Специалист", "Кандидат", "Имя", "Name"]);
  if (sourceName) {
    return sourceName;
  }

  const rawName = extractLabeledValue(item.specialist_original_text, ["Имя", "Name"]);
  if (rawName) {
    return rawName;
  }

  return fallbackName;
}

function buildReadableLiveBenchReference(item) {
  const sheet = toReadableText(item.specialist_live_sheet_name);
  const row = Number(item.specialist_live_row_index ?? 0);
  const parts = [];
  if (sheet) {
    parts.push(`Вкладка: ${sheet}`);
  }
  if (Number.isFinite(row) && row > 0) {
    parts.push(`Строка: ${row}`);
  }
  return parts.join(" · ");
}

function extractSourceIndex(sourceMeta, sourceDisplay) {
  const directValue = toReadableText(sourceMeta?.source_index);
  if (directValue) {
    return directValue;
  }

  const displayValue = String(sourceDisplay ?? "");
  const match = displayValue.match(/(?:Индекс|Index)\s*:\s*([^\s;]+)/i);
  return match?.[1]?.trim() ?? "";
}

function buildCandidateReference(item, candidateName, candidateLocation) {
  const sourceMeta = item.specialist_source_meta && typeof item.specialist_source_meta === "object" ? item.specialist_source_meta : {};
  const metaSheet = firstNonEmptyString(sourceMeta.sheet_name, sourceMeta.table_name);
  const metaRow = Number(sourceMeta.external_row_index ?? sourceMeta.row_index ?? 0);
  const liveName = firstNonEmptyString(
    item.specialist_live_name,
    sourceMeta.source_person_name,
    sourceMeta.structured_fields?.name,
    sourceMeta.row_map?.name,
    candidateName
  );
  const liveLocation = firstNonEmptyString(
    item.specialist_live_location,
    sourceMeta.structured_fields?.location,
    sourceMeta.row_map?.location,
    candidateLocation
  );
  const identityParts = [];
  const referenceParts = [];
  const liveReference = buildReadableLiveBenchReference(item);
  const sourceKind = String(sourceMeta?.source_kind ?? "").trim().toLowerCase();
  const isSheetLike = Boolean(
    metaSheet
    || metaRow > 0
    || sourceKind === "google_sheet"
    || sourceKind === "sheet"
    || sourceKind === "spreadsheet"
    || sourceKind === "file"
  );

  if (liveName && liveName !== "—") {
    identityParts.push(liveName);
  }
  if (liveLocation && liveLocation !== "—") {
    identityParts.push(liveLocation);
  }

  if (liveReference) {
    referenceParts.push(liveReference);
  } else if (isSheetLike) {
    if (metaSheet) {
      referenceParts.push(`Вкладка: ${metaSheet}`);
    }
    if (Number.isFinite(metaRow) && metaRow > 0) {
      referenceParts.push(`Строка: ${metaRow}`);
    }
  }

  return {
    identity: identityParts.join(" · "),
    reference: referenceParts.join(" · ")
  };
}

function formatLiveBenchReference(item) {
  const sheet = toReadableText(item.specialist_live_sheet_name);
  const row = Number(item.specialist_live_row_index ?? 0);
  const parts = [];
  if (sheet) {
    parts.push(`Вкладка: ${sheet}`);
  }
  if (Number.isFinite(row) && row > 0) {
    parts.push(`Строка: ${row}`);
  }
  return parts.join(" · ");
}

function normalizeVacancy(item) {
  if (Array.isArray(item)) {
    return attachMetadata([
      "—",
      firstNonEmptyString(item[0], "—"),
      firstNonEmptyString(Array.isArray(item[1]) ? item[1][0] : item[1], item[0], "—"),
      firstNonEmptyString(item[2], "—"),
      firstNonEmptyString(item[3], "—"),
      firstNonEmptyString(item[4], "Активно"),
      firstNonEmptyString(item[5], "—")
    ], { legacy: true });
  }

  const rate = item.rate
    ?? item.salary
    ?? item.compensation
    ?? formatMoneyRange(item.rate_min, item.rate_max, item.currency);

  return attachMetadata([
    deriveVacancyOwner(item),
    firstNonEmptyString(item.title, item.role, item.company, item.client, item.customer, "—"),
    firstNonEmptyString(item.role, item.title, toArray(item.stack ?? item.skills ?? item.tech_stack)[0], "—"),
    firstNonEmptyString(item.grade, item.level, "—"),
    firstNonEmptyString(rate, "—"),
    firstNonEmptyString(item.status, "Активно"),
    toDisplayDate(item.published_at ?? item.date ?? item.created_at ?? item.updated_at)
  ], item);
}

function normalizeBench(item) {
  if (Array.isArray(item)) {
    return attachMetadata([
      firstNonEmptyString(item[0], "—"),
      firstNonEmptyString(Array.isArray(item[1]) ? item[1][0] : item[1], "—"),
      firstNonEmptyString(item[2], "—"),
      firstNonEmptyString(item[5], item[3], "Активно")
    ], { legacy: true });
  }

  return attachMetadata([
    deriveSpecialistName(item),
    deriveSpecialistRole(item),
    firstNonEmptyString(item.grade, item.level, "—"),
    firstNonEmptyString(item.status, "Активно")
  ], item);
}

function normalizeInbox(item) {
  if (Array.isArray(item)) {
    return item;
  }

  return attachMetadata([
    firstNonEmptyString(item.type, item.kind, "Другое"),
    firstNonEmptyString(item.source, item.chat, item.channel, "—"),
    firstNonEmptyString(item.author, item.username, item.sender, "—"),
    firstNonEmptyString(item.preview, item.text, item.summary, "—"),
    firstNonEmptyString(item.status, "Активно"),
    toDisplayDate(item.date ?? item.created_at ?? item.received_at)
  ], item);
}

function normalizeLog(item) {
  if (Array.isArray(item)) {
    return item;
  }

  return [
    firstNonEmptyString(item.status, item.result, "—"),
    firstNonEmptyString(item.model, item.provider, "—"),
    repairMojibake(item.message_id ?? item.messageId ?? item.id ?? "—"),
    repairMojibake(item.duration_ms ?? item.duration ?? item.latency_ms ?? "—"),
    toDisplayDate(item.date ?? item.created_at ?? item.timestamp)
  ];
}

function normalizeMatch(item) {
  if (item && !Array.isArray(item)) {
    const source = item.source ?? item.origin ?? (item.is_internal || item.is_own_bench_source ? "own_bench" : "partner_bench");
    const vacancyRole = firstNonEmptyString(item.vacancy?.role, item.vacancy_role, item.role);
    const vacancyCompany = firstNonEmptyString(item.vacancy?.company, item.company, item.vacancy_company, vacancyRole, "—");
    const specialistRole = firstNonEmptyString(
      item.candidate?.role,
      item.specialist_role,
      item.specialist?.role,
      item.candidate_stack?.[0],
      item.specialist_stack?.[0],
      "—"
    );
    const specialistName = parseSpecialistName(item, specialistRole);
    const matchTitle = `${specialistName || specialistRole || "—"} → ${vacancyCompany || vacancyRole || "—"}`;
    const specialistLocation = firstNonEmptyString(
      item.specialist_live_location,
      item.candidate?.location,
      item.specialist?.location,
      item.specialist_location,
      extractLabeledValue(item.specialist_original_text, ["Локация", "Location", "Город", "City"])
    ) || "—";
    const candidateReference = buildCandidateReference(item, specialistName, specialistLocation);
    const specialistSourceUrl = pickActualSpecialistUrl(
      item.specialist_live_resume_url,
      item.specialist_live_source_url,
      item.candidate?.sourceUrl,
      item.candidate?.source_url,
      item.specialist?.sourceUrl,
      item.specialist?.source_url,
      item.specialist_source_url,
      item.specialist_source_display,
      item.specialist_original_text
    ) || null;
    const score = normalizeMatchScore(item.score ?? item.similarity_score ?? 0);
    const vacancyRate = item.vacancy?.rate
      ?? item.vacancy_rate
      ?? formatMoneyRange(item.vacancy_rate_min, item.vacancy_rate_max, item.vacancy_currency);
    const candidateRate = item.candidate?.rate
      ?? item.specialist?.rate
      ?? item.candidate_rate
      ?? item.specialist_rate
      ?? formatMoneyRange(item.specialist_rate_min, item.specialist_rate_max, item.specialist_currency)
      ?? formatMoneyRange(item.rate_min, item.rate_max, item.currency);

    return {
      score,
      title: matchTitle,
      vacancy: {
        company: vacancyCompany,
        stack: toArray(item.vacancy?.stack ?? item.vacancy_stack ?? item.vacancy_skills),
        grade: firstNonEmptyString(item.vacancy?.grade, item.vacancy_grade, "—"),
        rate: firstNonEmptyString(vacancyRate, "—"),
        sourceUrl: item.vacancy?.sourceUrl ?? item.vacancy?.source_url ?? item.vacancy_source_url ?? item.source_url ?? null
      },
      candidate: {
        name: specialistName,
        role: specialistRole,
        stack: toArray(item.candidate?.stack ?? item.specialist?.stack ?? item.candidate_stack ?? item.specialist_stack),
        grade: firstNonEmptyString(item.candidate?.grade, item.specialist?.grade, item.candidate_grade, item.specialist_grade, "—"),
        rate: firstNonEmptyString(candidateRate, "—"),
        location: specialistLocation,
        identityHint: candidateReference.identity,
        benchReference: candidateReference.reference,
        sourceDisplay: firstNonEmptyString(item.candidate?.sourceDisplay, item.specialist?.sourceDisplay, item.specialist_source_display, ""),
        sourceUrl: specialistSourceUrl
      },
      status: firstNonEmptyString(item.status, "На проверке"),
      date: item.date ?? toDisplayDate(item.created_at),
      source,
      priorityLabel:
        firstNonEmptyString(
          item.priorityLabel,
          item.priority_label,
          source === "own_bench" ? "Наш специалист" : "Партнерский бенч"
        )
    };
  }

  return {
    score: 0,
    title: "Автоматический мэтчинг",
    vacancy: { company: "—", stack: [], grade: "—", rate: "—" },
    candidate: { name: "—", role: "—", stack: [], grade: "—", rate: "—", location: "—", identityHint: "", benchReference: "" },
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

export async function importTextSync(payload) {
  return request("/api/imports/text-sync", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload)
  });
}

export async function importUrlSync(payload) {
  return request("/api/imports/url-sync", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload)
  });
}

export async function importFileSync({ file, forcedType, benchOrigin }) {
  const formData = new FormData();
  formData.append("file", file);
  if (forcedType) {
    formData.append("forced_type", forcedType);
  }
  if (benchOrigin) {
    formData.append("bench_origin", benchOrigin);
  }
  return request("/api/imports/file-sync", {
    method: "POST",
    body: formData
  });
}

export async function getTelegramChannels() {
  return request("/api/admin/telegram/channels");
}

export async function upsertTelegramChannel(payload) {
  return request("/api/admin/telegram/channels", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload)
  });
}

export async function importTelegramVacancies(params) {
  return request("/api/admin/telegram/import-vacancies", {
    method: "POST",
    params
  });
}

export async function rebuildMatching(params) {
  return request("/api/admin/matching/rebuild", {
    method: "POST",
    params
  });
}

export async function getPartnerBenchSources() {
  return request("/api/admin/partner-bench-sources");
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

export async function runManualMatch(payload) {
  return request("/api/manual-run", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload)
  });
}

export async function getSettings() {
  return request("/api/settings");
}

export async function updateSettings(payload) {
  return request("/api/settings", {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload)
  });
}

export async function runOwnBenchSync() {
  return request("/api/admin/jobs/own-bench-sync", {
    method: "POST"
  });
}

export const api = {
  getVacancies,
  getBench,
  importTextSync,
  importUrlSync,
  importFileSync,
  getTelegramChannels,
  upsertTelegramChannel,
  importTelegramVacancies,
  rebuildMatching,
  getPartnerBenchSources,
  getInbox,
  getLogs,
  getMatches,
  getHealth,
  runManualMatch,
  getSettings,
  updateSettings,
  runOwnBenchSync
};





