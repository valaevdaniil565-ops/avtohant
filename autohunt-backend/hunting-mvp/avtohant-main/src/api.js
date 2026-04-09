const API_BASE_URL = (import.meta.env.VITE_API_BASE_URL || "/api").replace(/\/+$/, "");

async function readJson(response) {
  const text = await response.text();
  if (!text) {
    return null;
  }
  try {
    return JSON.parse(text);
  } catch (error) {
    throw new Error(`Invalid JSON response (${response.status})`);
  }
}

async function request(path, options = {}) {
  const response = await fetch(`${API_BASE_URL}${path}`, {
    headers: {
      Accept: "application/json",
      ...(options.body instanceof FormData ? {} : { "Content-Type": "application/json" }),
      ...(options.headers || {})
    },
    ...options
  });
  const payload = await readJson(response);
  if (!response.ok) {
    const detail = payload && typeof payload === "object" ? payload.detail || payload.error : null;
    throw new Error(detail || `Request failed with status ${response.status}`);
  }
  return payload;
}

export async function fetchVacancies() {
  return request("/vacancies");
}

export async function fetchSpecialists() {
  return request("/specialists");
}

export async function fetchMatches() {
  return request("/matches");
}

export async function fetchOwnBenchStatus() {
  return request("/own-bench/status");
}

export async function submitTextImport(text, forcedType = null) {
  return request("/imports/text", {
    method: "POST",
    body: JSON.stringify({
      text,
      forced_type: forcedType
    })
  });
}

export async function submitUrlImport(url, forcedType = null) {
  return request("/imports/url", {
    method: "POST",
    body: JSON.stringify({
      url,
      forced_type: forcedType
    })
  });
}

export async function submitFileImport(file, forcedType = null) {
  const formData = new FormData();
  formData.append("file", file);
  if (forcedType) {
    formData.append("forced_type", forcedType);
  }
  return request("/imports/file", {
    method: "POST",
    body: formData
  });
}

export async function fetchImportJob(jobId) {
  return request(`/imports/${jobId}`);
}

export async function fetchRecentImportJobs(limit = 5) {
  return request(`/imports/recent-jobs?limit=${encodeURIComponent(limit)}`);
}

export { API_BASE_URL };
