from __future__ import annotations

from datetime import datetime
from typing import Any, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field


class SourceTraceItem(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    entity_type: Optional[str] = None
    entity_id: Optional[str] = None
    chat_title: Optional[str] = None
    sender_id: Optional[int] = None
    sender_name: Optional[str] = None
    message_url: Optional[str] = None
    external_url: Optional[str] = None
    external_kind: Optional[str] = None
    external_locator: Optional[str] = None
    source_type: Optional[str] = None
    raw_text: Optional[str] = None
    source_meta: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime


class EntitySourceResponse(BaseModel):
    entity_type: Literal["vacancy", "specialist"]
    entity_id: str
    items: list[SourceTraceItem]


class EntityBase(BaseModel):
    id: str
    role: str
    stack: list[str] = Field(default_factory=list)
    grade: Optional[str] = None
    experience_years: Optional[int] = None
    rate_min: Optional[int] = None
    rate_max: Optional[int] = None
    currency: Optional[str] = None
    location: Optional[str] = None
    description: Optional[str] = None
    original_text: Optional[str] = None
    status: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    expires_at: Optional[datetime] = None
    source_url: Optional[str] = None
    source_display: Optional[str] = None


class VacancyItem(EntityBase):
    company: Optional[str] = None
    is_strategic: Optional[bool] = None
    close_reason: Optional[str] = None
    closed_at: Optional[datetime] = None


class SpecialistItem(EntityBase):
    is_internal: Optional[bool] = None
    hired_at: Optional[datetime] = None


class VacancyListResponse(BaseModel):
    items: list[VacancyItem]
    total: int
    limit: int
    offset: int


class SpecialistListResponse(BaseModel):
    items: list[SpecialistItem]
    total: int
    limit: int
    offset: int


class VacancyDetailResponse(BaseModel):
    item: VacancyItem
    sources: list[SourceTraceItem]


class SpecialistDetailResponse(BaseModel):
    item: SpecialistItem
    sources: list[SourceTraceItem]


class MatchScoreComponents(BaseModel):
    semantic_score: float
    secondary_score: float
    grade_score: float
    rate_score: float
    location_score: float
    final_score: float
    stack_overlap: list[str] = Field(default_factory=list)


class MatchHit(BaseModel):
    id: str
    role: str
    stack: list[str] = Field(default_factory=list)
    grade: Optional[str] = None
    rate_min: Optional[int] = None
    rate_max: Optional[int] = None
    currency: Optional[str] = None
    location: Optional[str] = None
    is_internal: Optional[bool] = None
    is_own_bench_source: Optional[bool] = None
    source_url: Optional[str] = None
    source_display: Optional[str] = None
    score: float
    score_components: MatchScoreComponents


class MatchListResponse(BaseModel):
    entity_type: Literal["vacancy", "specialist"]
    entity_id: str
    items: list[MatchHit]
    total: int


class TextImportRequest(BaseModel):
    text: str
    forced_type: Optional[Literal["VACANCY", "BENCH"]] = None
    bench_origin: Optional[Literal["own", "partner"]] = None


class UrlImportRequest(BaseModel):
    url: str
    forced_type: Optional[Literal["VACANCY", "BENCH"]] = None
    bench_origin: Optional[Literal["own", "partner"]] = None


class HideBySourceRequest(BaseModel):
    source_ref: str


class ImportEntityRef(BaseModel):
    entity_type: Literal["vacancy", "specialist"]
    entity_id: str


class ImportSummaryResponse(BaseModel):
    vacancies: int = 0
    specialists: int = 0
    skipped: int = 0
    hidden: int = 0
    errors: list[str] = Field(default_factory=list)
    entity_refs: list[ImportEntityRef] = Field(default_factory=list)


class ImportJobAcceptedResponse(BaseModel):
    job_id: str
    status: str


class ImportImmediateResponse(BaseModel):
    status: str
    summary: ImportSummaryResponse


class ImportJobStatusResponse(BaseModel):
    job_id: str
    kind: str
    status: str
    submitted_at: datetime
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    error: Optional[str] = None
    summary: ImportSummaryResponse


class ImportJobListResponse(BaseModel):
    items: list[ImportJobStatusResponse]


class JobStatusResponse(BaseModel):
    job_id: str
    kind: str
    status: str
    submitted_at: datetime
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    error: Optional[str] = None
    result: dict[str, Any] = Field(default_factory=dict)


class JobListItemResponse(JobStatusResponse):
    attempts: int = 0
    max_attempts: int = 0
    available_at: Optional[datetime] = None


class JobListResponse(BaseModel):
    items: list[JobListItemResponse]
    total: int
    limit: int
    kind: Optional[str] = None
    status: Optional[str] = None


class JobRetryResponse(BaseModel):
    job_id: str
    status: str


class HideBySourceResponse(BaseModel):
    source_ref: str
    matched_sources: int
    hidden_entities: int


class OwnBenchStatusPayload(BaseModel):
    last_success_at: Optional[str] = None
    last_success_label: Optional[str] = None
    last_error: Optional[str] = None
    stats: dict[str, Any] = Field(default_factory=dict)
    active_rows: Optional[int] = None
    empty_text: str


class OwnBenchStatusResponse(BaseModel):
    status: str
    sync: OwnBenchStatusPayload


class SettingItem(BaseModel):
    key: str
    value: str
    source: str
    updated_at: Optional[datetime] = None


class SettingsResponse(BaseModel):
    items: list[SettingItem]


class SettingUpdateItem(BaseModel):
    key: str
    value: str


class SettingsUpdateRequest(BaseModel):
    items: list[SettingUpdateItem]


class SettingsUpdateResponse(BaseModel):
    updated: list[SettingItem]


class AuditEventItem(BaseModel):
    id: str
    event_type: str
    actor: str
    entity_type: Optional[str] = None
    entity_id: Optional[str] = None
    payload: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime


class AuditListResponse(BaseModel):
    items: list[AuditEventItem]
    total: int
    limit: int


class DigestSectionItem(BaseModel):
    id: str
    role: Optional[str] = None
    grade: Optional[str] = None
    stack: list[str] = Field(default_factory=list)
    location: Optional[str] = None
    status: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    url: Optional[str] = None
    source_display: Optional[str] = None


class DigestPreviewResponse(BaseModel):
    window_start: datetime
    window_end: datetime
    new_vacancies: list[DigestSectionItem]
    updated_vacancies: list[DigestSectionItem]
    new_specialists: list[DigestSectionItem]
    updated_specialists: list[DigestSectionItem]


class AdminOverviewResponse(BaseModel):
    status: str
    database: str
    counts: dict[str, int] = Field(default_factory=dict)
    jobs: dict[str, int] = Field(default_factory=dict)
    own_bench: dict[str, Any] = Field(default_factory=dict)
    recent_imports: list[dict[str, Any]] = Field(default_factory=list)
    recent_sources: list[dict[str, Any]] = Field(default_factory=list)


class TelegramChannelItem(BaseModel):
    telegram_id: int
    title: str
    username: Optional[str] = None
    source_kind: Literal["chat", "vacancy", "bench"] = "chat"
    is_active: bool = True
    last_message_id: int = 0
    added_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class TelegramChannelUpsertRequest(BaseModel):
    telegram_id: int
    title: str
    username: Optional[str] = None
    source_kind: Literal["chat", "vacancy", "bench"] = "chat"
    is_active: bool = True


class TelegramChannelListResponse(BaseModel):
    items: list[TelegramChannelItem]


class TelegramImportResponse(BaseModel):
    status: str
    selected_messages: int = 0
    imported_vacancies: int = 0
    skipped: int = 0
    hidden: int = 0
    errors: list[str] = Field(default_factory=list)
    entity_refs: list[ImportEntityRef] = Field(default_factory=list)


class MatchingRebuildResponse(BaseModel):
    status: str
    processed_vacancies: int = 0
    updated_matches: int = 0
