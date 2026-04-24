import { createHmac, timingSafeEqual } from "node:crypto";
import {
  definePlugin,
  runWorker,
  type PluginContext,
  type PluginEvent,
  type PluginWebhookInput,
  type PluginHealthDiagnostics,
} from "@paperclipai/plugin-sdk";
import { WEBHOOK_KEYS, STATE_KEYS, PLUGIN_ID } from "./constants.js";
import { postMessage, respondToAction, respondEphemeral } from "./slack-api.js";
import type { SlackMessage } from "./slack-api.js";
import type { SlackConfig, EscalationRecord, CommandDefinition, SessionEntry } from "./types.js";
import { SlackAdapter } from "./adapter.js";
import {
  spawnAgent,
  closeAgent,
  routeMessageToAgent,
  handleAgentOutput,
  handleHandoffAction,
  handleDiscussionAction,
  handleAcpSlashCommand,
  startDiscussion,
  buildHandoffBlocks,
} from "./acp-bridge.js";
import {
  setBaseUrl,
  setCompanyPrefix,
  prependMentions,
  formatIssueCreated,
  formatIssueDone,
  formatApprovalCreated,
  formatApprovalResubmitted,
  formatApprovalResolved,
  formatAgentError,
  formatAgentConnected,
  formatBudgetThreshold,
  formatOnboardingMilestone,
  formatDailyDigest,
  formatEscalationMessage,
  formatEscalationResolved,
} from "./formatters.js";
import { processMediaFile, isMediaFile } from "./media-pipeline.js";
import {
  registerCommand,
  handleCommandsSlash,
  tryCustomCommand,
  parseCommand,
} from "./custom-commands.js";
import {
  registerWatch,
  removeWatch,
  listWatches,
  checkWatches,
  BUILTIN_WATCH_TEMPLATES,
} from "./proactive-suggestions.js";

let pluginCtx: PluginContext;
let pluginToken: string;
let pluginConfig: SlackConfig;
let slackAdapter: SlackAdapter;
// Paperclip board API key, resolved from paperclipApiKeyRef at startup.
// Used to authenticate privileged calls (approve/reject) that require
// assertBoard. Empty when not configured — dependent actions will fail.
let paperclipApiKey = "";

// --- Team directory ---
// Sourced from the `team-directory` company skill's fenced JSON block.
// Cached in-memory for TEAM_DIRECTORY_TTL_MS per companyId.
//
// Supports two structures:
//   1. Nested (new): { users: {...}, labels: {...}, _default: {userId} }
//   2. Flat (legacy): { <labelName>: {slackUserId, ...} }
// fetchTeamDirectory normalises to the nested shape.

type TeamDirectoryOwner = {
  name?: string;
  slackUserId?: string;
  slackUserIds?: string[];
  githubUsername?: string;
  githubUsernames?: string[];
};
type TeamDirectory = {
  users: Record<string, TeamDirectoryOwner>;
  labels: Record<string, TeamDirectoryOwner>;
  defaultUserId: string | null;
};

const EMPTY_DIRECTORY: TeamDirectory = { users: {}, labels: {}, defaultUserId: null };
const TEAM_DIRECTORY_TTL_MS = 5 * 60 * 1000;
const teamDirectoryCache = new Map<string, { data: TeamDirectory; expiresAt: number }>();

function normaliseTeamDirectory(raw: unknown): TeamDirectory {
  if (!raw || typeof raw !== "object") return EMPTY_DIRECTORY;
  const obj = raw as Record<string, unknown>;

  // New nested structure
  if (obj.users || obj.labels || obj._default) {
    const users = (obj.users as Record<string, TeamDirectoryOwner> | undefined) ?? {};
    const labels = (obj.labels as Record<string, TeamDirectoryOwner> | undefined) ?? {};
    const defaultBlock = obj._default as { userId?: string } | undefined;
    return {
      users: users ?? {},
      labels: labels ?? {},
      defaultUserId: defaultBlock?.userId ?? null,
    };
  }

  // Legacy flat structure — every key is a label
  const labels: Record<string, TeamDirectoryOwner> = {};
  for (const [k, v] of Object.entries(obj)) {
    if (v && typeof v === "object") labels[k] = v as TeamDirectoryOwner;
  }
  return { users: {}, labels, defaultUserId: null };
}

async function fetchTeamDirectory(companyId: string): Promise<TeamDirectory> {
  const now = Date.now();
  const cached = teamDirectoryCache.get(companyId);
  if (cached && cached.expiresAt > now) return cached.data;

  const authHeaders: Record<string, string> = paperclipApiKey
    ? { Authorization: `Bearer ${paperclipApiKey}` }
    : {};

  try {
    const listRes = await pluginCtx.http.fetch(
      `${pluginConfig.paperclipBaseUrl}/api/companies/${companyId}/skills`,
      { headers: authHeaders },
    );
    if (!listRes.ok) {
      pluginCtx.logger.warn("team-directory: skills list fetch failed", {
        status: listRes.status,
        companyId,
      });
      return cacheAndReturn(companyId, EMPTY_DIRECTORY);
    }
    const skills = (await listRes.json()) as Array<{ id: string; slug: string }>;
    const match = skills.find((s) => s.slug === "team-directory");
    if (!match) return cacheAndReturn(companyId, EMPTY_DIRECTORY);

    const detailRes = await pluginCtx.http.fetch(
      `${pluginConfig.paperclipBaseUrl}/api/companies/${companyId}/skills/${match.id}`,
      { headers: authHeaders },
    );
    if (!detailRes.ok) {
      pluginCtx.logger.warn("team-directory: skill detail fetch failed", {
        status: detailRes.status,
        skillId: match.id,
      });
      return cacheAndReturn(companyId, EMPTY_DIRECTORY);
    }
    const skill = (await detailRes.json()) as { markdown?: string };
    const md = skill.markdown ?? "";
    const fenced = md.match(/```json\s*\n([\s\S]*?)\n```/);
    if (!fenced) return cacheAndReturn(companyId, EMPTY_DIRECTORY);
    try {
      const parsed = JSON.parse(fenced[1]);
      return cacheAndReturn(companyId, normaliseTeamDirectory(parsed));
    } catch (parseErr) {
      pluginCtx.logger.warn("team-directory: JSON parse failed", {
        err: String(parseErr),
      });
      return cacheAndReturn(companyId, EMPTY_DIRECTORY);
    }
  } catch (err) {
    pluginCtx.logger.warn("team-directory: fetch error", { err: String(err), companyId });
    return cacheAndReturn(companyId, EMPTY_DIRECTORY);
  }
}

function cacheAndReturn(companyId: string, data: TeamDirectory): TeamDirectory {
  teamDirectoryCache.set(companyId, { data, expiresAt: Date.now() + TEAM_DIRECTORY_TTL_MS });
  return data;
}

async function resolveMentionsFromEvent(event: PluginEvent): Promise<string> {
  const dir = await fetchTeamDirectory(event.companyId);

  // Nothing resolvable if directory has neither labels nor users.
  if (Object.keys(dir.labels).length === 0 && Object.keys(dir.users).length === 0) return "";

  const p = event.payload as Record<string, unknown>;
  const issueIds = Array.isArray(p.issueIds) ? (p.issueIds as string[]) : [];
  if (issueIds.length === 0) return "";

  const authHeaders: Record<string, string> = paperclipApiKey
    ? { Authorization: `Bearer ${paperclipApiKey}` }
    : {};
  const slackIds = new Set<string>();

  // Per-issue fallback chain: labels → assignee → createdBy (via parent chain).
  // Stop at the first tier that yields any mention for this particular issue,
  // then move on. Aggregate across all issues in the payload.
  const addOwner = (owner: TeamDirectoryOwner | undefined): boolean => {
    if (!owner) return false;
    let added = false;
    if (owner.slackUserId) {
      slackIds.add(owner.slackUserId);
      added = true;
    }
    if (Array.isArray(owner.slackUserIds)) {
      for (const u of owner.slackUserIds) {
        slackIds.add(u);
        added = true;
      }
    }
    return added;
  };

  for (const iid of issueIds) {
    try {
      // Correct route is `/api/issues/:id`. The old `/api/companies/:companyId/issues/:id`
      // path does not exist and silently 404'd, making Tier 1–3 all no-op.
      const url = `${pluginConfig.paperclipBaseUrl}/api/issues/${iid}`;
      const res = await pluginCtx.http.fetch(url, { headers: authHeaders });
      if (!res.ok) {
        pluginCtx.logger.warn("resolveMentions: issue fetch non-OK", {
          issueId: iid,
          status: res.status,
          url,
        });
        continue;
      }
      const body = (await res.json()) as {
        labels?: Array<{ name: string }>;
        assigneeUserId?: string | null;
      };

      // Tier 1: labels
      let resolved = false;
      for (const label of body.labels ?? []) {
        if (addOwner(dir.labels[label.name])) resolved = true;
      }
      if (resolved) continue;

      // Tier 2: assignee user
      if (body.assigneeUserId && addOwner(dir.users[body.assigneeUserId])) continue;

      // Tier 3: createdBy chain (walks parent issues up to depth 5)
      const chainUserId = await resolveUserFromIssueChain(event.companyId, iid);
      if (chainUserId) addOwner(dir.users[chainUserId]);
    } catch (err) {
      pluginCtx.logger.warn("resolveMentions: issue fetch failed", {
        issueId: iid,
        err: String(err),
      });
    }
  }

  return [...slackIds].map((u) => `<@${u}>`).join(" ");
}

// --- notify-board action: walk parent chain for createdByUserId, resolve via
// team-directory.users, post @mention to Slack. ---

async function resolveUserFromIssueChain(companyId: string, startIssueId: string): Promise<string | null> {
  const authHeaders: Record<string, string> = paperclipApiKey
    ? { Authorization: `Bearer ${paperclipApiKey}` }
    : {};
  // companyId is no longer part of the URL — paperclip exposes `/api/issues/:id`
  // at the top level and enforces company scope via session/auth. Kept as a
  // parameter for call-site symmetry and future use.
  void companyId;
  let currentId: string | null = startIssueId;
  for (let depth = 0; depth < 5 && currentId; depth += 1) {
    try {
      const url = `${pluginConfig.paperclipBaseUrl}/api/issues/${currentId}`;
      const res = await pluginCtx.http.fetch(url, { headers: authHeaders });
      if (!res.ok) {
        pluginCtx.logger.warn("resolveUserFromIssueChain: issue fetch non-OK", {
          issueId: currentId,
          status: res.status,
          url,
        });
        return null;
      }
      const issue = (await res.json()) as { createdByUserId?: string | null; parentId?: string | null };
      if (issue.createdByUserId) return issue.createdByUserId;
      currentId = issue.parentId ?? null;
    } catch (err) {
      pluginCtx.logger.warn("resolveUserFromIssueChain: fetch failed", {
        issueId: currentId,
        err: String(err),
      });
      return null;
    }
  }
  return null;
}

// Regex for GitHub PR URLs embedded in payloads or comments.
const GH_PR_URL_RE = /https:\/\/github\.com\/[^\s)"'<>]+\/pull\/\d+/;

function scrapeGhPrUrl(text: string | null | undefined): string | null {
  if (!text) return null;
  const m = text.match(GH_PR_URL_RE);
  return m ? m[0] : null;
}

// Scan the most-recent comments of a single issue for a GitHub PR URL.
// Agents conventionally post the review-request comment with an inline PR
// link ("[PR #19](https://...)") right next to the approval reference.
async function findPrUrlInIssueComments(
  issueId: string,
  authHeaders: Record<string, string>,
): Promise<string | null> {
  try {
    const url = `${pluginConfig.paperclipBaseUrl}/api/issues/${issueId}/comments`;
    const res = await pluginCtx.http.fetch(url, { headers: authHeaders });
    if (!res.ok) return null;
    const comments = (await res.json()) as Array<{
      body?: string | null;
      createdAt?: string | null;
    }>;
    const sorted = [...comments].sort((a, b) =>
      String(b.createdAt ?? "").localeCompare(String(a.createdAt ?? "")),
    );
    for (const c of sorted) {
      const hit = scrapeGhPrUrl(c.body ?? "");
      if (hit) return hit;
    }
  } catch (err) {
    pluginCtx.logger.warn("findPrUrlInIssueComments: error", {
      issueId,
      err: String(err),
    });
  }
  return null;
}

// Backwards lookup: when approval.issueIds is null, walk recent company
// issues and find the comment that mentions this approval id. That comment
// is almost always the agent's "review request" note which also embeds the
// PR URL — we get both the linking issue and the PR URL in one pass.
async function findApprovalReferenceBackwards(
  approvalId: string,
  companyId: string,
  authHeaders: Record<string, string>,
): Promise<{ prUrl: string | null; issueId: string | null } | null> {
  if (!companyId) return null;
  try {
    const listUrl = `${pluginConfig.paperclipBaseUrl}/api/companies/${companyId}/issues?limit=30`;
    const listRes = await pluginCtx.http.fetch(listUrl, { headers: authHeaders });
    if (!listRes.ok) return null;
    const issues = (await listRes.json()) as Array<{
      id: string;
      lastActivityAt?: string | null;
    }>;
    const sorted = [...issues].sort((a, b) =>
      String(b.lastActivityAt ?? "").localeCompare(String(a.lastActivityAt ?? "")),
    );
    // Limit fan-out to the 15 most recently active issues — the approval was
    // just created, so the referencing comment lives on an actively touched issue.
    for (const iss of sorted.slice(0, 15)) {
      const cUrl = `${pluginConfig.paperclipBaseUrl}/api/issues/${iss.id}/comments`;
      const cRes = await pluginCtx.http.fetch(cUrl, { headers: authHeaders });
      if (!cRes.ok) continue;
      const comments = (await cRes.json()) as Array<{ body?: string | null }>;
      for (const c of comments) {
        const body = c.body ?? "";
        if (body.includes(approvalId)) {
          return { prUrl: scrapeGhPrUrl(body), issueId: iss.id };
        }
      }
    }
  } catch (err) {
    pluginCtx.logger.warn("findApprovalReferenceBackwards: error", {
      approvalId,
      err: String(err),
    });
  }
  return null;
}

// Posts a one-line warning on the linked issue when plugin had to repair the
// approval payload. Intentionally best-effort: failure to post the warning
// must not block the main Slack forward.
async function postEnrichmentWarning(
  issueId: string,
  approvalId: string,
  missing: string[],
  authHeaders: Record<string, string>,
): Promise<void> {
  try {
    const url = `${pluginConfig.paperclipBaseUrl}/api/issues/${issueId}/comments`;
    const body = {
      body:
        `⚠️ **자동 경고** — approval \`${approvalId}\` payload 가 불완전합니다 ` +
        `(누락: ${missing.join(", ")}). slack plugin 이 자동 보강해 Board 알림은 ` +
        `정상 발송됐지만, **프로세스 위반 가능성** (SE premature 생성 의심). ` +
        `\`speckit-workflow\` Engineer 5번 참조 — SE 는 approval 생성 금지. ` +
        `CTO 는 이 approval 이 본인이 만든 것이 아니라면 reject 후 리뷰 통과 시점에 재생성.`,
    };
    await pluginCtx.http.fetch(url, {
      method: "POST",
      headers: { ...authHeaders, "content-type": "application/json" },
      body: JSON.stringify(body),
    });
  } catch (err) {
    pluginCtx.logger.warn("postEnrichmentWarning: error", {
      issueId,
      approvalId,
      err: String(err),
    });
  }
}

// approval.created events arrive with a minimal payload derived from the
// activity_log entry ({type, issueIds}). The richer per-approval data — title,
// description, pullRequestUrl, etc. — lives on the approval row's `payload`
// jsonb. Fetch and merge so the downstream formatter can render a useful card.
//
// Also performs recovery enrichment when an agent (typically SE) creates an
// approval without a pullRequestUrl or issueIds. We try to discover the PR
// URL from payload text, linked-issue comments, then a backwards search for
// any comment referencing the approval id. On repair, we tag the merged
// payload with `_enrichmentRepaired` + `_enrichmentMissing` for the formatter
// and post a best-effort warning on the linked issue.
async function enrichApprovalEvent(event: PluginEvent): Promise<PluginEvent> {
  const approvalId = String(event.entityId ?? "");
  if (!approvalId) return event;
  const authHeaders: Record<string, string> = paperclipApiKey
    ? { Authorization: `Bearer ${paperclipApiKey}` }
    : {};
  try {
    const url = `${pluginConfig.paperclipBaseUrl}/api/approvals/${approvalId}`;
    const res = await pluginCtx.http.fetch(url, { headers: authHeaders });
    if (!res.ok) {
      pluginCtx.logger.warn("enrichApprovalEvent: fetch non-OK", {
        approvalId,
        status: res.status,
        url,
      });
      return event;
    }
    const approval = (await res.json()) as {
      payload?: Record<string, unknown>;
      type?: string;
      issueIds?: string[] | null;
      requestedByAgentId?: string | null;
    };
    const base = (event.payload as Record<string, unknown>) ?? {};
    const payload = approval.payload ?? {};

    let prUrl: string | null =
      (typeof payload.pullRequestUrl === "string" && payload.pullRequestUrl) ||
      (typeof payload.prUrl === "string" && (payload.prUrl as string)) ||
      (typeof (payload as Record<string, unknown>).pr_url === "string" &&
        ((payload as Record<string, unknown>).pr_url as string)) ||
      (typeof (payload as Record<string, unknown>).pr === "string" &&
        ((payload as Record<string, unknown>).pr as string)) ||
      null;

    let resolvedIssueIds: string[] | null =
      approval.issueIds && approval.issueIds.length > 0 ? approval.issueIds : null;

    const missingReasons: string[] = [];
    if (!prUrl) missingReasons.push("pullRequestUrl");
    if (!resolvedIssueIds) missingReasons.push("issueIds");

    // Stage 1: scrape existing payload text.
    if (!prUrl) {
      prUrl = scrapeGhPrUrl(JSON.stringify(payload));
    }

    // Stage 2: walk linked issues' comments when issueIds known.
    if (!prUrl && resolvedIssueIds) {
      for (const iid of resolvedIssueIds) {
        prUrl = await findPrUrlInIssueComments(iid, authHeaders);
        if (prUrl) break;
      }
    }

    // Stage 3: backwards search on recent company issues for a comment that
    // references this approval id — yields both PR URL and the linking issue.
    if (!prUrl || !resolvedIssueIds) {
      const hit = await findApprovalReferenceBackwards(
        approvalId,
        String(event.companyId ?? ""),
        authHeaders,
      );
      if (hit) {
        if (!prUrl && hit.prUrl) prUrl = hit.prUrl;
        if (!resolvedIssueIds && hit.issueId) resolvedIssueIds = [hit.issueId];
      }
    }

    const repaired = missingReasons.length > 0;

    const merged: Record<string, unknown> = {
      ...base,
      ...payload,
      approvalId,
      ...(prUrl ? { pullRequestUrl: prUrl } : {}),
      ...(resolvedIssueIds ? { issueIds: resolvedIssueIds } : {}),
    };
    if (base.type !== undefined) merged.type = base.type;
    if (repaired) {
      merged._enrichmentRepaired = true;
      merged._enrichmentMissing = missingReasons;
      const warnIssueId = resolvedIssueIds?.[0];
      if (warnIssueId) {
        postEnrichmentWarning(warnIssueId, approvalId, missingReasons, authHeaders).catch(
          () => {},
        );
      }
    }

    return { ...event, payload: merged };
  } catch (err) {
    pluginCtx.logger.warn("enrichApprovalEvent: error", {
      approvalId,
      err: String(err),
    });
    return event;
  }
}

async function handleNotifyBoardAction(params: Record<string, unknown>): Promise<Record<string, unknown>> {
  const companyId = String(params.companyId ?? "");
  const issueId = String(params.issueId ?? "");
  const reason = String(params.reason ?? "");
  const approvalId = params.approvalId ? String(params.approvalId) : null;
  const channelOverride = params.channelId ? String(params.channelId) : null;
  const explicitPrUrl = params.prUrl ? String(params.prUrl) : null;

  if (!companyId || !issueId || !reason) {
    return { ok: false, reason: "missing_required_fields" };
  }

  const dir = await fetchTeamDirectory(companyId);
  let resolvedUserId = await resolveUserFromIssueChain(companyId, issueId);
  if (!resolvedUserId) {
    resolvedUserId = dir.defaultUserId;
  }
  if (!resolvedUserId) {
    pluginCtx.logger.warn("notify-board: no user resolved (chain empty, no _default)", {
      companyId,
      issueId,
    });
    return { ok: false, reason: "no_user_resolved" };
  }

  const owner = dir.users[resolvedUserId];
  if (!owner || !owner.slackUserId) {
    pluginCtx.logger.warn("notify-board: user not in team-directory.users", {
      resolvedUserId,
    });
    return { ok: false, reason: "user_not_in_directory", notifiedUserId: resolvedUserId };
  }

  // Channel resolve — use `||` so empty string falls through to default.
  // `??` only fallbacks on null/undefined, which leaked `""` through.
  const channelId = channelOverride || pluginConfig.approvalsChannelId || pluginConfig.defaultChannelId;
  if (!channelId) {
    return { ok: false, reason: "no_channel_configured" };
  }

  const authHeaders: Record<string, string> = paperclipApiKey
    ? { Authorization: `Bearer ${paperclipApiKey}` }
    : {};

  // Fetch issue for link context. Route is `/api/issues/:id` — the previous
  // `/api/companies/:companyId/issues/:id` shape doesn't exist (silent 404).
  let issueLink = "";
  try {
    const url = `${pluginConfig.paperclipBaseUrl}/api/issues/${issueId}`;
    const res = await pluginCtx.http.fetch(url, { headers: authHeaders });
    if (!res.ok) {
      pluginCtx.logger.warn("notify-board: issue fetch non-OK", {
        issueId,
        status: res.status,
        url,
      });
    }
    if (res.ok) {
      const issue = (await res.json()) as { identifier?: string; title?: string };
      if (issue.identifier) {
        const prefix = issue.identifier.split("-")[0];
        issueLink = `<${pluginConfig.paperclipBaseUrl}/${prefix}/issues/${issue.identifier}|${issue.identifier} · ${issue.title ?? ""}>`;
      }
    }
  } catch { /* best-effort */ }

  // Resolve PR URL — explicit param preferred; fall back to approval.payload
  // fields agents conventionally set (pullRequestUrl / prUrl / pr_url).
  let prUrl = explicitPrUrl ?? "";
  if (!prUrl && approvalId) {
    try {
      const res = await pluginCtx.http.fetch(
        `${pluginConfig.paperclipBaseUrl}/api/approvals/${approvalId}`,
        { headers: authHeaders },
      );
      if (res.ok) {
        const approval = (await res.json()) as { payload?: Record<string, unknown> | null };
        const payload = approval.payload ?? {};
        prUrl = String(payload.pullRequestUrl ?? payload.prUrl ?? payload.pr_url ?? "");
      }
    } catch { /* best-effort */ }
  }
  const prLink = prUrl ? `<${prUrl}|PR>` : "";

  const approvalLink = approvalId
    ? `<${pluginConfig.paperclipBaseUrl}/approvals/${approvalId}|Approval>`
    : "";

  const mention = `<@${owner.slackUserId}>`;
  const textParts = [`${mention} 🔔 Board 확인 요청`, reason];
  // Link order: most actionable first (PR) → context (issue) → tracking (approval).
  if (prLink) textParts.push(prLink);
  if (issueLink) textParts.push(issueLink);
  if (approvalLink) textParts.push(approvalLink);
  const text = textParts.join("\n");

  try {
    const result = await postMessage(pluginCtx, pluginToken, channelId, {
      text,
      blocks: [
        {
          type: "section",
          text: { type: "mrkdwn", text },
        },
      ],
    });
    if (!result.ok) {
      return { ok: false, reason: "slack_post_failed", error: result.error, notifiedUserId: resolvedUserId };
    }
    await pluginCtx.metrics.write("slack.notify_board.sent", 1);
    return { ok: true, slackTs: result.ts, notifiedUserId: resolvedUserId };
  } catch (err) {
    pluginCtx.logger.warn("notify-board: post failed", { err: String(err) });
    return { ok: false, reason: "slack_post_exception", notifiedUserId: resolvedUserId };
  }
}

// --- Slack signature verification ---

let slackSigningSecret: string | null = null;

function verifySlackSignature(
  headers: Record<string, string | string[]>,
  rawBody: string,
): boolean {
  if (!slackSigningSecret) return true; // skip if not configured

  const timestamp = String(
    headers["x-slack-request-timestamp"] ??
    headers["X-Slack-Request-Timestamp"] ?? ""
  );
  const signature = String(
    headers["x-slack-signature"] ??
    headers["X-Slack-Signature"] ?? ""
  );

  if (!timestamp || !signature) return false;

  // Reject requests older than 5 minutes to prevent replay attacks
  const now = Math.floor(Date.now() / 1000);
  if (Math.abs(now - Number(timestamp)) > 300) return false;

  const baseString = `v0:${timestamp}:${rawBody}`;
  const hmac = createHmac("sha256", slackSigningSecret)
    .update(baseString)
    .digest("hex");
  const expected = `v0=${hmac}`;

  if (expected.length !== signature.length) return false;
  return timingSafeEqual(Buffer.from(expected), Buffer.from(signature));
}

// --- Helpers ---

async function resolveChannel(
  ctx: PluginContext,
  companyId: string,
  fallback: string,
): Promise<string | null> {
  const override = await ctx.state.get({
    scopeKind: "company",
    scopeId: companyId,
    stateKey: STATE_KEYS.slackChannel,
  });
  return (override as string) ?? fallback ?? null;
}

function parseSlashCommand(rawBody: string): {
  command: string;
  text: string;
  responseUrl: string;
  userId: string;
  channelId: string;
  threadTs: string;
} {
  const params = new URLSearchParams(rawBody);
  return {
    command: params.get("command") ?? "",
    text: params.get("text") ?? "",
    responseUrl: params.get("response_url") ?? "",
    userId: params.get("user_id") ?? "",
    channelId: params.get("channel_id") ?? "",
    threadTs: params.get("thread_ts") ?? "",
  };
}

function statusBadge(status: string): string {
  const badges: Record<string, string> = {
    active: ":large_green_circle:",
    running: ":large_green_circle:",
    idle: ":white_circle:",
    paused: ":double_vertical_bar:",
    error: ":red_circle:",
    pending_approval: ":hourglass:",
    terminated: ":black_circle:",
  };
  return badges[status] ?? ":white_circle:";
}

function genId(prefix: string): string {
  return `${prefix}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
}

// --- Slash command routing ---

async function handleSlashCommand(ctx: PluginContext, rawBody: string): Promise<void> {
  const { text, responseUrl, channelId, threadTs } = parseSlashCommand(rawBody);
  const parts = text.trim().split(/\s+/);
  const subcommand = parts[0]?.toLowerCase() ?? "";
  const arg = parts[1]?.toLowerCase() ?? "";

  const companies = await ctx.companies.list({ limit: 1, offset: 0 });
  const companyId = companies[0]?.id ?? "";

  try {
    switch (subcommand) {
      case "status":
        await handleStatusCommand(ctx, companyId, responseUrl);
        break;
      case "help":
      case "":
        await handleHelpCommand(ctx, responseUrl);
        break;
      case "agents":
        await handleAgentsCommand(ctx, companyId, responseUrl);
        break;
      case "issues":
        await handleIssuesCommand(ctx, companyId, responseUrl, arg);
        break;
      case "approve":
        await handleApproveCommand(ctx, responseUrl, arg);
        break;
      case "acp": {
        const acpText = parts.slice(1).join(" ");
        await handleAcpSlashCommand(ctx, pluginToken, {
          channel: channelId,
          threadTs,
          text: acpText,
          companyId,
        });
        break;
      }
      case "commands":
        await handleCommandsSlash(ctx, companyId, responseUrl);
        break;
      case "watches": {
        const watches = await listWatches(ctx, companyId);
        if (watches.length === 0) {
          await respondEphemeral(ctx, responseUrl, {
            text: "No active watches. Use the `register_watch` tool to add watches.",
          });
        } else {
          const lines = watches.map((w) =>
            `:bell: \`${w.eventPattern}\` -> *${w.agentId}* (triggered ${w.triggerCount}x)`
          );
          await respondEphemeral(ctx, responseUrl, {
            text: `${watches.length} active watch(es)`,
            blocks: [
              {
                type: "header",
                text: { type: "plain_text", text: `Active Watches (${watches.length})` },
              },
              {
                type: "section",
                text: { type: "mrkdwn", text: lines.join("\n") },
              },
            ],
          });
        }
        break;
      }
      default:
        await respondEphemeral(ctx, responseUrl, {
          text: `Unknown command: \`${subcommand}\`. Use \`/clip help\` to see available commands.`,
        });
    }
    await ctx.metrics.write("slack.commands.handled", 1, { command_name: subcommand || "help" });
  } catch (err) {
    ctx.logger.warn("Slash command failed", { subcommand, err });
    await respondEphemeral(ctx, responseUrl, {
      text: "Something went wrong processing your command. Please try again.",
    });
  }
}

async function handleStatusCommand(ctx: PluginContext, companyId: string, responseUrl: string): Promise<void> {
  const agents = await ctx.agents.list({ companyId, limit: 100, offset: 0 });
  const activeAgents = agents.filter((a) => a.status === "active" || a.status === "running");
  const recentDone = await ctx.issues.list({ companyId, status: "done", limit: 5, offset: 0 });

  const agentSummary = activeAgents.length > 0
    ? activeAgents.map((a) => `${statusBadge(a.status)} ${a.name}`).join("\n")
    : "_No active agents_";

  const issueSummary = recentDone.length > 0
    ? recentDone.map((i) => `:white_check_mark: ${i.title}`).join("\n")
    : "_No recent completions_";

  await respondEphemeral(ctx, responseUrl, {
    text: `Status: ${activeAgents.length} active agents, ${recentDone.length} recent completions`,
    blocks: [
      {
        type: "header",
        text: { type: "plain_text", text: "Paperclip Status" },
      },
      {
        type: "section",
        fields: [
          { type: "mrkdwn", text: `*Active Agents (${activeAgents.length})*\n${agentSummary}` },
          { type: "mrkdwn", text: `*Recent Completions*\n${issueSummary}` },
        ],
      },
      {
        type: "actions",
        elements: [
          {
            type: "button",
            text: { type: "plain_text", text: "View Dashboard" },
            url: pluginConfig.paperclipBaseUrl,
            action_id: "view_dashboard",
          },
        ],
      },
    ],
  });
}

async function handleHelpCommand(ctx: PluginContext, responseUrl: string): Promise<void> {
  await respondEphemeral(ctx, responseUrl, {
    text: "Available /clip commands",
    blocks: [
      {
        type: "header",
        text: { type: "plain_text", text: "Paperclip Slash Commands" },
      },
      {
        type: "section",
        text: {
          type: "mrkdwn",
          text: [
            "`/clip status` - Show active agents and recent completions",
            "`/clip agents` - List all agents with status badges",
            "`/clip issues [open|done]` - List issues filtered by status",
            "`/clip approve <id>` - Approve a pending approval",
            "`/clip acp spawn <agent> [display]` - Add an agent to this thread",
            "`/clip acp status` - Show all agents in this thread",
            "`/clip acp close [name]` - Close a specific agent (or most recent)",
            "`/clip commands` - List registered custom commands",
            "`/clip watches` - List active event watches",
            "`/clip help` - Show this help message",
          ].join("\n"),
        },
      },
      {
        type: "context",
        elements: [
          { type: "mrkdwn", text: `<${pluginConfig.paperclipBaseUrl}|Open Paperclip Dashboard>` },
        ],
      },
    ],
  });
}

async function handleAgentsCommand(ctx: PluginContext, companyId: string, responseUrl: string): Promise<void> {
  const agents = await ctx.agents.list({ companyId, limit: 100, offset: 0 });

  if (agents.length === 0) {
    await respondEphemeral(ctx, responseUrl, { text: "No agents found." });
    return;
  }

  const lines = agents.map((a) => `${statusBadge(a.status)} *${a.name}* - \`${a.status}\``);

  await respondEphemeral(ctx, responseUrl, {
    text: `${agents.length} agents`,
    blocks: [
      {
        type: "header",
        text: { type: "plain_text", text: `Agents (${agents.length})` },
      },
      {
        type: "section",
        text: { type: "mrkdwn", text: lines.join("\n") },
      },
    ],
  });
}

async function handleIssuesCommand(ctx: PluginContext, companyId: string, responseUrl: string, filter: string): Promise<void> {
  const status = filter === "done" ? "done" as const : filter === "open" ? "todo" as const : undefined;
  const issues = await ctx.issues.list({ companyId, status, limit: 10, offset: 0 });

  if (issues.length === 0) {
    await respondEphemeral(ctx, responseUrl, { text: `No ${status ?? ""} issues found.` });
    return;
  }

  const lines = issues.map((i) => {
    const badge = i.status === "done" ? ":white_check_mark:" : ":blue_book:";
    return `${badge} *${i.title}* - \`${i.status}\``;
  });

  await respondEphemeral(ctx, responseUrl, {
    text: `${issues.length} issues`,
    blocks: [
      {
        type: "header",
        text: { type: "plain_text", text: `Issues${status ? ` (${status})` : ""} - showing ${issues.length}` },
      },
      {
        type: "section",
        text: { type: "mrkdwn", text: lines.join("\n") },
      },
    ],
  });
}

async function handleApproveCommand(ctx: PluginContext, responseUrl: string, approvalId: string): Promise<void> {
  if (!approvalId) {
    await respondEphemeral(ctx, responseUrl, { text: "Usage: `/clip approve <approval-id>`" });
    return;
  }

  try {
    await ctx.http.fetch(
      `${pluginConfig.paperclipBaseUrl}/api/approvals/${approvalId}/approve`,
      {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          ...(paperclipApiKey ? { Authorization: `Bearer ${paperclipApiKey}` } : {}),
        },
        body: JSON.stringify({ decidedByUserId: "slack:command" }),
      },
    );
    await respondEphemeral(ctx, responseUrl, { text: `:white_check_mark: Approval \`${approvalId}\` approved.` });
    await ctx.metrics.write("slack.approvals.decided", 1, { decision: "approve" });
  } catch (err) {
    ctx.logger.warn("Approve command failed", { approvalId, err });
    await respondEphemeral(ctx, responseUrl, { text: `:x: Failed to approve \`${approvalId}\`. Check the ID and try again.` });
  }
}

// --- Interactivity handler (extracted so onWebhook can fire-and-forget) ---

async function handleInteractivity(
  ctx: PluginContext,
  parsedBody: Record<string, unknown> | undefined,
): Promise<void> {
  const payload = parsedBody?.payload
    ? JSON.parse(String(parsedBody.payload)) as Record<string, unknown>
    : parsedBody;
  if (!payload || payload.type !== "block_actions") return;

  const actions = payload.actions as Array<Record<string, unknown>>;
  const responseUrl = String(payload.response_url ?? "");
  const user = payload.user as Record<string, unknown> | undefined;
  const userId = user ? String(user.id ?? user.username ?? "unknown") : "unknown";

  if (!actions?.length || !responseUrl) return;

  const action = actions[0];
  const actionId = String(action.action_id ?? "");
  const actionValue = String(action.value ?? "");

  if (!actionValue) return;

  const companies = await ctx.companies.list({ limit: 1, offset: 0 });
  const companyId = companies[0]?.id ?? "";

  // --- Approval buttons ---
  if (actionId === "approval_approve" || actionId === "approval_reject") {
    const approved = actionId === "approval_approve";
    const endpoint = approved ? "approve" : "reject";
    try {
      await ctx.http.fetch(
        `${pluginConfig.paperclipBaseUrl}/api/approvals/${actionValue}/${endpoint}`,
        {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            ...(paperclipApiKey ? { Authorization: `Bearer ${paperclipApiKey}` } : {}),
          },
          body: JSON.stringify({ decidedByUserId: `slack:${userId}` }),
        },
      );

      await respondToAction(
        ctx,
        pluginToken,
        responseUrl,
        formatApprovalResolved(actionValue, approved, userId),
      );
      await ctx.metrics.write("slack.approvals.decided", 1, { decision: endpoint });
    } catch (err) {
      ctx.logger.warn("Failed to handle approval action", { err, approvalId: actionValue });
    }
    return;
  }

  // --- Escalation buttons ---
  if (
    actionId === "escalation_use_suggested" ||
    actionId === "escalation_reply" ||
    actionId === "escalation_override" ||
    actionId === "escalation_dismiss"
  ) {
    try {
      const record = await ctx.state.get({
        scopeKind: "company",
        scopeId: companyId,
        stateKey: STATE_KEYS.escalationRecord(actionValue),
      }) as Record<string, unknown> | null;

      if (record) {
        await ctx.state.set(
          { scopeKind: "company", scopeId: companyId, stateKey: STATE_KEYS.escalationRecord(actionValue) },
          { ...record, status: "resolved", resolvedAt: new Date().toISOString(), resolvedBy: `slack:${userId}` },
        );
      }

      await respondToAction(
        ctx,
        pluginToken,
        responseUrl,
        formatEscalationResolved(actionValue, actionId, userId),
      );
      await ctx.metrics.write("slack.escalations.resolved", 1, { action: actionId });
    } catch (err) {
      ctx.logger.warn("Failed to handle escalation action", { err, escalationId: actionValue });
    }
    return;
  }

  // --- Handoff buttons ---
  if (actionId === "handoff_approve" || actionId === "handoff_reject") {
    try {
      const approved = actionId === "handoff_approve";
      await handleHandoffAction(ctx, pluginToken, companyId, actionValue, approved, userId);

      const emoji = approved ? ":white_check_mark:" : ":x:";
      const label = approved ? "Approved" : "Rejected";
      await respondToAction(ctx, pluginToken, responseUrl, {
        text: `Handoff ${label} by ${userId}`,
        blocks: [
          {
            type: "section",
            text: {
              type: "mrkdwn",
              text: `${emoji} *Handoff ${label}* by <@${userId}>`,
            },
          },
        ],
      });
    } catch (err) {
      ctx.logger.warn("Failed to handle handoff action", { err, handoffId: actionValue });
    }
    return;
  }

  // --- Discussion loop buttons ---
  if (actionId === "discussion_continue" || actionId === "discussion_stop") {
    try {
      const discAction = actionId === "discussion_continue" ? "continue" as const : "stop" as const;
      await handleDiscussionAction(ctx, pluginToken, companyId, actionValue, discAction, userId);

      const emoji = discAction === "continue" ? ":arrow_forward:" : ":stop_button:";
      const label = discAction === "continue" ? "Resumed" : "Stopped";
      await respondToAction(ctx, pluginToken, responseUrl, {
        text: `Discussion ${label} by ${userId}`,
        blocks: [
          {
            type: "section",
            text: {
              type: "mrkdwn",
              text: `${emoji} *Discussion ${label}* by <@${userId}>`,
            },
          },
        ],
      });
    } catch (err) {
      ctx.logger.warn("Failed to handle discussion action", { err, discussionId: actionValue });
    }
    return;
  }

  // --- Command step approval buttons (Phase 4) ---
  if (actionId === "command_step_approve" || actionId === "command_step_reject") {
    const approved = actionId === "command_step_approve";
    const emoji = approved ? ":white_check_mark:" : ":x:";
    const label = approved ? "Approved" : "Rejected";
    await respondToAction(ctx, pluginToken, responseUrl, {
      text: `Step ${label} by ${userId}`,
      blocks: [
        {
          type: "section",
          text: {
            type: "mrkdwn",
            text: `${emoji} *Step ${label}* by <@${userId}>`,
          },
        },
      ],
    });
    return;
  }
}

// --- Plugin definition ---

const plugin = definePlugin({
  async setup(ctx) {
    const rawConfig = await ctx.config.get();
    const config = rawConfig as unknown as SlackConfig;

    pluginCtx = ctx;
    pluginConfig = config;

    if (config.paperclipBaseUrl) {
      setBaseUrl(config.paperclipBaseUrl);
    }

    if (!config.slackTokenRef) {
      ctx.logger.warn("No slackTokenRef configured, notifications disabled");
      return;
    }

    const token = await ctx.secrets.resolve(config.slackTokenRef);
    pluginToken = token;

    // Resolve Slack signing secret for webhook signature verification
    if (config.slackSigningSecretRef) {
      try {
        slackSigningSecret = await ctx.secrets.resolve(config.slackSigningSecretRef);
      } catch {
        ctx.logger.warn("Slack signing secret not configured — webhook signature verification disabled");
      }
    }

    // Resolve Paperclip board API key. Required for approve/reject, which hit
    // endpoints guarded by assertBoard(). Without it, those actions return
    // HTTP 401/403 and the Slack button click silently fails.
    if (config.paperclipApiKeyRef) {
      try {
        paperclipApiKey = await ctx.secrets.resolve(config.paperclipApiKeyRef);
      } catch {
        ctx.logger.warn("Paperclip API key not configured — approve/reject buttons will fail");
      }
    } else {
      ctx.logger.warn("paperclipApiKeyRef not set — Slack approve/reject buttons will fail");
    }

    // Resolve the company's issuePrefix once and set it on the formatter so
    // View URLs include it (paperclip UI routes live under /<prefix>/...).
    try {
      const companies = await ctx.companies.list({ limit: 1, offset: 0 });
      const first = companies[0] as { issuePrefix?: string } | undefined;
      if (first?.issuePrefix) {
        setCompanyPrefix(first.issuePrefix);
      }
    } catch (err) {
      ctx.logger.warn("Failed to resolve issuePrefix for URL formatting", { err });
    }

    // =========================================================================
    // PHASE 1: Escalation - using 3-arg ctx.tools.register with ToolRunContext
    // =========================================================================

    ctx.tools.register(
      "escalate_to_human",
      {
        displayName: "Escalate to Human",
        description: "Escalates the current conversation to a human operator via the configured Slack escalation channel.",
        parametersSchema: {
          type: "object",
          properties: {
            reason: { type: "string", description: "Why the agent is escalating" },
            confidence: { type: "number", description: "Agent confidence score (0-1)" },
            agentName: { type: "string", description: "Name of the escalating agent" },
            conversationHistory: {
              type: "array",
              items: {
                type: "object",
                properties: {
                  role: { type: "string" },
                  text: { type: "string" },
                },
              },
              description: "Last N messages of conversation context",
            },
            agentReasoning: { type: "string", description: "Agent's reasoning for the escalation" },
            suggestedReply: { type: "string", description: "Agent's suggested reply for the human to use" },
          },
          required: ["reason"],
        },
      },
      async (params: unknown, runCtx) => {
        const p = params as Record<string, unknown>;
        const companyId = runCtx.companyId;
        const escalationId = genId("esc");

        const record: EscalationRecord = {
          id: escalationId,
          reason: String(p.reason ?? ""),
          confidence: p.confidence != null ? Number(p.confidence) : undefined,
          agentName: p.agentName != null ? String(p.agentName) : undefined,
          conversationHistory: p.conversationHistory as Array<{ role: string; text: string }> | undefined,
          agentReasoning: p.agentReasoning != null ? String(p.agentReasoning) : undefined,
          suggestedReply: p.suggestedReply != null ? String(p.suggestedReply) : undefined,
          status: "open",
          createdAt: new Date().toISOString(),
        };

        const channelId = config.escalationChatId || config.approvalsChannelId || config.defaultChannelId;
        if (!channelId) {
          return { error: "No escalation channel configured" };
        }

        const message = formatEscalationMessage(record);
        const result = await postMessage(ctx, token, channelId, message);

        if (result.ok && result.ts) {
          await ctx.state.set(
            { scopeKind: "company", scopeId: companyId, stateKey: STATE_KEYS.escalationTs(escalationId) },
            result.ts,
          );
          await ctx.state.set(
            { scopeKind: "company", scopeId: companyId, stateKey: STATE_KEYS.escalationChannel(escalationId) },
            channelId,
          );
          await ctx.state.set(
            { scopeKind: "company", scopeId: companyId, stateKey: STATE_KEYS.escalationRecord(escalationId) },
            record,
          );
          await ctx.activity.log({
            companyId,
            message: `Escalation posted to Slack: ${record.reason}`,
            entityType: "plugin",
            entityId: escalationId,
          });
          await ctx.metrics.write("slack.escalations.created", 1);
        }

        if (config.escalationHoldMessage) {
          return { content: JSON.stringify({ escalationId, holdMessage: config.escalationHoldMessage }) };
        }
        return { content: JSON.stringify({ escalationId }) };
      },
    );

    // =========================================================================
    // PHASE 2: Multi-Agent - handoff and discuss tools
    // =========================================================================

    ctx.tools.register(
      "handoff_to_agent",
      {
        displayName: "Handoff to Agent",
        description: "Requests a handoff from one agent to another in the same Slack thread. Posts an approval prompt with Approve/Reject buttons.",
        parametersSchema: {
          type: "object",
          properties: {
            fromAgent: { type: "string", description: "Name of the agent initiating the handoff" },
            toAgent: { type: "string", description: "Name of the target agent to hand off to" },
            reason: { type: "string", description: "Why the handoff is needed" },
            context: { type: "string", description: "Context to pass to the target agent on approval" },
            channelId: { type: "string", description: "Slack channel ID" },
            threadTs: { type: "string", description: "Slack thread timestamp" },
          },
          required: ["fromAgent", "toAgent", "reason", "channelId", "threadTs"],
        },
      },
      async (params: unknown, runCtx) => {
        const p = params as Record<string, unknown>;
        const companyId = runCtx.companyId;
        const fromAgent = String(p.fromAgent ?? "");
        const toAgent = String(p.toAgent ?? "");
        const reason = String(p.reason ?? "");
        const channelId = String(p.channelId ?? "");
        const threadTs = String(p.threadTs ?? "");
        const context = p.context != null ? String(p.context) : undefined;

        const handoffId = genId("hoff");

        await ctx.state.set(
          { scopeKind: "company", scopeId: companyId, stateKey: STATE_KEYS.handoff(handoffId) },
          {
            id: handoffId,
            fromAgent,
            toAgent,
            reason,
            context,
            channelId,
            threadTs,
            companyId,
            status: "pending",
            createdAt: new Date().toISOString(),
          },
        );

        const blocks = buildHandoffBlocks(fromAgent, toAgent, reason, handoffId);
        await postMessage(ctx, token, channelId, {
          text: `Handoff: ${fromAgent} -> ${toAgent}: ${reason}`,
          blocks,
        }, threadTs ? { threadTs } : undefined);

        return { content: JSON.stringify({ handoffId, status: "pending" }) };
      },
    );

    ctx.tools.register(
      "discuss_with_agent",
      {
        displayName: "Discuss with Agent",
        description: "Starts a conversation loop between two agents in a Slack thread with human checkpoints every 5 turns.",
        parametersSchema: {
          type: "object",
          properties: {
            initiatorAgent: { type: "string", description: "Name of the agent starting the discussion" },
            targetAgent: { type: "string", description: "Name of the other agent" },
            topic: { type: "string", description: "The topic or question to discuss" },
            maxTurns: { type: "number", description: "Maximum number of turns (default 10)" },
            channelId: { type: "string", description: "Slack channel ID" },
            threadTs: { type: "string", description: "Slack thread timestamp" },
          },
          required: ["initiatorAgent", "targetAgent", "topic", "channelId", "threadTs"],
        },
      },
      async (params: unknown, runCtx) => {
        const p = params as Record<string, unknown>;
        const companyId = runCtx.companyId;
        const result = await startDiscussion(ctx, token, companyId, {
          initiatorAgent: String(p.initiatorAgent ?? ""),
          targetAgent: String(p.targetAgent ?? ""),
          topic: String(p.topic ?? ""),
          channelId: String(p.channelId ?? ""),
          threadTs: String(p.threadTs ?? ""),
          maxTurns: Number(p.maxTurns ?? 10),
        });
        return { content: JSON.stringify(result) };
      },
    );

    // =========================================================================
    // PHASE 3: Media Pipeline tool
    // =========================================================================

    ctx.tools.register(
      "process_media",
      {
        displayName: "Process Media",
        description: "Processes a media file (audio/video) from Slack - transcribes audio and optionally generates a brief.",
        parametersSchema: {
          type: "object",
          properties: {
            fileId: { type: "string", description: "Slack file ID to process" },
            channelId: { type: "string", description: "Channel to post results to" },
            threadTs: { type: "string", description: "Thread to post results in" },
            briefAgentId: { type: "string", description: "Optional agent ID to generate a brief from the transcription" },
          },
          required: ["fileId", "channelId", "threadTs"],
        },
      },
      async (params: unknown, runCtx) => {
        const p = params as Record<string, unknown>;
        const result = await processMediaFile(
          ctx,
          token,
          runCtx.companyId,
          String(p.fileId),
          String(p.channelId),
          String(p.threadTs),
          p.briefAgentId ? String(p.briefAgentId) : undefined,
        );

        if (!result) {
          return { error: "Failed to process media file" };
        }
        return { content: JSON.stringify(result) };
      },
    );

    // =========================================================================
    // PHASE 4: Custom Commands tool
    // =========================================================================

    ctx.tools.register(
      "register_command",
      {
        displayName: "Register Custom Command",
        description: "Registers a custom !command that can be triggered from Slack messages. Commands can have workflow steps like invoking agents, posting messages, or creating issues.",
        parametersSchema: {
          type: "object",
          properties: {
            name: { type: "string", description: "Command name (without ! prefix)" },
            description: { type: "string", description: "What the command does" },
            usage: { type: "string", description: "Usage example (e.g. '!deploy staging')" },
            steps: {
              type: "array",
              items: {
                type: "object",
                properties: {
                  type: {
                    type: "string",
                    enum: ["invoke_agent", "post_message", "create_issue", "wait_approval"],
                  },
                  agentId: { type: "string" },
                  prompt: { type: "string" },
                  message: { type: "string" },
                  issueTitle: { type: "string" },
                  issueDescription: { type: "string" },
                  timeout: { type: "number" },
                },
                required: ["type"],
              },
              description: "Workflow steps to execute",
            },
          },
          required: ["name", "description", "usage", "steps"],
        },
      },
      async (params: unknown, runCtx) => {
        const p = params as Record<string, unknown>;
        const command: CommandDefinition = {
          name: String(p.name),
          description: String(p.description),
          usage: String(p.usage),
          steps: (p.steps as CommandDefinition["steps"]) ?? [],
        };

        const ok = await registerCommand(ctx, runCtx.companyId, command);
        return { content: JSON.stringify({ registered: ok, name: command.name }) };
      },
    );

    // =========================================================================
    // PHASE 5: Proactive Suggestions tool
    // =========================================================================

    ctx.tools.register(
      "register_watch",
      {
        displayName: "Register Event Watch",
        description: "Registers a watch that triggers an agent when a matching event occurs. The agent will be invoked with a prompt interpolated with event data.",
        parametersSchema: {
          type: "object",
          properties: {
            eventPattern: {
              type: "string",
              description: "Event pattern to watch (e.g. 'issue.created', 'agent.run.*')",
            },
            agentId: { type: "string", description: "Agent to invoke when triggered" },
            prompt: {
              type: "string",
              description: "Prompt template (use ${event.payload.key} for interpolation)",
            },
            channelId: { type: "string", description: "Slack channel to post results to" },
            threadTs: { type: "string", description: "Optional thread to post results in" },
          },
          required: ["eventPattern", "agentId", "prompt", "channelId"],
        },
      },
      async (params: unknown, runCtx) => {
        const p = params as Record<string, unknown>;
        const watch = await registerWatch(ctx, runCtx.companyId, {
          channelId: String(p.channelId),
          threadTs: String(p.threadTs ?? ""),
          companyId: runCtx.companyId,
          eventPattern: String(p.eventPattern),
          agentId: String(p.agentId),
          prompt: String(p.prompt),
          createdBy: runCtx.agentId ?? "tool",
        });
        return { content: JSON.stringify({ watchId: watch.id, eventPattern: watch.eventPattern }) };
      },
    );

    ctx.tools.register(
      "remove_watch",
      {
        displayName: "Remove Event Watch",
        description: "Removes a registered event watch by ID.",
        parametersSchema: {
          type: "object",
          properties: {
            watchId: { type: "string", description: "Watch ID to remove" },
          },
          required: ["watchId"],
        },
      },
      async (params: unknown, _runCtx) => {
        const p = params as Record<string, unknown>;
        const removed = await removeWatch(ctx, String(p.watchId));
        return { content: JSON.stringify({ removed, watchId: String(p.watchId) }) };
      },
    );

    ctx.tools.register(
      "list_watch_templates",
      {
        displayName: "List Watch Templates",
        description: "Lists built-in watch templates for common use cases like sales follow-ups, deal monitoring, and error diagnosis.",
        parametersSchema: {
          type: "object",
          properties: {},
        },
      },
      async (_params, _runCtx) => {
        const templates = BUILTIN_WATCH_TEMPLATES.map((t) => ({
          name: t.name,
          eventPattern: t.eventPattern,
          description: t.description,
        }));
        return { content: JSON.stringify({ templates }) };
      },
    );

    // =========================================================================
    // Notification helper (supports per-type channel override + threading)
    // =========================================================================

    const notify = async (
      event: PluginEvent,
      formatter: (e: PluginEvent) => SlackMessage,
      overrideChannelId?: string,
      opts?: { threadTs?: string },
    ) => {
      const fallback = overrideChannelId || config.defaultChannelId;
      const channelId = await resolveChannel(ctx, event.companyId, fallback);
      if (!channelId) return;
      const result = await postMessage(ctx, token, channelId, formatter(event), opts);
      if (result.ok) {
        await ctx.activity.log({
          companyId: event.companyId,
          message: `Forwarded ${event.eventType} to Slack`,
          entityType: "plugin",
          entityId: event.entityId,
        });
        await ctx.metrics.write("slack.notifications.sent", 1, { event_type: event.eventType });
      } else {
        await ctx.metrics.write("slack.notifications.failed", 1, { event_type: event.eventType, error_code: result.error ?? "unknown" });
      }
      return result;
    };

    // =========================================================================
    // Core event subscriptions (existing notifications)
    // =========================================================================

    if (config.notifyOnIssueCreated) {
      ctx.events.on("issue.created", async (event: PluginEvent) => {
        const result = await notify(event, formatIssueCreated);
        if (result?.ok && result.ts) {
          await ctx.state.set(
            { scopeKind: "company", scopeId: event.companyId, stateKey: STATE_KEYS.threadIssue(event.entityId ?? "") },
            result.ts,
          );
        }
      });
    }

    if (config.notifyOnIssueDone) {
      ctx.events.on("issue.updated", async (event: PluginEvent) => {
        const payload = event.payload as Record<string, unknown>;
        if (payload.status !== "done") return;
        const threadTs = await ctx.state.get({
          scopeKind: "company",
          scopeId: event.companyId,
          stateKey: STATE_KEYS.threadIssue(event.entityId ?? ""),
        }) as string | null;
        await notify(event, formatIssueDone, undefined, threadTs ? { threadTs } : undefined);
      });
    }

    if (config.notifyOnApprovalCreated) {
      ctx.events.on("approval.created", async (event: PluginEvent) => {
        // Enrich first so mention resolution and the formatter both see the
        // full approval payload (title, description, pullRequestUrl, …).
        const enriched = await enrichApprovalEvent(event);
        const mentions = await resolveMentionsFromEvent(enriched);
        await notify(
          enriched,
          (e) =>
            mentions
              ? prependMentions(formatApprovalCreated(e), mentions)
              : formatApprovalCreated(e),
          config.approvalsChannelId,
        );
      });

      // approval.resubmitted is not in upstream paperclip's PLUGIN_EVENT_TYPES
      // whitelist; our deploy.sh patches it in. Without that patch this
      // handler is registered but never fires. Once patched, resubmitted
      // approvals produce a Board notification just like fresh ones.
      // ts-cast: "approval.resubmitted" is runtime-valid after the deploy.sh
      // patch but absent from the installed plugin-sdk's PluginEventType union.
      ctx.events.on("approval.resubmitted" as Parameters<typeof ctx.events.on>[0], async (event: PluginEvent) => {
        const enriched = await enrichApprovalEvent(event);
        const mentions = await resolveMentionsFromEvent(enriched);
        await notify(
          enriched,
          (e) =>
            mentions
              ? prependMentions(formatApprovalResubmitted(e), mentions)
              : formatApprovalResubmitted(e),
          config.approvalsChannelId,
        );
      });
    }

    if (config.notifyOnAgentError) {
      ctx.events.on("agent.run.failed", async (event: PluginEvent) => {
        await notify(event, formatAgentError, config.errorsChannelId);
      });
    }

    if (config.notifyOnAgentConnected) {
      ctx.events.on("agent.status_changed", async (event: PluginEvent) => {
        const payload = event.payload as Record<string, unknown>;
        if (payload.status === "active" || payload.status === "online") {
          await notify(event, formatAgentConnected, config.pipelineChannelId);
        }
      });

      ctx.events.on("agent.run.finished", async (event: PluginEvent) => {
        const payload = event.payload as Record<string, unknown>;
        const key = STATE_KEYS.firstRunNotified(event.entityId ?? "");
        const alreadyNotified = await ctx.state.get({
          scopeKind: "company",
          scopeId: event.companyId,
          stateKey: key,
        });
        if (alreadyNotified) return;

        await ctx.state.set(
          { scopeKind: "company", scopeId: event.companyId, stateKey: key },
          true,
        );
        const milestoneEvent = {
          ...event,
          payload: { ...payload, milestone: "first successful run" },
        };
        await notify(milestoneEvent, formatOnboardingMilestone, config.pipelineChannelId);
      });
    }

    if (config.notifyOnBudgetThreshold) {
      ctx.events.on("cost_event.created", async (event: PluginEvent) => {
        const payload = event.payload as Record<string, unknown>;
        const pct = Number(payload.percentUsed ?? 0);
        if (pct < 80) return;

        const bucket = pct >= 100 ? 100 : pct >= 90 ? 90 : 80;
        const key = STATE_KEYS.budgetAlert(event.entityId ?? "", bucket);
        const alreadySent = await ctx.state.get({
          scopeKind: "company",
          scopeId: event.companyId,
          stateKey: key,
        });
        if (alreadySent) return;

        await ctx.state.set(
          { scopeKind: "company", scopeId: event.companyId, stateKey: key },
          true,
        );
        await notify(event, formatBudgetThreshold, config.pipelineChannelId);
        await ctx.metrics.write("slack.budget_alerts.sent", 1, { threshold: String(bucket) });
      });
    }

    // =========================================================================
    // Per-company channel overrides
    // =========================================================================

    ctx.data.register("channel-mapping", async (params) => {
      const companyId = String(params.companyId);
      const saved = await ctx.state.get({
        scopeKind: "company",
        scopeId: companyId,
        stateKey: STATE_KEYS.slackChannel,
      });
      return { channelId: saved ?? config.defaultChannelId };
    });

    ctx.actions.register("set-channel", async (params) => {
      const companyId = String(params.companyId);
      const channelId = String(params.channelId);
      await ctx.state.set(
        { scopeKind: "company", scopeId: companyId, stateKey: STATE_KEYS.slackChannel },
        channelId,
      );
      ctx.logger.info("Updated Slack channel mapping", { companyId, channelId });
      return { ok: true };
    });

    // notify-board: agent-invoked Slack @mention. See handleNotifyBoardAction
    // above for chain walk + team-directory resolution + Slack post logic.
    //
    // Registered in TWO shapes:
    //   1. `ctx.actions.register("notify-board", ...)` — invoked via the UI
    //      bridge (`POST /api/plugins/:pluginId/actions/:key`). That route is
    //      Board-only (`assertBoardOrgAccess`) so agents cannot use it.
    //   2. `ctx.tools.register("notify_board", ...)` — invoked as an agent tool
    //      by the Claude runtime. Same handler, companyId sourced from the
    //      agent's runCtx instead of the body.
    ctx.actions.register("notify-board", async (params) => {
      return handleNotifyBoardAction(params as Record<string, unknown>);
    });

    ctx.tools.register(
      "notify_board",
      {
        displayName: "Notify Board (Slack @mention)",
        description:
          "Posts an @-mentioned notification to the Board's Slack channel about an issue. " +
          "Resolves the recipient by walking the issue's parent chain to find the original " +
          "createdByUserId, then looking it up in team-directory.users. Use this when control " +
          "hands off to the Board (e.g. approval created, approval revision resubmitted).",
        parametersSchema: {
          type: "object",
          properties: {
            issueId: {
              type: "string",
              description: "Paperclip issue UUID that the Board should look at.",
            },
            reason: {
              type: "string",
              description:
                "Short human-readable reason why the Board needs to act (appears in the Slack message body).",
            },
            approvalId: {
              type: "string",
              description:
                "Optional approval UUID to link from the Slack message. When present, the message renders a 'Open approval' link.",
            },
            prUrl: {
              type: "string",
              description:
                "Optional GitHub PR URL. If omitted, the action falls back to approval.payload.pullRequestUrl when approvalId is set.",
            },
            channelId: {
              type: "string",
              description:
                "Optional Slack channel override. Defaults to approvalsChannelId, then defaultChannelId from plugin config.",
            },
          },
          required: ["issueId", "reason"],
        },
      },
      async (params: unknown, runCtx) => {
        const p = (params as Record<string, unknown>) ?? {};
        // runCtx.companyId is authoritative for agent calls — ignore any
        // companyId the agent may have supplied in params.
        const result = await handleNotifyBoardAction({
          ...p,
          companyId: runCtx.companyId,
        });
        return { content: JSON.stringify(result) };
      },
    );

    // =========================================================================
    // Jobs
    // =========================================================================

    // Daily digest
    if (config.enableDailyDigest) {
      ctx.jobs.register("daily-digest", async () => {
        const companies = await ctx.companies.list({ limit: 100, offset: 0 });
        for (const company of companies) {
          const channelId = await resolveChannel(ctx, company.id, config.defaultChannelId);
          if (!channelId) continue;

          const issues = await ctx.issues.list({ companyId: company.id, limit: 200, offset: 0 });
          const now = new Date();
          const dayAgo = new Date(now.getTime() - 24 * 60 * 60 * 1000);

          let tasksCompleted = 0;
          let tasksCreated = 0;
          for (const issue of issues) {
            const updated = new Date(issue.updatedAt);
            const created = new Date(issue.createdAt);
            if (issue.status === "done" && updated >= dayAgo) tasksCompleted++;
            if (created >= dayAgo) tasksCreated++;
          }

          const agents = await ctx.agents.list({ companyId: company.id, limit: 100, offset: 0 });
          const agentsActive = agents.filter((a) =>
            a.status === "active" || a.status === "running"
          ).length;

          const dateKey = now.toISOString().slice(0, 10);
          const dailyCost = await ctx.state.get({
            scopeKind: "company",
            scopeId: company.id,
            stateKey: STATE_KEYS.dailyCost(dateKey),
          });
          const totalCost = dailyCost ? String((dailyCost as number).toFixed(2)) : "0.00";

          const topAgentCosts = await ctx.state.get({
            scopeKind: "company",
            scopeId: company.id,
            stateKey: STATE_KEYS.dailyAgentCosts(dateKey),
          });
          let topAgent = "";
          if (topAgentCosts && typeof topAgentCosts === "object") {
            const costs = topAgentCosts as Record<string, number>;
            let maxCost = 0;
            for (const [name, cost] of Object.entries(costs)) {
              if (cost > maxCost) { maxCost = cost; topAgent = name; }
            }
          }

          await postMessage(ctx, token, channelId, formatDailyDigest({
            tasksCompleted,
            tasksCreated,
            agentsActive,
            totalCost,
            topAgent,
          }));

          // Clean up previous day's cost state
          const yesterday = new Date(now.getTime() - 86400000).toISOString().slice(0, 10);
          await ctx.state.delete({
            scopeKind: "company",
            scopeId: company.id,
            stateKey: STATE_KEYS.dailyCost(yesterday),
          });
          await ctx.state.delete({
            scopeKind: "company",
            scopeId: company.id,
            stateKey: STATE_KEYS.dailyAgentCosts(yesterday),
          });
        }
        ctx.logger.info("Daily digest posted to Slack");
        await ctx.metrics.write("slack.digest.sent", 1);
      });

      // Accumulate costs
      ctx.events.on("cost_event.created", async (event: PluginEvent) => {
        const payload = event.payload as Record<string, unknown>;
        const cost = Number(payload.cost ?? 0);
        if (cost <= 0) return;

        const dateKey = new Date().toISOString().slice(0, 10);
        const currentTotal = await ctx.state.get({
          scopeKind: "company",
          scopeId: event.companyId,
          stateKey: STATE_KEYS.dailyCost(dateKey),
        });
        await ctx.state.set(
          { scopeKind: "company", scopeId: event.companyId, stateKey: STATE_KEYS.dailyCost(dateKey) },
          ((currentTotal as number) ?? 0) + cost,
        );

        const agentName = String(payload.agentName ?? payload.name ?? event.entityId);
        const agentCosts = await ctx.state.get({
          scopeKind: "company",
          scopeId: event.companyId,
          stateKey: STATE_KEYS.dailyAgentCosts(dateKey),
        });
        const costs = (agentCosts as Record<string, number>) ?? {};
        costs[agentName] = (costs[agentName] ?? 0) + cost;
        await ctx.state.set(
          { scopeKind: "company", scopeId: event.companyId, stateKey: STATE_KEYS.dailyAgentCosts(dateKey) },
          costs,
        );
      });

      ctx.logger.info("Daily digest job registered (9am daily)");
    }

    // Escalation timeout job
    ctx.jobs.register("check-escalation-timeouts", async () => {
      const companies = await ctx.companies.list({ limit: 100, offset: 0 });
      const timeoutMs = config.escalationTimeoutMs ?? 900000;
      const now = Date.now();

      for (const company of companies) {
        const openEscalationsRaw = await ctx.state.get({
          scopeKind: "company",
          scopeId: company.id,
          stateKey: "escalation-records-index",
        });
        const escalationIds = Array.isArray(openEscalationsRaw) ? openEscalationsRaw as string[] : [];

        for (const escalationKey of escalationIds) {
          const record = await ctx.state.get({
            scopeKind: "company",
            scopeId: company.id,
            stateKey: STATE_KEYS.escalationRecord(escalationKey),
          }) as Record<string, unknown> | null;
          if (!record || record.status !== "open") continue;

          const createdAt = new Date(String(record.createdAt)).getTime();
          if (now - createdAt < timeoutMs) continue;

          const escalationId = String(record.id);
          const defaultAction = config.escalationDefaultAction ?? "defer";

          await ctx.state.set(
            { scopeKind: "company", scopeId: company.id, stateKey: STATE_KEYS.escalationRecord(escalationId) },
            { ...record, status: "timed_out", resolvedAt: new Date().toISOString(), resolvedBy: "system:timeout" },
          );

          const channelId = await ctx.state.get({
            scopeKind: "company",
            scopeId: company.id,
            stateKey: STATE_KEYS.escalationChannel(escalationId),
          }) as string | null;

          const threadTs = await ctx.state.get({
            scopeKind: "company",
            scopeId: company.id,
            stateKey: STATE_KEYS.escalationTs(escalationId),
          }) as string | null;

          if (channelId && threadTs) {
            await postMessage(ctx, token, channelId, {
              text: `Escalation timed out - default action: ${defaultAction}`,
              blocks: [
                {
                  type: "section",
                  text: {
                    type: "mrkdwn",
                    text: `:hourglass: *Escalation timed out*\nDefault action applied: \`${defaultAction}\``,
                  },
                },
              ],
            }, { threadTs });
          }

          await ctx.metrics.write("slack.escalations.timed_out", 1, { action: defaultAction });
          ctx.logger.info("Escalation timed out", { escalationId, defaultAction });
        }
      }
    });

    // Phase 5: Check watches job
    ctx.jobs.register("check-watches", async () => {
      const companies = await ctx.companies.list({ limit: 100, offset: 0 });
      for (const company of companies) {
        // Get recent events from state (populated by event listeners below)
        const recentEventsRaw = await ctx.state.get({
          scopeKind: "company",
          scopeId: company.id,
          stateKey: "recent-watch-events",
        });
        const recentEvents = Array.isArray(recentEventsRaw)
          ? (recentEventsRaw as Array<{ eventType: string; payload: Record<string, unknown> }>)
          : [];

        if (recentEvents.length > 0) {
          await checkWatches(ctx, token, company.id, recentEvents);
          // Clear after processing
          await ctx.state.set(
            { scopeKind: "company", scopeId: company.id, stateKey: "recent-watch-events" },
            [],
          );
        }
      }
    });

    // =========================================================================
    // Agent output listeners (native streaming + ACP events)
    // =========================================================================

    // Native agent streaming output
    ctx.events.on("plugin.slack.agent-stream-chunk", async (event: PluginEvent) => {
      const p = event.payload as Record<string, unknown>;
      await handleAgentOutput(ctx, token, event.companyId, {
        channel: String(p.channel ?? ""),
        threadTs: String(p.threadTs ?? ""),
        text: String(p.text ?? ""),
        agentName: p.agentName != null ? String(p.agentName) : undefined,
        agentDisplayName: p.agentDisplayName != null ? String(p.agentDisplayName) : undefined,
        toolName: p.toolName != null ? String(p.toolName) : undefined,
      });
    });

    // ACP output events (from cross-plugin)
    ctx.events.on(`plugin.paperclip-plugin-acp.output`, async (event: PluginEvent) => {
      const p = event.payload as Record<string, unknown>;
      await handleAgentOutput(ctx, token, event.companyId, {
        channel: String(p.channel ?? ""),
        threadTs: String(p.threadTs ?? ""),
        text: String(p.text ?? ""),
        agentName: p.agentName != null ? String(p.agentName) : undefined,
        agentDisplayName: p.agentDisplayName != null ? String(p.agentDisplayName) : undefined,
        toolName: p.toolName != null ? String(p.toolName) : undefined,
      });
    });

    // Escalation thread reply routing (from Slack Events API)
    ctx.events.on("plugin.slack.thread_reply_escalation", async (event: PluginEvent) => {
      const p = event.payload as Record<string, unknown>;
      const escalationId = String(p.escalationId ?? "");
      const replyText = String(p.text ?? "");
      const userId = String(p.userId ?? "unknown");
      if (!escalationId || !replyText) return;

      const record = await ctx.state.get({
        scopeKind: "company",
        scopeId: event.companyId,
        stateKey: STATE_KEYS.escalationRecord(escalationId),
      }) as Record<string, unknown> | null;

      if (record) {
        await ctx.state.set(
          { scopeKind: "company", scopeId: event.companyId, stateKey: STATE_KEYS.escalationRecord(escalationId) },
          { ...record, status: "resolved", resolvedAt: new Date().toISOString(), resolvedBy: `slack:${userId}` },
        );
      }

      // Route reply to agent session if we have one
      if (record?.sessionId && record?.agentName) {
        const sessions = await ctx.state.get({
          scopeKind: "company",
          scopeId: event.companyId,
          stateKey: STATE_KEYS.sessionRegistry(
            String(record.channelId ?? ""),
            String(record.threadTs ?? ""),
          ),
        });
        // Find session and send reply back
        if (Array.isArray(sessions)) {
          const session = (sessions as SessionEntry[]).find(
            (s) => s.agentName === String(record.agentName) && s.status === "active",
          );
          if (session && session.transport === "native") {
            await ctx.agents.sessions.sendMessage(session.sessionId, event.companyId, {
              prompt: `Human reply to escalation: ${replyText}`,
              reason: "Escalation reply from Slack",
            });
          }
        }
      }

      await ctx.metrics.write("slack.escalations.resolved", 1, { action: "human_reply" });
    });

    // Thread message routing (multi-agent + custom commands + media)
    ctx.events.on("plugin.slack.thread_message", async (event: PluginEvent) => {
      const p = event.payload as Record<string, unknown>;
      const channel = String(p.channel ?? "");
      const threadTs = String(p.threadTs ?? "");
      const text = String(p.text ?? "");
      const replyToMessageTs = p.replyToMessageTs != null ? String(p.replyToMessageTs) : undefined;
      const files = Array.isArray(p.files) ? p.files as Array<Record<string, unknown>> : [];
      if (!channel || !threadTs) return;

      // Phase 3: Check for media files
      for (const file of files) {
        const fileId = String(file.id ?? "");
        const mimetype = String(file.mimetype ?? "");
        if (fileId && isMediaFile(mimetype)) {
          await processMediaFile(ctx, token, event.companyId, fileId, channel, threadTs);
        }
      }

      // Phase 4: Check for custom commands
      if (text) {
        const handled = await tryCustomCommand(ctx, token, event.companyId, channel, threadTs, text);
        if (handled) return;
      }

      // Phase 2: Route to agent sessions
      if (text) {
        await routeMessageToAgent(ctx, event.companyId, channel, threadTs, text, replyToMessageTs);
      }
    });

    // Collect events for watch checking (Phase 5)
    const watchableEvents: Array<"issue.created" | "issue.updated" | "agent.run.failed" | "agent.run.finished" | "agent.status_changed" | "cost_event.created" | "approval.created"> = [
      "issue.created", "issue.updated",
      "agent.run.failed", "agent.run.finished", "agent.status_changed",
      "cost_event.created", "approval.created",
    ];
    for (const eventType of watchableEvents) {
      ctx.events.on(eventType, async (event: PluginEvent) => {
        const recentEventsRaw = await ctx.state.get({
          scopeKind: "company",
          scopeId: event.companyId,
          stateKey: "recent-watch-events",
        });
        const recentEvents = Array.isArray(recentEventsRaw)
          ? (recentEventsRaw as Array<{ eventType: string; payload: Record<string, unknown> }>)
          : [];

        // Keep last 100 events
        recentEvents.push({
          eventType: event.eventType,
          payload: event.payload as Record<string, unknown>,
        });
        if (recentEvents.length > 100) {
          recentEvents.splice(0, recentEvents.length - 100);
        }

        await ctx.state.set(
          { scopeKind: "company", scopeId: event.companyId, stateKey: "recent-watch-events" },
          recentEvents,
        );
      });
    }

    slackAdapter = new SlackAdapter(ctx, token);

    ctx.logger.info("Slack Chat OS plugin started (v2.0.0) - all 5 phases active");
  },

  // =========================================================================
  // Webhook handler (Slack Events, Slash Commands, Interactivity)
  // =========================================================================

  async onWebhook(input: PluginWebhookInput): Promise<void> {
    // Verify Slack request signature (skip for url_verification challenge)
    const body = input.parsedBody as Record<string, unknown> | undefined;
    const isVerificationChallenge = body?.type === "url_verification";

    if (!isVerificationChallenge && !verifySlackSignature(input.headers, input.rawBody)) {
      pluginCtx.logger.warn("Rejected webhook: invalid Slack signature");
      return;
    }

    // Slack Events API (url_verification + event callbacks)
    if (input.endpointKey === WEBHOOK_KEYS.slackEvents) {
      if (body?.type === "url_verification") {
        return;
      }

      // Handle file_shared events for Phase 3 media pipeline
      if (body?.type === "event_callback") {
        const event = body.event as Record<string, unknown> | undefined;
        if (event?.type === "file_shared") {
          const companies = await pluginCtx.companies.list({ limit: 1, offset: 0 });
          const companyId = companies[0]?.id ?? "";
          const fileId = String(event.file_id ?? "");
          const channelId = String(event.channel_id ?? "");

          if (fileId && channelId) {
            await processMediaFile(pluginCtx, pluginToken, companyId, fileId, channelId, "");
          }
        }
      }
    }

    // Slash commands — Slack enforces a ≤3s HTTP response. paperclip awaits
    // onWebhook before sending the response, and handleSlashCommand can take
    // multiple seconds (paperclip API calls + Slack response_url post).
    // Fire-and-forget so onWebhook returns immediately; the actual reply
    // arrives in-channel via Slack's response_url.
    if (input.endpointKey === WEBHOOK_KEYS.slashCommand) {
      void handleSlashCommand(pluginCtx, input.rawBody).catch((err) => {
        pluginCtx.logger.error("slash command handler failed", { err });
      });
      return;
    }

    // Interactivity (button clicks) — Slack enforces a ≤3s ack. Fire-and-forget
    // so onWebhook returns immediately; updates post via response_url.
    if (input.endpointKey === WEBHOOK_KEYS.interactivity) {
      void handleInteractivity(pluginCtx, body).catch((err) => {
        pluginCtx.logger.error("interactivity handler failed", { err });
      });
      return;
    }
  },

  async onValidateConfig(config) {
    if (!config.slackTokenRef || typeof config.slackTokenRef !== "string") {
      return { ok: false, errors: ["slackTokenRef is required"] };
    }
    if (!config.defaultChannelId || typeof config.defaultChannelId !== "string") {
      return { ok: false, errors: ["defaultChannelId is required"] };
    }
    return { ok: true };
  },

  async onHealth(): Promise<PluginHealthDiagnostics> {
    return { status: "ok" };
  },
});

export default plugin;
runWorker(plugin, import.meta.url);
