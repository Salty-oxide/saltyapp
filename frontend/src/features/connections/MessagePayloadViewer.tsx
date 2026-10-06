import { useEffect, useMemo, useRef, useState } from "react";
import { save } from "@tauri-apps/plugin-dialog";
import { JsonTreeView } from "../../components/JsonTreeView";
import { LineNumberedText } from "../../components/LineNumberedText";
import { CheckIcon, CopyIcon, SaveIcon } from "../../components/AppIcons";
import { ValueFormatSelect, valueFormat } from "../../components/ValueFormatSelect";
import { XmlTreeView } from "../../components/XmlTreeView";
import { api } from "../../lib/tauri";
import { useDecodeAvro, useDecodeProtobuf, useFullPayload } from "./useClusterResources";
import { useJsonViewerTabsStore } from "../tabs/useJsonViewerTabsStore";
import { useTabsStore } from "../tabs/useTabsStore";
import { useMessageViewerStore } from "../workspace/useMessageViewerStore";
import {
  PanelTabId,
  selectTabPrefs,
  useMessageViewerPrefsStore,
  ValueMode,
} from "../workspace/useMessageViewerPrefsStore";
import { tabDataKey } from "../workspace/useTabDataStore";
import {
  base64DecodedLength,
  base64ToBytes,
  base64ToDisplayText,
  bytesToHexDump,
  bytesToText,
  formatPayloadSize,
  formatXmlNode,
  isPayloadTruncated,
  textToBase64,
  tryParseJson,
  tryParseXml,
  wrapBase64,
} from "./payloadDecoding";

/**
 * How much of a payload the raw Text view renders before offering the rest
 * behind a click. A `<pre>` is one text node the browser lays out in a
 * single pass, so a 10 MB message costs a visible freeze to display text
 * that runs thousands of screens deep. 256 KB is far more than anyone reads
 * at a glance and renders instantly.
 */
export const TEXT_PREVIEW_CHARS = 256 * 1024;

/**
 * The same bound for the Hex view, expressed in payload bytes.
 *
 * A hex dump is about five characters per byte — offset column, two hex
 * digits, a space and a share of the ASCII column — so capping it at
 * [`TEXT_PREVIEW_CHARS`] *bytes* would render over a megabyte of text and
 * reintroduce exactly the freeze that constant exists to prevent. 64 KB
 * dumps to roughly 4,000 lines, which is already far more than anyone reads
 * byte by byte.
 */
export const HEX_PREVIEW_BYTES = 64 * 1024;

/**
 * The formats that render as a collapsible tree, and so the ones Expand all
 * has anything to act on. XML is a tree too, but `XmlTreeView`'s own, with
 * its own collapse state — this button doesn't reach it.
 */
const TREE_MODES: ValueMode[] = ["json", "avro", "protobuf"];

/** Which toolbar button a status line came from. */
type ToolbarAction = "copy" | "open" | "save";

const PANEL_TABS: { id: PanelTabId; label: string }[] = [
  { id: "headers", label: "Headers" },
  { id: "value", label: "Value" },
];

/**
 * Shown by the structured views while the real bytes are still coming.
 *
 * Those views can't render a truncated preview honestly: `JSON.parse` on a
 * payload cut mid-document fails, and an Avro decode of one fails at the
 * broker's expense. Both used to report that failure — "Payload is not valid
 * JSON", an Avro error — for as long as the fetch took, which reads as a
 * broken message rather than a pending one.
 */
function PayloadLoadingSpinner() {
  return (
    <p className="message-payload-truncation">
      <span className="spinner" role="status" aria-label="Loading the full payload" />
    </p>
  );
}

function CloseIcon() {
  return (
    <svg width="14" height="14" viewBox="0 0 16 16" fill="none" aria-hidden="true">
      <path d="M4 4l8 8M12 4l-8 8" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" />
    </svg>
  );
}

function ExternalLinkIcon() {
  return (
    <svg width="14" height="14" viewBox="0 0 16 16" fill="none" aria-hidden="true">
      <path d="M6.5 3.5h-3a1 1 0 0 0-1 1v8a1 1 0 0 0 1 1h8a1 1 0 0 0 1-1v-3" stroke="currentColor" />
      <path d="M9.5 2.5h4v4M13.3 2.7L7.5 8.5" stroke="currentColor" strokeLinecap="round" strokeLinejoin="round" />
    </svg>
  );
}

/**
 * The layout switch, drawn as the layout it will produce rather than the one
 * in force — the same convention as every editor's split-pane button, and the
 * reason the two glyphs are mirror images of each other.
 */
function DockBottomIcon() {
  return (
    <svg width="14" height="14" viewBox="0 0 16 16" fill="none" aria-hidden="true">
      <rect x="2" y="2" width="12" height="12" rx="1.5" stroke="currentColor" />
      <path d="M2 10h12" stroke="currentColor" />
      <rect x="2.6" y="10.6" width="10.8" height="2.8" fill="currentColor" opacity="0.35" stroke="none" />
    </svg>
  );
}

function DockRightIcon() {
  return (
    <svg width="14" height="14" viewBox="0 0 16 16" fill="none" aria-hidden="true">
      <rect x="2" y="2" width="12" height="12" rx="1.5" stroke="currentColor" />
      <path d="M10 2v12" stroke="currentColor" />
      <rect x="10.6" y="2.6" width="2.8" height="10.8" fill="currentColor" opacity="0.35" stroke="none" />
    </svg>
  );
}

function kilobytes(chars: number): string {
  return Math.max(Math.round(chars / 1024), 1).toLocaleString();
}

/**
 * The Raw/Hex/Base64 text for a payload, with the bound each view is capped
 * at already applied.
 *
 * All three are literal renderings of the same bytes, so they share one
 * shape: what to show, and whether that is all of it. `total`/`shown` are in
 * whatever unit the view is bounded by (characters for Raw and Base64, bytes
 * for Hex) — they are only ever compared with each other and printed as a
 * size, never mixed between views.
 */
interface LiteralBody {
  text: string;
  truncated: boolean;
  shown: number;
  total: number;
}

function literalBodyFor(mode: ValueMode, payloadBase64: string, text: string, showAll: boolean): LiteralBody | null {
  if (mode === "raw") {
    const truncated = !showAll && text.length > TEXT_PREVIEW_CHARS;
    return {
      text: truncated ? text.slice(0, TEXT_PREVIEW_CHARS) : text,
      truncated,
      shown: truncated ? TEXT_PREVIEW_CHARS : text.length,
      total: text.length,
    };
  }
  if (mode === "base64") {
    const truncated = !showAll && payloadBase64.length > TEXT_PREVIEW_CHARS;
    const shown = truncated ? payloadBase64.slice(0, TEXT_PREVIEW_CHARS) : payloadBase64;
    return { text: wrapBase64(shown), truncated, shown: shown.length, total: payloadBase64.length };
  }
  if (mode === "hex") {
    const bytes = base64ToBytes(payloadBase64);
    const truncated = !showAll && bytes.length > HEX_PREVIEW_BYTES;
    const shown = truncated ? bytes.subarray(0, HEX_PREVIEW_BYTES) : bytes;
    return { text: bytesToHexDump(shown), truncated, shown: shown.length, total: bytes.length };
  }
  return null;
}

export function MessagePayloadViewer() {
  const message = useMessageViewerStore((s) => s.message);
  const connectionId = useMessageViewerStore((s) => s.connectionId);
  const topic = useMessageViewerStore((s) => s.topic);
  const clearViewedMessage = useMessageViewerStore((s) => s.clear);
  const openJsonTab = useJsonViewerTabsStore((s) => s.openTab);
  const selectTab = useTabsStore((s) => s.selectTab);
  // Which panel tab and Value format are showing is deliberately NOT
  // component state: App.tsx renders this component `key={activeTabId}`, so
  // switching top-level tabs unmounts it and `useState` would hand back the
  // defaults on the way back in — dropping the user out of the JSON (or
  // Avro/XML) view they left open. See `useMessageViewerPrefsStore`.
  const prefsKey = tabDataKey(useTabsStore((s) => s.activeTabId));
  const { panelTab: activeTab, valueMode: mode } = useMessageViewerPrefsStore((s) => selectTabPrefs(s, prefsKey));
  const setPanelTab = useMessageViewerPrefsStore((s) => s.setPanelTab);
  const setValueMode = useMessageViewerPrefsStore((s) => s.setValueMode);
  const placement = useMessageViewerPrefsStore((s) => s.placement);
  const togglePlacement = useMessageViewerPrefsStore((s) => s.togglePlacement);
  const setActiveTab = (tab: PanelTabId) => setPanelTab(prefsKey, tab);
  const setMode = (valueMode: ValueMode) => setValueMode(prefsKey, valueMode);
  /**
   * Which payload the user asked to see in full, rather than a boolean.
   *
   * A boolean reset by an effect looks equivalent and isn't: effects run
   * after React commits and the browser paints, so the first render after
   * switching messages would still see the previous message's `true`, put the
   * whole new payload into the `<pre>`, and paint it — the exact freeze
   * `TEXT_PREVIEW_CHARS` exists to prevent, one render before the reset lands.
   * Comparing against the current payload is decided during render, so a
   * different message is always truncated from its first frame.
   *
   * The format is part of it for the same reason: "show me all 300 KB of
   * text" must not also mean "and now dump all 300 KB as hex", which is five
   * times the characters.
   */
  const [expanded, setExpanded] = useState<{ payload: string; mode: ValueMode } | null>(null);
  /**
   * Transient feedback for the toolbar's Copy/Save — cleared on a
   * timer, or replaced by the next action's. `action` is carried so the Copy
   * button can show its own tick without matching on the message text.
   */
  const [status, setStatus] = useState<{ kind: "ok" | "error"; action: ToolbarAction; message: string } | null>(null);
  /** The pending clear for `status`, so a new action cancels the old one's timer rather than racing it. */
  const statusTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const decodeAvro = useDecodeAvro();
  const { mutate: runDecodeAvro } = decodeAvro;
  const decodeProtobuf = useDecodeProtobuf();
  const { mutate: runDecodeProtobuf } = decodeProtobuf;

  // The Data tab's grid rows carry only a bounded slice of each payload —
  // that truncation is what keeps a large fetch inside the webview's memory
  // ceiling. So what a row hands this component is usually a preview, and
  // displaying or decoding the message means going back for the real bytes,
  // for this one message.
  const rowPayloadBase64 = message?.payloadBase64 ?? null;
  const isTruncated = isPayloadTruncated(rowPayloadBase64, message?.payloadSizeBytes ?? null);
  // Kept per tab rather than in the query cache, because this component is
  // rendered `key={activeTabId}` and so is destroyed on every top-level tab
  // switch. Without this, leaving a tab and coming back re-fetched the open
  // message's payload off the broker each time.
  const rememberedPayloadBase64 = useMessageViewerStore((s) => s.fullPayloadBase64);
  const rememberFullPayload = useMessageViewerStore((s) => s.setFullPayload);
  const needsFullPayload = isTruncated && rememberedPayloadBase64 === undefined;
  const fullPayload = useFullPayload(
    connectionId,
    topic,
    message?.partition,
    message?.offset,
    needsFullPayload,
  );
  const fetchedPayloadBase64 =
    rememberedPayloadBase64 ??
    fullPayload.data?.messages.find((m) => m.partition === message?.partition && m.offset === message?.offset)
      ?.payloadBase64 ??
    null;

  // Hand a freshly fetched payload to the store so the next visit to this tab
  // finds it there. Guarded on it being new, or this would loop.
  useEffect(() => {
    if (fetchedPayloadBase64 !== null && rememberedPayloadBase64 === undefined) {
      rememberFullPayload(fetchedPayloadBase64);
    }
  }, [fetchedPayloadBase64, rememberedPayloadBase64, rememberFullPayload]);
  // Falls back to the preview while the full fetch is in flight or if it
  // fails — a truncated payload beats a blank pane — but that fallback is
  // always labelled, because a few KB of a multi-megabyte message is
  // otherwise indistinguishable from the whole of a small one.
  const payloadBase64 = isTruncated ? (fetchedPayloadBase64 ?? rowPayloadBase64) : rowPayloadBase64;
  const isShowingPreviewOnly = isTruncated && fetchedPayloadBase64 === null;
  /**
   * How big the *message* is, not how much of it is on screen.
   *
   * `payloadSizeBytes` is what the broker reported and the backend sends it
   * on every row — including rows fetched without their payload — so it is
   * the only figure that stays right while a bounded preview is what's
   * loaded. Measuring `payloadBase64` instead would report the size of the
   * slice, which is the one thing this label must never do: the whole reason
   * to show it is to tell a 2 KB record from a 40 MB one before deciding to
   * open it. The base64 is the fallback for the rare message the backend
   * sent no size for.
   */
  const payloadSizeBytes =
    message?.payloadSizeBytes ?? (payloadBase64 === null ? null : base64DecodedLength(payloadBase64));
  // A failed fetch is not still in flight: the banner above says why, and the
  // views below go back to reporting what they actually have rather than
  // spinning for a payload that is never going to arrive.
  const isLoadingFullPayload = isShowingPreviewOnly && !fullPayload.isError;

  // Re-decodes whenever a different message is viewed while Avro mode is
  // already active (e.g. clicking through grid rows without switching
  // modes each time) — not just on the button click that first selects it.
  useEffect(() => {
    if (mode === "avro" && !isLoadingFullPayload && payloadBase64 && connectionId && topic) {
      runDecodeAvro({ connectionId, topic, payloadBase64 });
    }
  }, [mode, payloadBase64, connectionId, topic, runDecodeAvro, isLoadingFullPayload]);

  // Same contract as the Avro effect above — re-decodes on every message
  // while Protobuf mode stays selected, not only on the click that chose it.
  useEffect(() => {
    if (mode === "protobuf" && !isLoadingFullPayload && payloadBase64 && connectionId && topic) {
      runDecodeProtobuf({ connectionId, topic, payloadBase64 });
    }
  }, [mode, payloadBase64, connectionId, topic, runDecodeProtobuf, isLoadingFullPayload]);

  // Decoding is memoised on the payload itself, not left to run inline.
  // Every one of these is O(payload): the base64 decode walks byte by byte,
  // the UTF-8 decode copies the lot, and `JSON.parse` builds an object graph
  // the size of the document. Inline, they re-ran on *every* render of this
  // component — switching panel tabs, toggling a mode, a parent re-rendering
  // — so on a 2-10 MB message the app froze each time rather than only on
  // the first look at it.
  const text = useMemo(
    () => (payloadBase64 === null ? null : bytesToText(base64ToBytes(payloadBase64))),
    [payloadBase64],
  );

  const json = useMemo(
    () => (mode === "json" && text !== null ? tryParseJson(text) : undefined),
    [mode, text],
  );
  const xml = useMemo(() => (mode === "xml" && text !== null ? tryParseXml(text) : undefined), [mode, text]);

  // A `<pre>` holding megabytes of text is a single enormous DOM text node
  // that the browser lays out in one go, so the literal views are capped
  // until the user asks for the rest — and asking is per payload and per
  // format, so clicking through to a different message (or switching to a
  // heavier rendering of the same one) starts collapsed again.
  const showAll = expanded !== null && expanded.payload === payloadBase64 && expanded.mode === mode;
  const literal = useMemo(
    () => (payloadBase64 === null || text === null ? null : literalBodyFor(mode, payloadBase64, text, showAll)),
    [mode, payloadBase64, text, showAll],
  );

  /**
   * Whether a decode mutation's successful result is this message's.
   *
   * `useMutation` keeps the previous `data` until a new call resolves, and the
   * decode effects above deliberately don't fire while the full payload is
   * still being fetched. Between clicking a large message and its bytes
   * arriving, `decodeAvro.isSuccess` therefore still describes the message
   * before it. The *views* were safe — they hide behind the spinner — but the
   * toolbar isn't rendered inside that branch, so Copy/Save/Open would hand
   * back the previous message's decode under this message's filename.
   *
   * `variables` is what the mutation was last called with, so comparing its
   * payload is exactly the question "is this result about what I'm showing?".
   */
  function decodeMatchesPayload(variables: { payloadBase64: string } | undefined): boolean {
    return variables?.payloadBase64 === payloadBase64;
  }

  const avroDecodeIsCurrent = decodeAvro.isSuccess && decodeMatchesPayload(decodeAvro.variables);
  const protobufDecodeIsCurrent = decodeProtobuf.isSuccess && decodeMatchesPayload(decodeProtobuf.variables);

  function report(kind: "ok" | "error", action: ToolbarAction, message: string) {
    setStatus({ kind, action, message });
    // Cancel whatever the previous action scheduled before arming this one.
    // Without that, a Copy's 1.5s timer outlives its own message and clears
    // the *next* action's — so a Save that failed a second later had its
    // reason wiped after half a second, and the 6s branch below never
    // actually gave anyone time to read anything.
    if (statusTimerRef.current !== null) {
      clearTimeout(statusTimerRef.current);
    }
    // An error stays up long enough to read a path or a permissions message;
    // a success is a tick and a line the user asked for and already expects.
    statusTimerRef.current = setTimeout(() => setStatus(null), kind === "ok" ? 1500 : 6000);
  }

  /**
   * The current view rendered as text, in full — what Copy, Save and "open in
   * new tab" all act on.
   *
   * Deliberately computed on demand rather than memoised alongside the
   * rendered body: this is the *whole* payload, uncapped, and for a
   * multi-megabyte message pretty-printing it is exactly the work the preview
   * bounds exist to avoid doing on every render. It is cheap to do once, on a
   * click that asked for it.
   */
  function currentViewText(): string | null {
    if (payloadBase64 === null || text === null) return null;
    switch (mode) {
      case "json":
        return json === undefined ? null : JSON.stringify(json, null, 2);
      case "avro":
        return avroDecodeIsCurrent ? JSON.stringify(decodeAvro.data, null, 2) : null;
      case "protobuf":
        return protobufDecodeIsCurrent ? JSON.stringify(decodeProtobuf.data.value, null, 2) : null;
      case "xml":
        return xml === undefined ? null : formatXmlNode(xml);
      case "base64":
        return wrapBase64(payloadBase64);
      case "hex":
        return bytesToHexDump(base64ToBytes(payloadBase64));
      default:
        return text;
    }
  }

  function messageLabel(): string {
    return `Partition ${message?.partition} · Offset ${message?.offset}`;
  }

  /** `partition-0-offset-42` — a filename stem that says which message this is. */
  function fileStem(): string {
    return `partition-${message?.partition}-offset-${message?.offset}`;
  }

  /**
   * The reason this message can't be written out yet, or `null` if it can.
   *
   * While `isShowingPreviewOnly` holds, `payloadBase64` is the Data tab's
   * bounded preview slice, not the message — and if the full-payload fetch
   * *failed* it stays that way permanently. Copy and Save render straight
   * from it, so without this they wrote a few KB of a multi-megabyte message
   * to `partition-0-offset-42.json` with no error and nothing to distinguish
   * it from the real thing.
   */
  function incompletePayloadReason(): string | null {
    if (!isShowingPreviewOnly) return null;
    return fullPayload.isError
      ? "the full payload could not be loaded, so only a truncated preview is here"
      : "the full payload is still loading";
  }

  async function handleCopy() {
    const incomplete = incompletePayloadReason();
    if (incomplete !== null) {
      report("error", "copy", `Not copied — ${incomplete}.`);
      return;
    }
    const content = currentViewText();
    if (content === null) {
      report("error", "copy", `Nothing to copy — the payload isn't valid ${valueFormat(mode).label}.`);
      return;
    }
    try {
      await navigator.clipboard.writeText(content);
      report("ok", "copy", "Copied.");
    } catch (error) {
      report("error", "copy", `Copy failed: ${error instanceof Error ? error.message : String(error)}`);
    }
  }

  /**
   * What the opened tab is told about where its value came from: the format
   * it was read as (its glyph in the tab strip), the message's size and a
   * filename stem — so that tab can show the same size chip and offer the
   * same Copy/Save this toolbar does rather than being a read-only dead end.
   *
   * The size is the broker's figure for the whole message, deliberately, and
   * so is right even when only a preview has been loaded — the tab renders a
   * capped view of the payload just as this pane does, and labelling that
   * view with the length of the cap would say a 40 MB record is 256 KB.
   */
  function origin(format: ValueMode) {
    return {
      format,
      payloadSizeBytes: payloadSizeBytes ?? undefined,
      fileStem: fileStem(),
    };
  }

  function handleOpenInNewTab() {
    if (mode === "json" && json !== undefined) {
      selectTab(openJsonTab(messageLabel(), json, "json", origin("json")));
      return;
    }
    if (mode === "avro" && avroDecodeIsCurrent) {
      selectTab(openJsonTab(messageLabel(), decodeAvro.data, "json", origin("avro")));
      return;
    }
    if (mode === "protobuf" && protobufDecodeIsCurrent) {
      selectTab(openJsonTab(messageLabel(), decodeProtobuf.data.value, "json", origin("protobuf")));
      return;
    }
    if (mode === "xml" && xml !== undefined) {
      selectTab(openJsonTab(messageLabel(), xml, "xml", origin("xml")));
      return;
    }
    // Deliberately `literal.text`, not `currentViewText()`: the tab gets
    // exactly what the pane is showing, capped the same way — including the
    // user's own "show the whole payload" choice. Rebuilding it uncapped put
    // a 10 MB payload's ~50 MB hex dump into one `<pre>` with ~650,000 gutter
    // lines, which is the freeze `TEXT_PREVIEW_CHARS` and `HEX_PREVIEW_BYTES`
    // exist to prevent — and a viewer tab holds it for the app's lifetime,
    // outside the retained-payload budget.
    const content = literal?.text ?? null;
    if (content === null) {
      report("error", "open", `Nothing to open — the payload isn't valid ${valueFormat(mode).label}.`);
      return;
    }
    selectTab(openJsonTab(`${messageLabel()} · ${valueFormat(mode).label}`, content, "text", origin(mode)));
  }

  /**
   * Save writes the value **as the chosen format renders it** — pretty JSON,
   * indented XML, the hex dump, the decoded Avro document — to a file the
   * user picks. It is the "give me what I'm looking at" button.
   */
  async function handleSave() {
    const incomplete = incompletePayloadReason();
    if (incomplete !== null) {
      report("error", "save", `Not saved — ${incomplete}.`);
      return;
    }
    const format = valueFormat(mode);
    const content = currentViewText();
    if (content === null) {
      report("error", "save", `Nothing to save — the payload isn't valid ${format.label}.`);
      return;
    }
    try {
      const path = await save({
        defaultPath: `${fileStem()}.${format.extension}`,
        filters: [{ name: format.label, extensions: [format.extension] }],
      });
      if (!path) return;
      await api.savePayloadFile(path, textToBase64(content));
      report("ok", "save", `Saved to ${path}`);
    } catch (error) {
      report("error", "save", `Save failed: ${error instanceof Error ? error.message : String(error)}`);
    }
  }

  if (!message) {
    return <p className="resizable-pane-placeholder">Select a message to view its payload.</p>;
  }

  const dockLabel = placement === "right" ? "Move the payload panel to the bottom" : "Move the payload panel to the right";
  const justCopied = status?.kind === "ok" && status.action === "copy";

  return (
    <div className="message-payload-viewer">
      <div className="message-payload-header">
        <p className="message-payload-meta">
          Partition {message.partition} · Offset {message.offset}
          {message.keyBase64 !== null && <> · Key: {base64ToDisplayText(message.keyBase64)}</>}
        </p>
        <div className="message-payload-header-actions">
          <button
            type="button"
            className="json-tree-icon-button"
            title={dockLabel}
            aria-label={dockLabel}
            onClick={togglePlacement}
          >
            {placement === "right" ? <DockBottomIcon /> : <DockRightIcon />}
          </button>
          <button
            type="button"
            className="json-tree-icon-button"
            title="Close"
            aria-label="Close message payload viewer"
            onClick={clearViewedMessage}
          >
            <CloseIcon />
          </button>
        </div>
      </div>

      <div className="connection-modal-tabs" role="tablist">
        {PANEL_TABS.map((tab) => (
          <button
            key={tab.id}
            type="button"
            role="tab"
            aria-selected={activeTab === tab.id}
            className={`connection-modal-tab${activeTab === tab.id ? " connection-modal-tab--active" : ""}`}
            onClick={() => setActiveTab(tab.id)}
          >
            {tab.label}
          </button>
        ))}
      </div>

      {activeTab === "headers" && (
        <div role="tabpanel" aria-label="Headers" className="message-payload-panel">
          {message.headers.length === 0 ? (
            <p className="resizable-pane-placeholder">No headers.</p>
          ) : (
            <div className="message-payload-scroll">
              <table className="topic-detail-table message-payload-headers-table">
                <thead>
                  <tr>
                    <th>Key</th>
                    <th>Value</th>
                  </tr>
                </thead>
                <tbody>
                  {message.headers.map((header, index) => (
                    <tr key={index}>
                      <td>{header.key}</td>
                      <td>{base64ToDisplayText(header.valueBase64) ?? <em>null</em>}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>
      )}

      {activeTab === "value" && (
        <div role="tabpanel" aria-label="Value" className="message-payload-panel">
          {text === null ? (
            <p className="resizable-pane-placeholder">
              Payload wasn't loaded for this fetch — check "Fetch message payload" above Fetch, then Fetch again.
            </p>
          ) : (
            <>
              {isShowingPreviewOnly && (
                <p className="message-payload-truncation">
                  {fullPayload.isError
                    ? `Showing a preview only — the full payload could not be loaded: ${fullPayload.error?.message}`
                    : "Loading the full payload — showing a preview until it arrives."}
                </p>
              )}
              {/* One toolbar for every format, rather than the two the tree
                  views used to bring with them: the format picker on the
                  left, what you can do with the result on the right. The
                  tree views' own toolbars are switched off below
                  (`showToolbar={false}`) so the controls don't stack and
                  don't move depending on which format is selected. */}
              <div className="message-payload-toolbar">
                <ValueFormatSelect value={mode} onChange={setMode} />
                <div className="message-payload-toolbar-actions">
                  {payloadSizeBytes !== null && (
                    <span
                      className="message-payload-size"
                      title={`Payload size: ${payloadSizeBytes.toLocaleString()} bytes`}
                    >
                      {formatPayloadSize(payloadSizeBytes)}
                    </span>
                  )}
                  <button
                    type="button"
                    className="json-tree-icon-button"
                    title="Open in new tab"
                    aria-label="Open in new tab"
                    onClick={handleOpenInNewTab}
                  >
                    <ExternalLinkIcon />
                  </button>
                  <button
                    type="button"
                    className="json-tree-icon-button"
                    title={justCopied ? "Copied!" : "Copy"}
                    aria-label={justCopied ? "Copied!" : "Copy"}
                    onClick={handleCopy}
                  >
                    {justCopied ? <CheckIcon /> : <CopyIcon />}
                  </button>
                  <button
                    type="button"
                    className="json-tree-icon-button"
                    title={`Save as ${valueFormat(mode).label}…`}
                    aria-label={`Save as ${valueFormat(mode).label}`}
                    onClick={handleSave}
                  >
                    <SaveIcon />
                  </button>
                </div>
              </div>
              {status && (
                <p
                  className={`message-payload-truncation${status.kind === "error" ? " message-payload-status--error" : ""}`}
                  role={status.kind === "error" ? "alert" : "status"}
                >
                  {status.message}
                </p>
              )}
              {/* Everything above this — the partition/offset line, the
                  Headers/Value tabs, the preview notice and the toolbar —
                  stays put; only the decoded payload itself scrolls. With
                  the whole pane as one scroll box, a multi-megabyte JSON or
                  Avro document pushed those controls off the top, so
                  switching format or closing the message meant paging all the
                  way back up. */}
              <div
                className={`message-payload-scroll${TREE_MODES.includes(mode) ? " message-payload-scroll--tree" : ""}`}
              >
                {literal !== null && (
                  <>
                    <LineNumberedText
                      text={literal.text}
                      ariaLabel={`Payload as ${valueFormat(mode).label}`}
                      // Raw and Hex render a capped preview, so find can only
                      // see the part that is on screen. Saying so is the
                      // difference between "no matches here" and the flat
                      // "No results", which would be untrue of the payload.
                      findScopeNote={literal.truncated ? "in the shown preview" : undefined}
                      // The hex dump only: its offset/byte/ASCII grid is
                      // the view, and in a proportional font the three
                      // columns stop lining up at all.
                      //
                      // Base64 used to be pinned here too, on the grounds
                      // that `wrapBase64` lays it out in 76-character MIME
                      // lines. It doesn't need it: those lines are 76
                      // characters in any font, and nothing lines up
                      // *across* them — the only thing a proportional face
                      // costs is a ragged right edge. Pinning it meant the
                      // Font style setting visibly did nothing on two of the
                      // seven payload formats instead of one.
                      forceMonospace={mode === "hex"}
                    />
                    {literal.truncated && (
                      <p className="message-payload-truncation">
                        Showing the first {kilobytes(literal.shown)} KB of {kilobytes(literal.total)} KB.{" "}
                        <button
                          type="button"
                          className="link-button"
                          onClick={() => setExpanded({ payload: payloadBase64 as string, mode })}
                        >
                          Show the whole payload
                        </button>
                      </p>
                    )}
                  </>
                )}
                {mode === "json" &&
                  (isLoadingFullPayload ? (
                    <PayloadLoadingSpinner />
                  ) : json !== undefined ? (
                    // Keyed by payload so every message gets a fresh tree.
                    // `JsonNode` decides whether to start expanded in a
                    // `useState` initialiser, which React runs once per mounted
                    // instance — and nodes reconcile by position and property
                    // name, so without this an `events` node expanded on a
                    // message with three entries stays expanded on the next
                    // message where it holds three thousand, rendering all of
                    // them in one pass.
                    <JsonTreeView key={payloadBase64} value={json} lineNumbers showToolbar={false} />
                  ) : (
                    <p role="alert">Payload is not valid JSON.</p>
                  ))}
                {mode === "avro" && isLoadingFullPayload && <PayloadLoadingSpinner />}
                {mode === "avro" && !isLoadingFullPayload && (
                  <>
                    {decodeAvro.isPending && <p>Decoding…</p>}
                    {decodeAvro.isError && <p role="alert">{decodeAvro.error?.message}</p>}
                    {decodeAvro.isSuccess && (
                      // Same reason as the JSON view above.
                      <JsonTreeView
                        key={payloadBase64}
                        value={decodeAvro.data}
                        lineNumbers
                        showToolbar={false}
                       
                      />
                    )}
                  </>
                )}
                {mode === "protobuf" && isLoadingFullPayload && <PayloadLoadingSpinner />}
                {mode === "protobuf" && !isLoadingFullPayload && (
                  <>
                    {decodeProtobuf.isPending && <p>Decoding…</p>}
                    {decodeProtobuf.isError && <p role="alert">{decodeProtobuf.error?.message}</p>}
                    {decodeProtobuf.isSuccess && (
                      <>
                        {/* Protobuf is the one format that decodes with no
                            schema at all — the wire format carries field
                            numbers and types but no names. That has to be
                            said out loud: a tree of "1", "2", "3" otherwise
                            looks like a schema that decoded badly rather
                            than like no schema at all. */}
                        <p className="message-payload-truncation">
                          {decodeProtobuf.data.source === "none" ? (
                            <>
                              No .proto schema for this topic — showing field numbers read from the wire format. Paste
                              one in the Schema tab for field names.
                            </>
                          ) : (
                            <>
                              Decoded as {decodeProtobuf.data.messageType ?? "a message"} using the{" "}
                              {decodeProtobuf.data.source === "manual" ? "schema saved for this topic" : "Schema Registry"}
                              .
                            </>
                          )}
                        </p>
                        {/* Same reason as the JSON view above. */}
                        <JsonTreeView
                          key={payloadBase64}
                          value={decodeProtobuf.data.value}
                          lineNumbers
                          showToolbar={false}
                         
                        />
                      </>
                    )}
                  </>
                )}
                {mode === "xml" &&
                  (isLoadingFullPayload ? (
                    <PayloadLoadingSpinner />
                  ) : xml !== undefined ? (
                    <XmlTreeView value={xml} lineNumbers showToolbar={false} />
                  ) : (
                    <p role="alert">Payload is not valid XML.</p>
                  ))}
              </div>
            </>
          )}
        </div>
      )}
    </div>
  );
}
