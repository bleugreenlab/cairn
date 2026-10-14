export type Modifier = "meta" | "ctrl" | "shift" | "alt";

export interface KeyStroke {
  key: string;
  modifiers: Modifier[];
}

/** An ordered list of strokes. Empty is reserved for a disabled customization. */
export type KeySequence = readonly KeyStroke[];

/** @deprecated Use KeyStroke. */
export type Keybind = KeyStroke;

export type ActionCategory = "navigation" | "actions" | "focus" | "chat";
export type ActionContext =
  | "global"
  | "issueList"
  | "issueDetail"
  | "jobSelected"
  | "projectTerminal"
  | "chat"
  | "dialog";

export interface ActionDefinition {
  id: string;
  label: string;
  category: ActionCategory;
  contexts: readonly ActionContext[];
  defaultSequence: KeySequence;
  alternativeSequences?: readonly KeySequence[];
  description?: string;
  keywords?: readonly string[];
  editable?: true;
  /** @deprecated Sequences now encode prefixes explicitly. */
  isGPrefixAction?: false;
}

export interface KeybindCustomization {
  action: string;
  sequence: KeySequence;
  description?: string;
}

export interface KeybindsFile {
  version: 2;
  keybinds: KeybindCustomization[];
}

export interface ResolvedAction {
  definition: ActionDefinition;
  sequence: KeySequence;
  alternativeSequences: readonly KeySequence[];
  /** Compatibility projection for one-stroke consumers during dispatcher migration. */
  keybind: KeyStroke;
  alternativeKeybinds: KeyStroke[];
  isCustomized: boolean;
  isDisabled: boolean;
}

export type Platform = "macos" | "windows" | "linux";

const MODIFIER_ORDER: readonly Modifier[] = ["meta", "ctrl", "alt", "shift"];
const SHIFTED_PUNCTUATION = new Set([
  "?", "!", "@", "#", "$", "%", "^", "&", "*", "(", ")", "_", "+",
  "{", "}", "|", ":", '"', "<", ">", "~",
]);

export function keybind(key: string, ...modifiers: Modifier[]): KeyStroke {
  return normalizeStroke({ key, modifiers });
}

export function keySequence(...strokes: KeyStroke[]): KeySequence {
  return strokes.map(normalizeStroke);
}

export function normalizeKey(key: string): string {
  if (key === " ") return " ";
  const lower = key.toLowerCase();
  const special: Record<string, string> = {
    up: "ArrowUp", arrowup: "ArrowUp",
    down: "ArrowDown", arrowdown: "ArrowDown",
    left: "ArrowLeft", arrowleft: "ArrowLeft",
    right: "ArrowRight", arrowright: "ArrowRight",
    esc: "Escape", escape: "Escape",
    return: "Enter", enter: "Enter",
    space: " ", spacebar: " ",
    backspace: "Backspace", tab: "Tab",
  };
  return special[lower] ?? (key.length === 1 ? lower : key);
}

export function normalizeStroke(stroke: KeyStroke): KeyStroke {
  const modifiers = MODIFIER_ORDER.filter((modifier) => stroke.modifiers.includes(modifier));
  return { key: normalizeKey(stroke.key), modifiers };
}

export function normalizeSequence(sequence: KeySequence): KeySequence {
  return sequence.map(normalizeStroke);
}

export function strokeSignature(stroke: KeyStroke): string {
  const normalized = normalizeStroke(stroke);
  return `${normalized.modifiers.join("+")}:${normalized.key.toLowerCase()}`;
}

export function sequenceSignature(sequence: KeySequence): string {
  return normalizeSequence(sequence).map(strokeSignature).join(" ");
}

export function eventToStroke(event: KeyboardEvent): KeyStroke {
  const modifiers: Modifier[] = [];
  if (event.metaKey) modifiers.push("meta");
  if (event.ctrlKey) modifiers.push("ctrl");
  if (event.altKey) modifiers.push("alt");
  // Shift is implicit for punctuation KeyboardEvent.key values.
  if (event.shiftKey && !SHIFTED_PUNCTUATION.has(event.key)) modifiers.push("shift");
  return normalizeStroke({ key: event.key, modifiers });
}

export function matchesKeybind(event: KeyboardEvent, bind: KeyStroke): boolean {
  return strokeSignature(eventToStroke(event)) === strokeSignature(bind);
}

export function matchesStroke(a: KeyStroke, b: KeyStroke): boolean {
  return strokeSignature(a) === strokeSignature(b);
}

function formatKey(key: string): string {
  const names: Record<string, string> = {
    ArrowUp: "↑", ArrowDown: "↓", ArrowLeft: "←", ArrowRight: "→",
    Enter: "Enter", Escape: "Esc", " ": "Space", Backspace: "Backspace", Tab: "Tab",
  };
  return names[normalizeKey(key)] ?? key.toUpperCase();
}

export function formatStroke(stroke: KeyStroke, platform: Platform = detectPlatform()): string {
  const normalized = normalizeStroke(stroke);
  const mac = platform === "macos";
  const labels: Record<Modifier, string> = mac
    ? { meta: "⌘", ctrl: "⌃", alt: "⌥", shift: "⇧" }
    : { meta: platform === "windows" ? "Meta" : "Super", ctrl: "Ctrl", alt: "Alt", shift: "Shift" };
  const parts = normalized.modifiers.map((modifier) => labels[modifier]);
  parts.push(formatKey(normalized.key));
  return parts.join(mac ? "" : "+");
}

export function formatSequence(sequence: KeySequence, platform: Platform = detectPlatform()): string {
  return sequence.map((stroke) => formatStroke(stroke, platform)).join(" ");
}

/** @deprecated Use formatSequence. */
export function formatKeybind(bind: KeyStroke, _legacyPrefix?: boolean): string {
  return formatStroke(bind);
}

export function detectPlatform(userAgent = typeof navigator === "undefined" ? "" : navigator.userAgent): Platform {
  if (/mac|iphone|ipad/i.test(userAgent)) return "macos";
  if (/win/i.test(userAgent)) return "windows";
  return "linux";
}

export function parseKeybindString(value: string): KeyStroke | null {
  const input = value.trim();
  if (!input) return null;
  const parts = input.split("+");
  const key = parts.pop() ?? "";
  const modifiers: Modifier[] = [];
  for (const raw of parts) {
    const part = raw.trim().toLowerCase();
    if (["cmd", "meta", "⌘"].includes(part)) modifiers.push("meta");
    else if (["ctrl", "control", "⌃"].includes(part)) modifiers.push("ctrl");
    else if (["alt", "option", "⌥"].includes(part)) modifiers.push("alt");
    else if (["shift", "⇧"].includes(part)) modifiers.push("shift");
  }
  return normalizeStroke({ key, modifiers });
}
