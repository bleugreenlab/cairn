/**
 * Default Action Definitions
 *
 * The canonical list of all keyboard shortcuts in the application.
 * These are the defaults that can be customized by users.
 */

import type { ActionDefinition } from "./types";
import { keySequence, keybind } from "./types";

/**
 * All default actions organized by category.
 * Action IDs follow the pattern: category.action (e.g., 'issue.create', 'nav.nextIssue')
 */
export const actionManifest = [
  // ============================================================================
  // GLOBAL ACTIONS
  // ============================================================================
  {
    id: "view.toggleSidebar",
    label: "Toggle sidebar",
    category: "navigation",
    contexts: ["global"],
    defaultSequence: keySequence(keybind("b", "meta")),
    alternativeSequences: [keySequence(keybind("b", "ctrl"))],
  },
  // ============================================================================
  // ISSUE LIST VIEW
  // ============================================================================
  {
    id: "issue.focusNext",
    label: "Focus next issue",
    category: "navigation",
    contexts: ["issueList"],
    defaultSequence: keySequence(keybind("ArrowDown")),
  },
  {
    id: "issue.focusPrevious",
    label: "Focus previous issue",
    category: "navigation",
    contexts: ["issueList"],
    defaultSequence: keySequence(keybind("ArrowUp")),
  },
  {
    id: "issue.open",
    label: "Open focused issue",
    category: "actions",
    contexts: ["issueList"],
    defaultSequence: keySequence(keybind("Enter")),
  },
  {
    id: "voice.pushToTalk",
    label: "Dictate into the focused field",
    category: "actions",
    contexts: ["global"],
    defaultSequence: keySequence(keybind("d", "meta")),
    keywords: ["voice", "dictation", "microphone", "speech"],
  },

  // ============================================================================
  // ISSUE DETAIL VIEW (OVERVIEW - no job selected)
  // ============================================================================
  {
    id: "issue.setStatus",
    label: "Set issue status",
    category: "actions",
    contexts: ["issueDetail", "jobSelected"],
    defaultSequence: keySequence(keybind("s")),
  },
  {
    id: "execution.editSnapshot",
    label: "Edit execution snapshot",
    category: "actions",
    contexts: ["issueDetail", "jobSelected"],
    defaultSequence: keySequence(keybind("e", "meta")),
  },

  // ============================================================================
  // ISSUE DETAIL VIEW (JOB SELECTED)
  // ============================================================================
  {
    id: "job.previousFacet",
    label: "Previous facet",
    category: "navigation",
    contexts: ["jobSelected"],
    defaultSequence: keySequence(keybind("ArrowLeft")),
  },
  {
    id: "job.nextFacet",
    label: "Next facet",
    category: "navigation",
    contexts: ["jobSelected"],
    defaultSequence: keySequence(keybind("ArrowRight")),
  },
  {
    id: "job.previousNode",
    label: "Previous node",
    category: "navigation",
    contexts: ["jobSelected"],
    defaultSequence: keySequence(keybind("ArrowLeft", "shift")),
  },
  {
    id: "job.nextNode",
    label: "Next node",
    category: "navigation",
    contexts: ["jobSelected"],
    defaultSequence: keySequence(keybind("ArrowRight", "shift")),
  },

  // ============================================================================
  // WORKSPACE PANE FOCUS (every pane leaf across both split tiers)
  // ============================================================================
  {
    id: "workspace.focusPaneUp",
    label: "Focus previous pane",
    category: "navigation",
    contexts: ["issueDetail", "jobSelected"],
    defaultSequence: keySequence(keybind("ArrowUp")),
  },
  {
    id: "workspace.focusPaneDown",
    label: "Focus next pane",
    category: "navigation",
    contexts: ["issueDetail", "jobSelected"],
    defaultSequence: keySequence(keybind("ArrowDown")),
  },

  // ============================================================================
  // SIDEBAR NAVIGATION (navigates active/waiting issues across all projects)
  // ============================================================================
  {
    id: "sidebar.previousIssue",
    label: "Previous sidebar issue",
    category: "navigation",
    contexts: ["issueDetail", "jobSelected"],
    defaultSequence: keySequence(keybind("j")),
  },
  {
    id: "sidebar.nextIssue",
    label: "Next sidebar issue",
    category: "navigation",
    contexts: ["issueDetail", "jobSelected"],
    defaultSequence: keySequence(keybind("k")),
  },

  // ============================================================================
  // TAB ACTIONS
  // ============================================================================
  {
    id: "tab.close",
    label: "Close current tab or pane",
    category: "actions",
    contexts: ["jobSelected"],
    defaultSequence: keySequence(keybind("Backspace", "meta")),
  },
  {
    id: "workspace.closeIssuePane",
    label: "Close issue pane",
    category: "actions",
    contexts: ["issueDetail", "jobSelected"],
    defaultSequence: keySequence(keybind("Backspace", "meta", "shift")),
  },

  // ============================================================================
  // CHAT NAVIGATION
  // ============================================================================
  {
    id: "chat.jumpToTop",
    label: "Jump to top of chat",
    category: "chat",
    contexts: ["chat"],
    defaultSequence: keySequence(keybind("ArrowUp", "meta")),
  },
  {
    id: "chat.jumpToBottom",
    label: "Jump to bottom of chat",
    category: "chat",
    contexts: ["chat"],
    defaultSequence: keySequence(keybind("ArrowDown", "meta")),
  },
  {
    id: "chat.stop",
    label: "Stop/interrupt running chat",
    category: "chat",
    contexts: ["chat"],
    defaultSequence: keySequence(keybind("Backspace", "meta")),
  },

  // ============================================================================
  // G-PREFIX NAVIGATION (vim-style go-to)
  // ============================================================================
  {
    id: "go.issues",
    label: "Go to issues list",
    category: "navigation",
    contexts: ["global"],
    defaultSequence: keySequence(keybind("g"), keybind("g")),
  },
  {
    id: "go.recipes",
    label: "Go to recipes",
    category: "navigation",
    contexts: ["global"],
    defaultSequence: keySequence(keybind("g"), keybind("r")),
  },
  {
    id: "go.settings",
    label: "Go to settings",
    category: "navigation",
    contexts: ["global"],
    defaultSequence: keySequence(keybind("g"), keybind("s")),
  },
  {
    id: "go.terminal",
    label: "Go to project terminal",
    category: "navigation",
    contexts: ["global"],
    defaultSequence: keySequence(keybind("g"), keybind("t")),
  },
  {
    id: "go.files",
    label: "Go to project settings",
    category: "navigation",
    contexts: ["global"],
    defaultSequence: keySequence(keybind("g"), keybind("f")),
  },

  // ============================================================================
  // PROJECT NAVIGATION
  // ============================================================================
  {
    id: "project.previous",
    label: "Previous project",
    category: "navigation",
    contexts: ["global"],
    defaultSequence: keySequence(keybind("[", "meta")),
  },
  {
    id: "project.next",
    label: "Next project",
    category: "navigation",
    contexts: ["global"],
    defaultSequence: keySequence(keybind("]", "meta")),
  },

  // ============================================================================
  // JOB ACTIONS
  // ============================================================================
  {
    id: "job.openActions",
    label: "Open job actions",
    category: "actions",
    contexts: ["jobSelected"],
    defaultSequence: keySequence(keybind("t", "meta")),
  },

  // ============================================================================
  // PROJECT TERMINAL ACTIONS
  // ============================================================================
  {
    id: "projectTerminal.newTerminal",
    label: "New project terminal",
    category: "actions",
    contexts: ["projectTerminal"],
    defaultSequence: keySequence(keybind("t", "meta")),
    alternativeSequences: [keySequence(keybind("t", "ctrl"))],
  },
  {
    id: "terminal.prevCommand",
    label: "Jump to previous command",
    category: "navigation",
    contexts: ["projectTerminal"],
    defaultSequence: keySequence(keybind("ArrowUp", "meta")),
    alternativeSequences: [keySequence(keybind("ArrowUp", "ctrl"))],
  },
  {
    id: "terminal.nextCommand",
    label: "Jump to next command",
    category: "navigation",
    contexts: ["projectTerminal"],
    defaultSequence: keySequence(keybind("ArrowDown", "meta")),
    alternativeSequences: [keySequence(keybind("ArrowDown", "ctrl"))],
  },

  // ============================================================================
  // EXECUTION ACTIONS
  // ============================================================================
  {
    id: "execution.expand",
    label: "Expand execution form",
    category: "actions",
    contexts: ["issueDetail", "dialog"],
    defaultSequence: keySequence(keybind("n", "meta")),
  },
  {
    id: "execution.cycleRecipe",
    label: "Cycle through recipes",
    category: "actions",
    contexts: ["issueDetail", "dialog"],
    defaultSequence: keySequence(keybind("j", "meta")),
  },
  {
    id: "execution.cycleModel",
    label: "Cycle through models",
    category: "actions",
    contexts: ["issueDetail", "dialog"],
    defaultSequence: keySequence(keybind("l", "meta")),
  },
  {
    id: "execution.start",
    label: "Start composed execution",
    category: "actions",
    contexts: ["issueDetail", "dialog"],
    defaultSequence: keySequence(keybind("Enter", "meta")),
    alternativeSequences: [keySequence(keybind("Enter", "ctrl"))],
  },

  // ============================================================================
  // GLOBAL ACTIONS
  // ============================================================================
  {
    id: "issue.create",
    label: "Create new issue",
    category: "actions",
    contexts: ["global"],
    defaultSequence: keySequence(keybind("c")),
  },
  {
    id: "palette.open",
    label: "Open command palette",
    category: "actions",
    contexts: ["global"],
    defaultSequence: keySequence(keybind("k", "meta")),
  },
  {
    id: "help.showShortcuts",
    label: "Show keyboard shortcuts",
    category: "actions",
    contexts: ["global"],
    defaultSequence: keySequence(keybind("?")),
  },
  {
    id: "logs.popout",
    label: "Open log viewer window",
    category: "actions",
    contexts: ["global"],
    defaultSequence: keySequence(keybind("l", "meta", "shift")),
  },
  {
    id: "workspace.autoSplitActiveIssues",
    label: "Split workspace to active issues",
    category: "actions",
    contexts: ["global"],
    defaultSequence: keySequence(keybind("a", "meta", "shift")),
  },

] as const satisfies readonly ActionDefinition[];

/** Stable-order compatibility alias. */
export const defaultActions = actionManifest;
