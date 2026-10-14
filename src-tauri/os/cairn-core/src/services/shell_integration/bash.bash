# Cairn shell integration for bash: OSC 133 semantic prompt markers.
#
# Sourced via `bash --rcfile`. Load the user's real .bashrc first, then install
# preexec (DEBUG trap) and precmd (PROMPT_COMMAND) hooks that emit OSC 133 C/D
# markers. Note: a DEBUG trap set by the user's .bashrc is overridden here.

if [ -f ~/.bashrc ]; then
  . ~/.bashrc
fi

__cairn_preexec_ran=""

__cairn_preexec() {
  # The DEBUG trap fires before every simple command, including those run from
  # PROMPT_COMMAND. Emit the command-start marker only once per prompt, and never
  # while the precmd hook itself is running.
  if [ -n "$COMP_LINE" ]; then return; fi
  case "$BASH_COMMAND" in
    __cairn_precmd*) return ;;
  esac
  if [ -n "$__cairn_preexec_ran" ]; then return; fi
  __cairn_preexec_ran=1
  printf '\033]133;C\007'
}

__cairn_precmd() {
  # Capture the just-finished command's status before anything else clobbers it.
  local __cairn_st=$?
  printf '\033]133;D;%s\007' "$__cairn_st"
  __cairn_preexec_ran=""
  printf '\033]133;A\007'
  return $__cairn_st
}

case ";$PROMPT_COMMAND;" in
  *";__cairn_precmd;"*) ;;
  *) PROMPT_COMMAND="__cairn_precmd${PROMPT_COMMAND:+;$PROMPT_COMMAND}" ;;
esac

trap '__cairn_preexec' DEBUG
