# Cairn shell integration for zsh (.zshrc stage): OSC 133 semantic prompt markers.
#
# Source the user's real .zshrc, restore the original ZDOTDIR so nested shells
# behave, then install preexec/precmd hooks that emit OSC 133 C/D markers.

__cairn_user_zdotdir="${CAIRN_ZDOTDIR_ORIG:-$HOME}"
if [ -f "$__cairn_user_zdotdir/.zshrc" ]; then
  source "$__cairn_user_zdotdir/.zshrc"
fi

if [ -n "$CAIRN_ZDOTDIR_ORIG" ]; then
  export ZDOTDIR="$CAIRN_ZDOTDIR_ORIG"
else
  unset ZDOTDIR
fi
unset CAIRN_ZDOTDIR_ORIG __cairn_user_zdotdir

__cairn_osc133_preexec() {
  printf '\033]133;C\007'
}
__cairn_osc133_precmd() {
  # Capture the just-finished command's status before anything else clobbers it.
  printf '\033]133;D;%s\007' "$?"
  printf '\033]133;A\007'
}

autoload -Uz add-zsh-hook 2>/dev/null
if (( $+functions[add-zsh-hook] )); then
  add-zsh-hook preexec __cairn_osc133_preexec
  add-zsh-hook precmd __cairn_osc133_precmd
else
  typeset -ga preexec_functions precmd_functions
  preexec_functions+=(__cairn_osc133_preexec)
  precmd_functions+=(__cairn_osc133_precmd)
fi
