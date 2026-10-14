# Cairn shell integration for zsh (.zshenv stage).
#
# Loaded from a temporary ZDOTDIR. Source the user's real .zshenv from their
# original ZDOTDIR (or $HOME) while keeping our ZDOTDIR in place so our .zshrc
# still loads afterward. ZDOTDIR is restored in our .zshrc, not here.

__cairn_user_zdotdir="${CAIRN_ZDOTDIR_ORIG:-$HOME}"
if [ -f "$__cairn_user_zdotdir/.zshenv" ]; then
  source "$__cairn_user_zdotdir/.zshenv"
fi
