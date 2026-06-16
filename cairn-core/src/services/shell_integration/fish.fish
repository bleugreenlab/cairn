# Cairn shell integration for fish: OSC 133 semantic prompt markers.
#
# Loaded via `fish --init-command 'source <this>'`, after the user's config.fish.
# Install preexec/postexec event handlers that emit OSC 133 C/D markers.

function __cairn_osc133_preexec --on-event fish_preexec
    printf '\033]133;C\007'
end

function __cairn_osc133_postexec --on-event fish_postexec
    printf '\033]133;D;%s\007' $status
    printf '\033]133;A\007'
end
