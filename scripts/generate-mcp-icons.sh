#!/bin/sh
set -eu

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
stylesheet=$(mktemp)
trap 'rm -f "$stylesheet"' EXIT HUP INT TERM

mkdir -p "$repo_root/internal/porcupine/assets"
for theme in light dark; do
	case "$theme" in
		light) color='#000' ;;
		dark) color='#fff' ;;
	esac
	printf ':root { color: %s !important; }\n' "$color" > "$stylesheet"
	rsvg-convert --width 96 --height 96 --stylesheet "$stylesheet" \
		--output "$repo_root/internal/porcupine/assets/mcp-icon-$theme.png" \
		"$repo_root/site/assets/favicon.svg"
done
