#!/bin/sh
set -e

apk add --no-cache jq
npm install -g --force yarn@latest
cd "$1"
APP_DIR=$(pwd)

# Workspace packages that ship from this checkout instead of their published release. Each is
# packed next to the app tarball and the app depends on that file, so patches to it reach the image.
BUNDLED="@subql/node-core"

workspace_directory() {
  jq --arg dep "$1" -r '.compilerOptions.paths[$dep][0]' ../../tsconfig.json | cut -d'/' -f 2
}

# Replace "workspace:~" versions in the current directory's package.json with actual versions
pin_workspace_versions() {
  jq -r '.dependencies | to_entries[] | select(.value == "workspace:~") | .key' package.json | while read -r dep; do
    directory=$(workspace_directory "$dep")
    version=$(jq --arg directory "$directory" -r '.version' ../"$directory"/package.json)
    if [ "$version" != null ]; then
      jq --arg dep "$dep" --arg version "$version" -r '.dependencies[$dep] = $version' package.json > package.tmp.json && mv package.tmp.json package.json
    fi
  done
}

for dep in $(jq -r '.dependencies | to_entries[] | select(.value == "workspace:~") | .key' package.json); do
  case " $BUNDLED " in
    *" $dep "*)
      directory=$(workspace_directory "$dep")
      (cd ../"$directory" && pin_workspace_versions && yarn pack --filename "$APP_DIR/$directory.tgz")
      jq --arg dep "$dep" --arg file "file:./$directory.tgz" -r '.dependencies[$dep] = $file' package.json > package.tmp.json && mv package.tmp.json package.json
      ;;
  esac
done

pin_workspace_versions

yarn pack --filename app.tgz
rm -rf /root/.npm /root/.cache
