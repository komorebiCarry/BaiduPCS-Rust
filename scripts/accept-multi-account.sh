#!/bin/bash

set -euo pipefail

API_BASE="${API_BASE:-http://127.0.0.1:4924/api/v1}"
ALLOW_SINGLE=0
RESTORE_ACTIVE=1

while [ $# -gt 0 ]; do
    case "$1" in
        --allow-single)
            ALLOW_SINGLE=1
            shift
            ;;
        --no-restore)
            RESTORE_ACTIVE=0
            shift
            ;;
        --api-base)
            API_BASE="$2"
            shift 2
            ;;
        -h|--help)
            cat <<EOF
Usage: $0 [--allow-single] [--no-restore] [--api-base URL]

Runs a low-risk multi-account acceptance sweep against a running backend.
By default it requires at least two saved Baidu accounts, switches through
each account, checks read-only/low-risk feature endpoints, and restores the
original active account before exiting.

Environment:
  API_BASE  API root, default http://127.0.0.1:4924/api/v1
EOF
            exit 0
            ;;
        *)
            echo "unknown argument: $1" >&2
            exit 1
            ;;
    esac
done

need_cmd() {
    command -v "$1" >/dev/null 2>&1 || {
        echo "missing required command: $1" >&2
        exit 1
    }
}

need_cmd curl
need_cmd jq
need_cmd mktemp

tmp_files=()
original_active_uid=""
restoring_active=0
cleanup() {
    if [ "$RESTORE_ACTIVE" -eq 1 ] && [ "$restoring_active" -eq 0 ] && [ -n "$original_active_uid" ]; then
        restoring_active=1
        curl -sS -X POST "$API_BASE/auth/accounts/$original_active_uid/switch" >/dev/null 2>&1 || true
    fi

    for f in "${tmp_files[@]:-}"; do
        [ -n "$f" ] && rm -f "$f"
    done
}
trap cleanup EXIT

request() {
    local method="$1"
    local path="$2"
    local out
    out="$(mktemp)"
    tmp_files+=("$out")

    local http
    http="$(curl -sS -o "$out" -w '%{http_code}' -X "$method" "$API_BASE$path")"
    printf '%s\n' "$http"
    printf '%s\n' "$out"
}

json_get() {
    local file="$1"
    local filter="$2"
    jq -r "$filter" "$file"
}

check_http_ok() {
    local name="$1"
    local http="$2"
    local file="$3"

    if [ "$http" != "200" ]; then
        echo "FAIL $name http=$http"
        sed -n '1,3p' "$file" >&2
        exit 1
    fi
}

check_api_code_if_present() {
    local name="$1"
    local file="$2"
    local code
    code="$(jq -r 'if type == "object" and has("code") then .code else "none" end' "$file")"
    if [ "$code" != "none" ] && [ "$code" != "0" ]; then
        local msg
        msg="$(jq -r '.message // "no message"' "$file")"
        echo "FAIL $name code=$code message=$msg"
        exit 1
    fi
}

summarize_count() {
    local file="$1"
    jq -r '
      if type == "array" then length
      elif type == "object" and (.data | type) == "array" then (.data | length)
      elif type == "object" and (.data.list | type) == "array" then (.data.list | length)
      elif type == "object" and (.data.items | type) == "array" then (.data.items | length)
      elif type == "object" and (.data.tasks | type) == "array" then (.data.tasks | length)
      elif type == "object" and (.data.accounts | type) == "array" then (.data.accounts | length)
      elif type == "object" and (.data.subscriptions | type) == "array" then (.data.subscriptions | length)
      else "-"
      end
    ' "$file"
}

check_get() {
    local name="$1"
    local path="$2"

    mapfile -t result < <(request GET "$path")
    local http="${result[0]}"
    local file="${result[1]}"
    check_http_ok "$name" "$http" "$file"
    check_api_code_if_present "$name" "$file"
    printf '  %-32s http=%s count=%s\n' "$name" "$http" "$(summarize_count "$file")"
}

accounts_file="$(mktemp)"
tmp_files+=("$accounts_file")
accounts_http="$(curl -sS -o "$accounts_file" -w '%{http_code}' "$API_BASE/auth/accounts")"
check_http_ok "auth.accounts" "$accounts_http" "$accounts_file"
check_api_code_if_present "auth.accounts" "$accounts_file"

account_count="$(json_get "$accounts_file" '.data.accounts | length')"
active_uid="$(json_get "$accounts_file" '.data.active_uid // empty')"
original_active_uid="$active_uid"
echo "accounts=$account_count active_uid=${active_uid:-none}"

if [ "$account_count" -lt 2 ] && [ "$ALLOW_SINGLE" -ne 1 ]; then
    echo "FAIL need at least 2 saved accounts for real multi-account acceptance"
    exit 2
fi

if [ "$account_count" -lt 1 ]; then
    echo "FAIL no saved accounts"
    exit 2
fi

mapfile -t uids < <(jq -r '.data.accounts[].uid' "$accounts_file")

switch_account() {
    local uid="$1"
    mapfile -t result < <(request POST "/auth/accounts/$uid/switch")
    local http="${result[0]}"
    local file="${result[1]}"
    check_http_ok "auth.switch.$uid" "$http" "$file"
    check_api_code_if_present "auth.switch.$uid" "$file"

    local switched_uid
    switched_uid="$(jq -r '.data.user.uid // empty' "$file")"
    if [ "$switched_uid" != "$uid" ]; then
        echo "FAIL switch returned uid=$switched_uid expected=$uid"
        exit 1
    fi

    printf 'switch uid=%s ok\n' "$uid"
}

check_active_account() {
    local uid="$1"
    mapfile -t result < <(request GET "/auth/accounts")
    local http="${result[0]}"
    local file="${result[1]}"
    check_http_ok "auth.accounts.after-switch" "$http" "$file"
    check_api_code_if_present "auth.accounts.after-switch" "$file"

    local active_count active_uid_after
    active_count="$(jq -r '[.data.accounts[] | select(.is_active == true)] | length' "$file")"
    active_uid_after="$(jq -r '[.data.accounts[] | select(.is_active == true) | .uid][0] // empty' "$file")"
    if [ "$active_count" != "1" ] || [ "$active_uid_after" != "$uid" ]; then
        echo "FAIL active account mismatch count=$active_count uid=$active_uid_after expected=$uid"
        exit 1
    fi
}

check_current_user() {
    local uid="$1"
    mapfile -t result < <(request GET "/auth/user")
    local http="${result[0]}"
    local file="${result[1]}"
    check_http_ok "auth.user" "$http" "$file"
    check_api_code_if_present "auth.user" "$file"

    local current_uid
    current_uid="$(jq -r '.data.uid // empty' "$file")"
    if [ "$current_uid" != "$uid" ]; then
        echo "FAIL current user uid=$current_uid expected=$uid"
        exit 1
    fi
    printf '  %-32s http=%s uid=%s\n' "auth.user" "$http" "$current_uid"
}

check_feature_sweep() {
    check_get "files.root" "/files?dir=/&page=1&page_size=1"
    check_get "downloads" "/downloads"
    check_get "downloads.all" "/downloads/all"
    check_get "downloads.active" "/downloads/active"
    check_get "downloads.folders" "/downloads/folders"
    check_get "uploads" "/uploads"
    check_get "transfers" "/transfers"
    check_get "fs.roots" "/fs/roots"
    check_get "fs.list.root" "/fs/list?path=/&page=0&page_size=1"
    check_get "local-files.default" "/local-files?page=0&page_size=1"
    check_get "config" "/config"
    check_get "config.transfer" "/config/transfer"
    check_get "proxy.status" "/proxy/status"
    check_get "autobackup.configs" "/autobackup/configs"
    check_get "autobackup.status" "/autobackup/status"
    check_get "autobackup.stats" "/autobackup/stats"
    check_get "encryption.status" "/encryption/status"
    check_get "share-sync.subscriptions" "/share-sync/subscriptions"
    check_get "cloud-dl.tasks" "/cloud-dl/tasks"
    check_get "shares" "/shares?page=1"
    check_get "system.watch-capability" "/system/watch-capability"
    check_get "config.autobackup.trigger" "/config/autobackup/trigger"

    local sub_file sub_http sub_id
    sub_file="$(mktemp)"
    tmp_files+=("$sub_file")
    sub_http="$(curl -sS -o "$sub_file" -w '%{http_code}' "$API_BASE/share-sync/subscriptions")"
    check_http_ok "share-sync.subscriptions.recheck" "$sub_http" "$sub_file"
    sub_id="$(jq -r 'if type == "array" then .[0].id // empty elif (.data | type) == "array" then .data[0].id // empty else empty end' "$sub_file")"
    if [ -n "$sub_id" ]; then
        check_get "share-sync.runs" "/share-sync/subscriptions/$sub_id/runs"
        check_get "share-sync.snapshot" "/share-sync/subscriptions/$sub_id/snapshots/latest"
    fi
}

for uid in "${uids[@]}"; do
    echo "== account $uid =="
    switch_account "$uid"
    check_active_account "$uid"
    check_current_user "$uid"
    check_feature_sweep
done

if [ "$RESTORE_ACTIVE" -eq 1 ] && [ -n "$original_active_uid" ]; then
    switch_account "$original_active_uid" >/dev/null
    check_active_account "$original_active_uid"
    restoring_active=1
    echo "restored active_uid=$original_active_uid"
fi

echo "multi-account acceptance sweep passed"
