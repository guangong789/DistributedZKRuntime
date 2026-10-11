#!/usr/bin/env bash
# DEVELOPMENT ONLY: one offline CA and three localhost identities. No rotation,
# revocation, transfer or service startup. Run explicitly; --out is mandatory.
set -euo pipefail
umask 077

fail() { printf 'dev-pki: %s\n' "$1" >&2; exit 1; }
usage() { printf 'Usage: %s --out DIRECTORY\nRequires OpenSSL 3, Go and Git. Existing material is never reused or overwritten.\n' "$0"; }
if [[ ${1:-} == --help && $# == 1 ]]; then usage; exit 0; fi
[[ $# == 2 && $1 == --out && -n $2 ]] || { usage >&2; exit 1; }
for tool in openssl go git realpath mktemp stat cmp ln chmod mkdir rm grep; do
    command -v "$tool" >/dev/null 2>&1 || fail "required tool is missing: $tool"
done
version=$(openssl version) || fail 'cannot query OpenSSL version'
[[ $version == 'OpenSSL 3.'* ]] || fail 'OpenSSL 3.x is required'
repo=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd -P)
out=$(realpath -ms -- "$2")
[[ $out != / && $out != *$'\n'* && $out != *$'\r'* ]] || fail 'invalid output directory'

# Check every component before mkdir/chmod; never resolve an existing symlink.
check_path() {
    local path=$1 probe='' component
    local -a components
    IFS=/ read -r -a components <<< "$path"
    for component in "${components[@]}"; do
        [[ -n $component ]] || continue
        probe+="/$component"
        [[ ! -L $probe ]] || fail 'output path contains a symlink'
        [[ ! -e $probe || -d $probe ]] || fail 'output path contains a non-directory'
    done
}
check_path "$out"
ancestor=$out
while [[ ! -d $ancestor ]]; do ancestor=$(dirname -- "$ancestor"); done
git_root=$(git -C "$ancestor" rev-parse --show-toplevel 2>/dev/null || true)
if [[ -n $git_root ]]; then
    [[ $git_root == "$repo" && $out == "$repo/private/"* ]] || fail 'Git outputs must be inside the ignored private/ directory of this repository'
    for file in ca/ca.crt ca/ca.key coordinator/cert.pem coordinator/key.pem worker-1/cert.pem worker-1/key.pem client-1/cert.pem client-1/key.pem; do
        relative=${out#"$repo/"}/$file
        git -C "$repo" check-ignore -q -- "$relative" || fail 'a PKI output is not ignored by Git'
        if git -C "$repo" ls-files --error-unmatch -- "$relative" >/dev/null 2>&1; then
            fail 'a PKI output is already tracked by Git'
        fi
    done
fi

# Permit the developer's pre-created, empty node directories only.
shopt -s nullglob dotglob
if [[ -d $out ]]; then
    for entry in "$out"/*; do
        case ${entry##*/} in ca|coordinator|worker-1|client-1) ;; *) fail 'output is not empty; back up existing material or choose a new empty directory' ;; esac
        [[ -d $entry && ! -L $entry ]] || fail 'unexpected file or symlink in output'
        contents=("$entry"/*)
        [[ ${#contents[@]} == 0 ]] || fail 'output contains material; back it up or choose a new empty directory'
    done
fi
mkdir -p -- "$out"
check_path "$out"
chmod 0700 -- "$out"
work=$(mktemp -d "$out/.bootstrap.XXXXXXXX")
complete=false
cleanup() {
    local node file
    local -a files
    if [[ $complete != true ]]; then
        # Match hard-link identity, including interruption immediately after
        # ln. Never delete another invocation's or pre-existing material.
        for node in ca coordinator worker-1 client-1; do
            if [[ $node == ca ]]; then files=(ca.crt ca.key); else files=(cert.pem key.pem); fi
            for file in "${files[@]}"; do
                if [[ ! -L $out/$node/$file && $out/$node/$file -ef $work/$node/$file ]]; then
                    rm -f -- "$out/$node/$file"
                fi
            done
        done
    fi
    rm -rf -- "$work"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
trap 'exit 129' HUP
openssl_checked() {
    openssl "$@" 2>"$work/openssl-error.log" || fail 'OpenSSL operation failed; no successful issuance'
}
serial() {
    local random
    random=$(openssl_checked rand -hex 16) || fail 'random serial generation failed'
    printf '0x01%s\n' "$random"
}
for node in ca coordinator worker-1 client-1; do mkdir -m 0700 -- "$work/$node"; done
openssl_checked genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256 -out "$work/ca/ca.key"
ca_serial=$(serial) || fail 'random CA serial generation failed'
openssl_checked req -new -x509 -batch -sha256 -days 30 -set_serial "$ca_serial" \
    -key "$work/ca/ca.key" -out "$work/ca/ca.crt" -subj '/CN=DistributedZKRuntime Development CA' \
    -addext 'basicConstraints=critical,CA:TRUE,pathlen:0' \
    -addext 'keyUsage=critical,keyCertSign,cRLSign' -addext 'subjectKeyIdentifier=hash'

issue() {
    local node=$1 identity=$2 eku=$3 san leaf_serial
    san="URI:$identity"
    if [[ $node != client-1 ]]; then san+=',IP:127.0.0.1,DNS:localhost'; fi
    cat >"$work/leaf.conf" <<EOF
[leaf]
basicConstraints=critical,CA:FALSE
keyUsage=critical,digitalSignature
extendedKeyUsage=$eku
subjectAltName=$san
subjectKeyIdentifier=hash
authorityKeyIdentifier=keyid,issuer
EOF
    openssl_checked genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256 -out "$work/$node/key.pem"
    openssl_checked req -new -batch -sha256 -key "$work/$node/key.pem" -out "$work/leaf.csr" -subj "/CN=Development $node"
    leaf_serial=$(serial) || fail 'random leaf serial generation failed'
    openssl_checked x509 -req -sha256 -days 7 -set_serial "$leaf_serial" \
        -in "$work/leaf.csr" -CA "$work/ca/ca.crt" -CAkey "$work/ca/ca.key" \
        -extfile "$work/leaf.conf" -extensions leaf -out "$work/$node/cert.pem" >/dev/null
}
issue coordinator spiffe://distributedzk.local/coordinator/main serverAuth,clientAuth
issue worker-1 spiffe://distributedzk.local/worker/worker-1 serverAuth,clientAuth
issue client-1 spiffe://distributedzk.local/client/client-1 clientAuth

openssl_checked verify -no-CApath -no-CAstore -CAfile "$work/ca/ca.crt" -check_ss_sig -x509_strict "$work/ca/ca.crt" >/dev/null
for node in coordinator worker-1 client-1; do
    openssl_checked verify -no-CApath -no-CAstore -CAfile "$work/ca/ca.crt" -x509_strict -purpose sslclient "$work/$node/cert.pem" >/dev/null
    openssl_checked x509 -in "$work/$node/cert.pem" -noout -ext subjectAltName,extendedKeyUsage >"$work/extensions.txt"
    case $node in
        coordinator) identity=spiffe://distributedzk.local/coordinator/main ;;
        worker-1) identity=spiffe://distributedzk.local/worker/worker-1 ;;
        client-1) identity=spiffe://distributedzk.local/client/client-1 ;;
    esac
    grep -Fq "URI:$identity" "$work/extensions.txt" || fail 'unexpected URI SAN'
    grep -Fq 'TLS Web Client Authentication' "$work/extensions.txt" || fail 'missing ClientAuth EKU'
    if [[ $node != client-1 ]]; then
        grep -Fq 'IP Address:127.0.0.1' "$work/extensions.txt" || fail 'missing loopback IP SAN'
        grep -Fq 'TLS Web Server Authentication' "$work/extensions.txt" || fail 'missing ServerAuth EKU'
        openssl_checked verify -no-CApath -no-CAstore -CAfile "$work/ca/ca.crt" -x509_strict -purpose sslserver -verify_ip 127.0.0.1 "$work/$node/cert.pem" >/dev/null
    fi
done
# Compare only public keys, including the CA; no private material is printed.
for node in ca coordinator worker-1 client-1; do
    cert=$work/$node/cert.pem; key=$work/$node/key.pem
    if [[ $node == ca ]]; then cert=$work/ca/ca.crt; key=$work/ca/ca.key; fi
    openssl_checked x509 -in "$cert" -pubkey -noout >"$work/cert.pub"
    openssl_checked pkey -in "$key" -pubout >"$work/key.pub"
    cmp -s "$work/cert.pub" "$work/key.pub" || fail 'certificate/key mismatch'
done
(cd -- "$repo" && go run -mod=readonly ./scripts/dev-pki -dir "$work" >/dev/null) || fail 'Go rpcauth compatibility verification failed'

for node in ca coordinator worker-1 client-1; do
    check_path "$out/$node"
    mkdir -p -- "$out/$node"
    chmod 0700 -- "$out/$node"
    if [[ $node == ca ]]; then files=(ca.crt ca.key); else files=(cert.pem key.pem); fi
    for file in "${files[@]}"; do
        check_path "$out/$node"
        chmod 0600 -- "$work/$node/$file"
        # Never follow a destination symlink or treat its name as a directory.
        ln -T -- "$work/$node/$file" "$out/$node/$file" || fail 'publication refused; output changed during issuance'
    done
done
complete=true
printf 'Development PKI created and verified: %s\nCA: 30 days; leaves: 7 days; ECDSA P-256 / SHA-256. Keep ca/ca.key offline.\n' "$out"
