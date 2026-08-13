#!/usr/bin/env bash
# Push the API from this laptop to EC2 and restart it.
#
#   deploy/deploy.sh              # sync + restart
#   deploy/deploy.sh --provision  # sync, then run provision.sh remotely
#   deploy/deploy.sh --migrate    # sync, then apply deploy/sql/
#
# Mirrors prosite_flask/deploy/deploy.sh so both apps are operated the same way.
set -euo pipefail

# Target host and SSH key are deliberately not hardcoded — this repository is
# public. Put them in deploy/deploy.env (gitignored), or set them in the
# environment:
#
#   RESTAPI_HOST=1.2.3.4
#   RESTAPI_SSH_KEY=$HOME/.ssh/your-key.pem
#
[[ -f "$(dirname "$0")/deploy.env" ]] && . "$(dirname "$0")/deploy.env"

HOST="${RESTAPI_HOST:-}"
SSH_USER="${RESTAPI_SSH_USER:-ec2-user}"
KEY="${RESTAPI_SSH_KEY:-}"
REMOTE="${RESTAPI_REMOTE:-/home/ec2-user/restapi_flask}"

[[ -n $HOST ]] || { echo "RESTAPI_HOST is not set (see deploy/deploy.env.example)" >&2; exit 1; }
[[ -n $KEY  ]] || { echo "RESTAPI_SSH_KEY is not set (see deploy/deploy.env.example)" >&2; exit 1; }

PROVISION=0
MIGRATE=0
for arg in "$@"; do
  case "$arg" in
    --provision) PROVISION=1 ;;
    --migrate)   MIGRATE=1 ;;
    -h|--help)   sed -n '2,9p' "$0"; exit 0 ;;
    *) echo "unknown option: $arg" >&2; exit 1 ;;
  esac
done

cd "$(dirname "$0")/.."
[[ -f app.py ]] || { echo "run from the project root" >&2; exit 1; }
[[ -f "$KEY" ]] || { echo "SSH key not found: $KEY" >&2; exit 1; }

SSH=(ssh -i "$KEY" -o StrictHostKeyChecking=accept-new -o ConnectTimeout=20)
say() { printf '\n\033[1;34m==>\033[0m %s\n' "$*"; }

say "Target: ${SSH_USER}@${HOST}:${REMOTE}"
"${SSH[@]}" "${SSH_USER}@${HOST}" "install -d -m 0755 ${REMOTE}"

say "Syncing application"
# --exclude .env is deliberate: the real secrets live in /etc/recipes on the
# server and must never be pushed from a laptop.
rsync -az --delete \
  --exclude '.git' --exclude '.venv' --exclude '__pycache__' --exclude '*.pyc' \
  --exclude '.DS_Store' --exclude '.env' --exclude '.pytest_cache' \
  --exclude 'tests/' \
  -e "${SSH[*]}" \
  ./ "${SSH_USER}@${HOST}:${REMOTE}/"

if [[ $PROVISION -eq 1 ]]; then
  say "Running provision.sh on the server"
  "${SSH[@]}" -t "${SSH_USER}@${HOST}" "sudo bash ${REMOTE}/deploy/provision.sh"
  exit 0
fi

if [[ $MIGRATE -eq 1 ]]; then
  say "Applying migrate.sql"
  "${SSH[@]}" "${SSH_USER}@${HOST}" \
    "sudo -u postgres psql -d recipes -v ON_ERROR_STOP=1 -f ${REMOTE}/deploy/sql/"
fi

say "Restarting"
"${SSH[@]}" "${SSH_USER}@${HOST}" bash -s <<REMOTE_EOF
set -e
sudo chown -R ec2-user:ec2-user ${REMOTE} 2>/dev/null || true
# Dependencies can change between deploys; keep the venv in step.
if [ -x ${REMOTE}/.venv/bin/pip ]; then
  ${REMOTE}/.venv/bin/pip install --quiet -r ${REMOTE}/requirements.txt
fi
sudo systemctl restart restapi
sleep 2
echo "--- status ---"
systemctl is-active restapi
echo "--- health ---"
curl -s --unix-socket /run/recipes/recipes.sock http://localhost/health || echo "(socket not reachable)"
echo
REMOTE_EOF

say "Deployed."
